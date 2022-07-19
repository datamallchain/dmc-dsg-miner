use std::collections::HashMap;
use std::future::Future;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use async_std::io::ReadExt;
use cyfs_base::*;
use cyfs_chunk_lib::{Chunk, MemChunk};
use cyfs_lib::{NDNAPILevel, NDNGetDataOutputRequest, NDNOutputRequestCommon, SharedCyfsStack};

pub struct MerkleChunkReader {
    stack: Arc<SharedCyfsStack>,
    chunk_list: Vec<ChunkId>,
    pos: u64,
    chunk_size: u32,
    chunk_map: HashMap<ChunkId, Box<dyn Chunk>>,
}

impl MerkleChunkReader {
    pub fn new(stack: Arc<SharedCyfsStack>, chunk_list: Vec<ChunkId>, chunk_size: u32) -> Self {
        Self {
            stack,
            chunk_list,
            pos: 0,
            chunk_size,
            chunk_map: Default::default(),
        }
    }

    fn get_chunk_id_by_pos(&self, pos: u64) -> BuckyResult<(u64, ChunkId, usize)> {
        let index = pos / self.chunk_size as u64;
        return if index >= self.chunk_list.len() as u64 {
            Err(BuckyError::new(BuckyErrorCode::NotFound, "can't find chunkid"))
        } else {
            Ok((index * self.chunk_size as u64, self.chunk_list[index as usize].clone(), self.chunk_list.len() * self.chunk_size as usize))
        }
    }

    async fn get_chunk(&self, chunk_id: &ChunkId) -> BuckyResult<Box<dyn Chunk>> {
        let mut resp = self.stack.ndn_service().get_data(NDNGetDataOutputRequest {
            common: NDNOutputRequestCommon {
                req_path: None,
                dec_id: None,
                level: NDNAPILevel::NDC,
                target: None,
                referer_object: vec![],
                flags: 0
            },
            object_id: chunk_id.object_id(),
            range: None,
            inner_path: None
        }).await?;

        let mut chunk_data = vec![];
        let _ = resp.data.read_to_end(&mut chunk_data).await.map_err(|e| {
            let msg = format!("get chunk err {}", e);
            log::error!("{}", msg.as_str());
            BuckyError::new(BuckyErrorCode::IoError, msg)
        })?;

        Ok(Box::new(MemChunk::from(chunk_data)))
    }

    async fn get_chunk_by_pos(&mut self, pos: u64) -> BuckyResult<(u64, ChunkId, Box<dyn Chunk>, usize)> {
        let (chunk_pos, chunk_id, file_size) = self.get_chunk_id_by_pos(pos)?;
        if !self.chunk_map.contains_key(&chunk_id) {
            let chunk = self.get_chunk(&chunk_id).await?;
            self.chunk_map.insert(chunk_id.clone(), chunk);
        }
        Ok((chunk_pos, chunk_id.clone(), self.chunk_map.remove(&chunk_id).unwrap(), file_size))
    }

    pub async fn read_async(&mut self, buf: &mut [u8]) -> BuckyResult<usize> {
        let mut tmp_buf = buf;
        let mut read_len = 0;
        let file_size = self.get_file_size()?;
        if self.pos >= file_size {
            return Ok(0);
        }
        loop {
            let (chunk_pos, chunk_id, mut chunk, file_size) = self.get_chunk_by_pos(self.pos).await.map_err(|e| {
                let msg = format!("get_chunk_by_pos {} failed.err {}", self.pos, e);
                println!("{}", msg.as_str());
                log::error!("{}", msg.as_str());
                std::io::Error::new(std::io::ErrorKind::Other, msg)
            })?;

            let chunk_offset = self.pos - chunk_pos;
            if chunk_offset as usize >= chunk_id.len() {
                if tmp_buf.len() > self.chunk_size as usize - chunk_offset as usize {
                    unsafe {
                        std::ptr::write_bytes(tmp_buf.as_mut_ptr(), 0, self.chunk_size as usize - chunk_offset as usize);
                    }
                    tmp_buf = &mut tmp_buf[self.chunk_size as usize - chunk_offset as usize..];
                    self.pos += self.chunk_size as u64 - chunk_offset;
                    read_len += self.chunk_size as usize - chunk_offset as usize;
                } else {
                    unsafe {
                        std::ptr::write_bytes(tmp_buf.as_mut_ptr(), 0, tmp_buf.len());
                    }
                    self.pos += tmp_buf.len() as u64;
                    read_len += tmp_buf.len();
                    let len = tmp_buf.len();
                    tmp_buf = &mut tmp_buf[len..];
                }
            } else {
                chunk.seek(SeekFrom::Start(chunk_offset)).await?;
                let read_size = chunk.read(tmp_buf).await?;
                tmp_buf = &mut tmp_buf[read_size..];
                self.pos += read_size as u64;
                read_len += read_size;
            }
            self.chunk_map.insert(chunk_id, chunk);

            if tmp_buf.len() == 0 || self.pos >= file_size as u64 {
                break;
            }
        }

        Ok(read_len)
    }

    fn get_file_size(&self) -> BuckyResult<u64> {
                Ok(self.chunk_list.len() as u64 * self.chunk_size as u64)
    }

    pub async fn seek_async(&mut self, pos: SeekFrom) -> BuckyResult<u64> {
        let this = self;
        match pos {
            SeekFrom::Start(pos) => {
                this.pos = pos;
                Ok(pos)
            },
            SeekFrom::End(pos) => {
                let file_size = this.get_file_size()?;

                if file_size as i64 + pos < 0 {
                    return Err(BuckyError::new(BuckyErrorCode::Failed, format!("seek failed")));
                }
                this.pos = (file_size as i64 + pos) as u64;
                Ok(this.pos as u64)
            },
            SeekFrom::Current(pos) => {
                if this.pos as i64 + pos < 0 {
                    return Err(BuckyError::new(BuckyErrorCode::Failed, format!("seek failed")));
                }
                this.pos = (this.pos as i64 + pos) as u64;
                Ok(this.pos)
            }
        }
    }
}

impl std::io::Read for MerkleChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let this: &'static mut Self = unsafe {std::mem::transmute(self)};
        let buf: &'static mut [u8] = unsafe {std::mem::transmute(buf)};
        async_std::task::block_on(async move {
            match this.read_async(buf).await {
                Ok(writed_size) => {
                    Ok(writed_size)
                },
                Err(e) => {
                    Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{}", e)))
                }
            }
        })
    }
}

impl std::io::Seek for MerkleChunkReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let this: &'static mut Self = unsafe {std::mem::transmute(self)};
        async_std::task::block_on(async move {
            match this.seek_async(pos).await {
                Ok(pos) => {
                    Ok(pos)
                },
                Err(e) => {
                    Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{}", e)))
                }
            }
        })
    }
}

pub struct AsyncMerkleChunkReader {
    reader: Box<MerkleChunkReader>,
    read_future: Mutex<Option<Pin<Box<dyn Future<Output = BuckyResult<usize>> + Send>>>>,
    seek_future: Mutex<Option<Pin<Box<dyn Future<Output = BuckyResult<u64>> + Send>>>>,
}

impl AsyncMerkleChunkReader {
    pub fn new(reader: MerkleChunkReader) -> Self {
        Self {
            reader: Box::new(reader),
            read_future: Mutex::new(None),
            seek_future: Mutex::new(None)
        }
    }
}
impl async_std::io::Read for AsyncMerkleChunkReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
        unsafe {
            let this: &'static mut Self = std::mem::transmute(self.get_unchecked_mut());
            let buf: &'static mut [u8] = std::mem::transmute(buf);
            let mut future = this.read_future.lock().unwrap();
            if future.is_none() {
                *future = Some(Box::pin(this.reader.read_async(buf)));
            }
            match future.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Ready(ret) => {
                    *future = None;
                    match ret {
                        Ok(ret) => Poll::Ready(Ok(ret)),
                        Err(e) => Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!("{}", e),
                        ))),
                    }
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }
}
impl async_std::io::Seek for AsyncMerkleChunkReader {
    fn poll_seek(self: Pin<&mut Self>, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<std::io::Result<u64>> {
        unsafe {
            let this: &'static mut Self = std::mem::transmute(self.get_unchecked_mut());
            let mut future = this.seek_future.lock().unwrap();
            if future.is_none() {
                *future = Some(Box::pin(this.reader.seek_async(pos)));
            }
            match future.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Ready(ret) => {
                    *future = None;
                    match ret {
                        Ok(ret) => Poll::Ready(Ok(ret)),
                        Err(e) => Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!("{}", e),
                        ))),
                    }
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }
}
