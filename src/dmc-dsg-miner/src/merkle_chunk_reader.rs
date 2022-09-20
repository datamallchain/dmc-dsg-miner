use std::collections::HashMap;
use std::convert::TryInto;
use std::future::Future;
use std::io::SeekFrom;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use cyfs_base::*;
use cyfs_chunk_lib::{Chunk, MemChunk};
use crate::{ContractChunkStore, ContractMetaStore, DSG_CHUNK_PIECE_SIZE, HashStore, HashVecStore, MetaStore, VecCache};

pub struct MerkleChunkReader<CHUNKSTORE: ContractChunkStore> {
    chunk_store: Arc<CHUNKSTORE>,
    chunk_list: Vec<ChunkId>,
    pos: u64,
    chunk_size: u32,
    chunk_map: HashMap<ChunkId, Box<dyn Chunk>>,
}

impl<CHUNKSTORE: ContractChunkStore> MerkleChunkReader<CHUNKSTORE> {
    pub fn new(chunk_store: Arc<CHUNKSTORE>, chunk_list: Vec<ChunkId>, chunk_size: u32, chunk_map: Option<HashMap<ChunkId, Box<dyn Chunk>>>) -> Self {
        Self {
            chunk_store,
            chunk_list,
            pos: 0,
            chunk_size,
            chunk_map: chunk_map.unwrap_or(Default::default()),
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
        let chunk_data = self.chunk_store.get_chunk(chunk_id.clone()).await?;
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

impl<CHUNKSTORE: ContractChunkStore> std::io::Read for MerkleChunkReader<CHUNKSTORE> {
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

impl<CHUNKSTORE: ContractChunkStore> std::io::Seek for MerkleChunkReader<CHUNKSTORE> {
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

pub struct AsyncMerkleChunkReader<CHUNKSTORE: ContractChunkStore> {
    reader: Box<MerkleChunkReader<CHUNKSTORE>>,
    read_future: Mutex<Option<Pin<Box<dyn Future<Output = BuckyResult<usize>> + Send>>>>,
    seek_future: Mutex<Option<Pin<Box<dyn Future<Output = BuckyResult<u64>> + Send>>>>,
}

impl<CHUNKSTORE: ContractChunkStore> AsyncMerkleChunkReader<CHUNKSTORE> {
    pub fn new(reader: MerkleChunkReader<CHUNKSTORE>) -> Self {
        Self {
            reader: Box::new(reader),
            read_future: Mutex::new(None),
            seek_future: Mutex::new(None)
        }
    }
}
impl<CHUNKSTORE: ContractChunkStore> async_std::io::Read for AsyncMerkleChunkReader<CHUNKSTORE> {
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
impl<CHUNKSTORE: ContractChunkStore> async_std::io::Seek for AsyncMerkleChunkReader<CHUNKSTORE> {
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

pub struct MerkleMemoryChunkReader<'a> {
    buf: &'a [u8],
    chunk_size: u32,
    pos: u64,
}

impl<'a> MerkleMemoryChunkReader<'a> {
    pub fn new(buf: &'a [u8], chunk_size: u32) -> Self {
        Self {
            buf,
            chunk_size,
            pos: 0
        }
    }
}

impl<'a> async_std::io::Read for MerkleMemoryChunkReader<'a> {
    fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        if this.pos >= this.chunk_size as u64 {
            return Poll::Ready(Ok(0));
        }

        if this.pos >= this.buf.len() as u64 {
            if this.pos + buf.len() as u64 > this.chunk_size as u64 {
                let len = this.chunk_size as u64 - this.pos;
                unsafe {
                    std::ptr::write_bytes(buf.as_mut_ptr(), 0, len as usize);
                }
                this.pos = this.chunk_size as u64;
                Poll::Ready(Ok(len as usize))
            } else {
                unsafe {
                    std::ptr::write_bytes(buf.as_mut_ptr(), 0, buf.len());
                }
                this.pos = this.pos + buf.len() as u64;
                Poll::Ready(Ok(buf.len()))
            }
        } else {
            if this.pos + buf.len() as u64 > this.buf.len() as u64 {
                buf[..this.buf.len() - this.pos as usize].copy_from_slice(&this.buf[this.pos as usize..]);
                if this.pos + buf.len() as u64 > this.chunk_size as u64 {
                    unsafe {
                        std::ptr::write_bytes(buf[this.buf.len() - this.pos as usize..this.chunk_size as usize - this.pos as usize].as_mut_ptr(), 0, this.chunk_size as usize - this.buf.len());
                    }
                    this.pos = this.chunk_size as u64;
                    Poll::Ready(Ok(this.chunk_size as usize - this.pos as usize))
                } else {
                    unsafe {
                        std::ptr::write_bytes(buf[this.buf.len() - this.pos as usize..].as_mut_ptr(), 0, buf.len() - this.buf.len() + this.pos as usize);
                    }
                    this.pos += buf.len() as u64;
                    Poll::Ready(Ok(buf.len()))
                }
            } else {
                buf.copy_from_slice(&this.buf[this.pos as usize..this.pos as usize + buf.len()]);
                this.pos += buf.len() as u64;
                Poll::Ready(Ok(buf.len()))
            }
        }
    }
}

impl<'a> async_std::io::Seek for MerkleMemoryChunkReader<'a> {
    fn poll_seek(self: Pin<&mut Self>, _cx: &mut Context<'_>, pos: SeekFrom) -> Poll<std::io::Result<u64>> {
        let this = self.get_mut();
        match pos {
            SeekFrom::Start(pos) => {
                this.pos = pos;
            }
            SeekFrom::End(pos) => {
                if pos + (this.chunk_size as i64) < 0 {
                    this.pos = 0;
                } else {
                    this.pos = this.chunk_size as u64;
                }
            }
            SeekFrom::Current(pos) => {
                if pos + (this.pos as i64) < 0 {
                    this.pos = 0;
                } else {
                    this.pos += pos as u64;
                }
            }
        }
        Poll::Ready(Ok(this.pos))
    }
}

pub struct MinerHashStore<
    T: Send + Sync + Deref<Target=[u8]> + DerefMut<Target=[u8]>,
    CONN: ContractMetaStore,
    METASTORE: MetaStore<CONN>> {
    base_layer: u16,
    chunk_padding_len: u32,
    chunks: Vec<(ChunkId, HashValue)>,
    sub_cache: Mutex<HashMap<ChunkId, Arc<HashVecStore<Vec<u8>>>>>,
    hash_store: HashVecStore<T>,
    chunk_meta_store: Arc<METASTORE>,
    maker: PhantomData<CONN>,
}

impl<
    T: Send + Sync + Deref<Target=[u8]> + DerefMut<Target=[u8]>,
    CONN: ContractMetaStore,
    METASTORE: MetaStore<CONN>,> MinerHashStore<T, CONN, METASTORE> {
    pub fn new<C: VecCache<T>>(
        base_layer: u16,
        chunk_padding_len: u32,
        chunks: Vec<(ChunkId, HashValue)>,
        chunk_meta_store: Arc<METASTORE>,
    ) -> BuckyResult<Self> {
        let leafs = if chunks.len() % 2 == 0 {
            chunks.len() / 2
        } else {
            chunks.len() / 2 + 1
        };
        Ok(Self {
            base_layer,
            chunk_padding_len,
            chunks,
            sub_cache: Mutex::new(Default::default()),
            hash_store: HashVecStore::<T>::new::<C>(leafs as u64)?,
            chunk_meta_store,
            maker: Default::default()
        })
    }
}

#[async_trait::async_trait]
impl <
    T: Send + Sync + Deref<Target=[u8]> + DerefMut<Target=[u8]>,
    CONN: ContractMetaStore,
    METASTORE: MetaStore<CONN>> HashStore for MinerHashStore<T, CONN, METASTORE> {
    async fn get_node_list_len(&self, layer_number: u16) -> BuckyResult<u64> {
        if self.base_layer == layer_number {
            Ok(self.chunks.len() as u64)
        } else if self.base_layer > layer_number {
            Ok(self.chunks.len() as u64 * 2u64.pow((self.base_layer - layer_number) as u32))
        } else {
            self.hash_store.get_node_list_len(layer_number - self.base_layer - 1).await
        }
    }

    async fn get_node(&self, layer_number: u16, index: u64) -> BuckyResult<&[u8; 32]> {
        if self.base_layer == layer_number {
            match self.chunks.get(index as usize) {
                None => {
                    let msg = format!("can't find index {} at {}", index, layer_number);
                    log::error!("{}", msg.as_str());

                    Err(BuckyError::new(BuckyErrorCode::NotFound, msg))
                }
                Some((_, hash)) => {
                    Ok(hash.as_slice().try_into().unwrap())
                }
            }
        } else if self.base_layer > layer_number {
            let mut chunk_index = index;
            for _ in 0..self.base_layer - layer_number {
                chunk_index = chunk_index / 2;
            }
            let mut start_pos = chunk_index;
            if start_pos != 0 {
                let mut offset_unit = 1;
                for _ in 0..self.base_layer - layer_number {
                    offset_unit = offset_unit * 2;
                }
                start_pos = chunk_index * offset_unit;
            }
            match self.chunks.get(chunk_index as usize) {
                Some((chunk_id, _)) => {
                    let need_read = {
                        let mut sub_cache = self.sub_cache.lock().unwrap();
                        if let None = sub_cache.get(chunk_id) {
                            true
                        } else {
                            false
                        }
                    };
                    if need_read {
                        let mut conn = self.chunk_meta_store.create_meta_connection().await?;
                        let (_, tree_data) = conn.get_chunk_merkle_data(chunk_id, self.chunk_padding_len).await?;
                        let sub_hash_store = HashVecStore::<Vec<u8>>::load(self.chunk_padding_len as u64 / DSG_CHUNK_PIECE_SIZE, tree_data)?;
                        let mut sub_cache = self.sub_cache.lock().unwrap();
                        sub_cache.insert(chunk_id.clone(), Arc::new(sub_hash_store));
                    }

                    let sub_store = {
                        let mut sub_cache = self.sub_cache.lock().unwrap();
                        sub_cache.get(chunk_id).unwrap().clone()
                    };
                    let ret = sub_store.get_node(layer_number, index - start_pos).await?;
                    log::info!("sub hash layer {} index {} hash {}", layer_number, index - start_pos, hex::encode(ret));
                    unsafe {
                        Ok(std::mem::transmute(ret))
                    }
                },
                None => {
                    let msg = format!("can't find index {} at {}", index, layer_number);
                    log::error!("{}", msg.as_str());

                    Err(BuckyError::new(BuckyErrorCode::NotFound, msg))
                }
            }
        } else {
            self.hash_store.get_node(layer_number - self.base_layer - 1, index).await
        }
    }

    async fn set_node(&mut self, layer_number: u16, index: u64, hash: &[u8; 32]) -> BuckyResult<()> {
        // log::info!("set node layer {} index {} hash {}", layer_number, index, hex::encode(hash));
        if self.base_layer >= layer_number {
            let msg = format!("set node error, base_layer {} input layrer {}", self.base_layer, layer_number);
            log::error!("{}", msg);
            return Err(BuckyError::new(BuckyErrorCode::Failed, msg));
        } else {
            self.hash_store.set_node(layer_number - self.base_layer - 1, index, hash).await
        }
    }

    async fn get_min_layer_number(&self) -> BuckyResult<u16> {
        let sub_store = {
            let sub_cache = self.sub_cache.lock().unwrap();
            if sub_cache.len() != 0 {
                let (_, sub_store) = sub_cache.iter().next().unwrap();
                Some(sub_store.clone())
            } else {
                None
            }
        };
        if sub_store.is_some() {
            return sub_store.as_ref().unwrap().get_min_layer_number().await;
        }
        if self.chunks.len() > 0 {
            let (chunk_id, _) = self.chunks.get(0).unwrap();
            let chunk_len = chunk_id.len();
            let mut padding_len = 1024;
            loop {
                if padding_len >= chunk_len {
                    break;
                }
                padding_len *= 2;
            }
            let mut conn = self.chunk_meta_store.create_meta_connection().await?;
            let (_, tree_data) = conn.get_chunk_merkle_data(chunk_id, self.chunk_padding_len).await?;
            let sub_hash_store = HashVecStore::<Vec<u8>>::load(padding_len as u64 / DSG_CHUNK_PIECE_SIZE, tree_data)?;
            sub_hash_store.get_min_layer_number().await
        } else {
            log::error!("chunk list is null");
            Err(BuckyError::new(BuckyErrorCode::Failed, "chunk list is null"))
        }
    }
}
