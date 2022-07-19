use cyfs_base::*;
use cyfs_lib::*;
use crate::*;
use std::ops::{Range};
use async_std::{io::{Read, ReadExt, Cursor}, sync::Arc};

pub struct NocChunkStore {
    stack: Arc<SharedCyfsStack>
}
impl NocChunkStore {
    pub fn new(stack: Arc<SharedCyfsStack>) -> Self {
        Self {
            stack
        }
    }

    pub async fn get_id_by_path(&self, path: String) -> BuckyResult<Option<ObjectId>> {
        let key = hash(path).await;
        let op_env = self.stack.root_state_stub(None, None).create_path_op_env().await?;
        if let Some(object_id) = op_env.get_by_key(format!("/miner/contracts/chunk_id_path/"), key).await? {
            Ok(Some(object_id))
        } else {
            Ok(None)
        }
    }

    pub async fn save_id_by_path(&self, path: String, object_id: &ObjectId) -> BuckyResult<()> {
        let key = hash(path).await;
        let op_env = self.stack.root_state_stub(None, None).create_path_op_env().await?;
        op_env.set_with_key(format!("/miner/contracts/chunk_id_path/"), key, object_id, None, true).await?;
        if let Err(e) = op_env.commit().await {
            error!("save err: {}", e);
        }

        Ok(())
    }

    pub async fn get_noc_chunk(&self, object_id: ObjectId, range: Option<Range<u64>>) -> BuckyResult<Box<dyn Read + Unpin + Send + Sync + 'static>> {
        let crange = match range {
            Some(r) => {
                let pos = r.start;
                let len =  r.end - pos;

                Some(NDNDataRequestRange::new_data_range(vec![NDNDataRange{
                    start: Some(pos),
                    length: Some(len)
                }]))
            },
            None => None
        };

        let rsp = self.stack.ndn_service().get_data(NDNGetDataOutputRequest {
            common: NDNOutputRequestCommon{
                req_path: None,
                dec_id: None,
                level: NDNAPILevel::Router,
                target: None,
                referer_object: vec![],
                flags: 0,
            },
            object_id,
            range: crange,
            inner_path: None }).await?;

        Ok(rsp.data)
    }

    pub async fn build_merkle_tree(
        &self,
        chunk_list: &Vec<ChunkId>,
        chunk_size: u32
    ) -> BuckyResult<MerkleTree<AsyncMerkleChunkReader, HashVecStore<Vec<u8>>>> {
        let len = chunk_list.len() as u64 * chunk_size as u64;
        let leafs = if len % DSG_CHUNK_PIECE_SIZE == 0 { len / DSG_CHUNK_PIECE_SIZE } else { len / DSG_CHUNK_PIECE_SIZE + 1};
        let merkle = MerkleTree::create_from_raw(
            AsyncMerkleChunkReader::new(
                MerkleChunkReader::new(self.stack.clone(), chunk_list.clone(), chunk_size)),
            HashVecStore::<Vec<u8>>::new::<MemVecCache>(leafs)?).await?;
        Ok(merkle)
    }

}

#[async_trait::async_trait]
impl ContractChunkStore for NocChunkStore {
    async fn save_chunk(&self, chunk_id: ChunkId, buf: Vec<u8>) -> BuckyResult<()> {
        let _rsp = self.stack.ndn_service().put_data(NDNPutDataOutputRequest {
            common: NDNOutputRequestCommon{
                req_path: None,
                dec_id: None,
                level: NDNAPILevel::NDC,
                target: None,
                referer_object: vec![],
                flags: 0,
            },
            object_id: chunk_id.object_id(),
            length: buf.len() as u64,
            data: Box::new(Cursor::new(buf)) }).await?;

        Ok(())
    }

    async fn get_chunk(&self, chunk_id: ChunkId) -> BuckyResult<Vec<u8>> {
        let mut buf = vec![];
        let mut reader = self.get_noc_chunk(chunk_id.object_id(), None).await?;
        reader.read_to_end(&mut buf).await?;

        Ok(buf)
    }

    async fn get_chunk_reader(&self, chunk_id: ChunkId) -> BuckyResult<Box<dyn Unpin + Read + Send + Sync>> {
        let reader = self.get_noc_chunk(chunk_id.object_id(), None).await?;
        return Ok(reader);
    }

    async fn get_chunk_by_range(&self, chunk_id: ChunkId, range: Range<u64>) -> BuckyResult<Vec<u8>> {
        let mut buf = vec![];

        let mut reader = self.get_noc_chunk(chunk_id.object_id(), Some(range)).await?;
        reader.read_to_end(&mut buf).await?;

        Ok(buf)
    }

    async fn get_merkle(&self, chunk_list: Vec<ChunkId>, _contract_id: &ObjectId, chunk_size: u32) -> BuckyResult<MerkleTree<AsyncMerkleChunkReader, HashVecStore<Vec<u8>>>> {
        let merkle = self.build_merkle_tree(&chunk_list, chunk_size).await?;

        Ok(merkle)
    }

    async fn get_contract_data(&self, chunk_list: Vec<ChunkId>, range: Range<u64>, chunk_size: u32) -> BuckyResult<Vec<u8>> {
        let start = range.start / chunk_size as u64;
        let mut end = range.end / chunk_size as u64 + 1;
        if end > chunk_list.len() as u64{
            end = chunk_list.len() as u64;
        }
        let mut read_len = range.end - range.start;
        let mut data = Vec::with_capacity(read_len as usize);

        let mut cpos = range.start % chunk_size as u64;
        for idx in start..end {
            let chunk_id = chunk_list[idx as usize].clone();
            let csize = chunk_id.len() as u64;
            if csize == chunk_size as u64 {
                if csize - cpos > read_len {
                    let mut buf = self.get_chunk_by_range(chunk_id, cpos..cpos+read_len).await?;
                    read_len -= buf.len() as u64;
                    cpos = 0;
                    data.append(&mut buf);
                } else {
                    let mut buf = self.get_chunk_by_range(chunk_id, cpos..csize).await?;
                    read_len -= buf.len() as u64;
                    cpos = 0;
                    data.append(&mut buf);
                }
            } else if csize < chunk_size as u64 {
                if csize - cpos > read_len {
                    let mut buf = self.get_chunk_by_range(chunk_id, cpos..cpos+read_len).await?;
                    read_len -= buf.len() as u64;
                    cpos += 0;
                    data.append(&mut buf);
                } else {
                    let mut buf = self.get_chunk_by_range(chunk_id, cpos..csize).await?;
                    read_len -= buf.len() as u64;
                    cpos += buf.len() as u64;
                    data.append(&mut buf);
                    if cpos < chunk_size as u64 && read_len > 0 {
                        let mut padding = if read_len > chunk_size as u64 - cpos {
                            let mut padding = Vec::<u8>::new();
                            padding.resize(chunk_size as usize - cpos as usize, 0);
                            padding
                        } else {
                            let mut padding = Vec::<u8>::new();
                            padding.resize(read_len as usize, 0);
                            padding
                        };
                        read_len -= buf.len() as u64;
                        cpos += 0;
                        data.append(&mut padding);
                    }
                }
            } else {
                let msg = format!("chunk {} len {} big than {}", chunk_id.to_string(), csize, chunk_size);
                log::error!("{}", msg);
                return Err(BuckyError::new(BuckyErrorCode::Failed, msg));
            }
        }

        Ok(data)
    }

    async fn chunk_exists(&self, chunk_id: &ChunkId) -> bool {
        if let Ok(_obj) = self.get_chunk(chunk_id.clone()).await {
            return true;
        }
        false
    }

}

