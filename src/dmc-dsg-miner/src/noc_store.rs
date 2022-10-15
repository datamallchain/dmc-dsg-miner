use cyfs_base::*;
use cyfs_lib::*;
use crate::*;
use std::ops::{Range};
use async_std::{io::{Read, ReadExt, Cursor}, sync::Arc};
use cyfs_chunk_lib::{Chunk, MemChunk};

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
}

#[async_trait::async_trait]
impl ContractChunkStore for NocChunkStore {
    async fn save_chunk(&self, chunk_id: &ChunkId, buf: &[u8]) -> BuckyResult<()> {
        unsafe {
            let buf: &'static [u8] = std::mem::transmute(buf);
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
    }

    async fn get_chunk(&self, chunk_id: &ChunkId) -> BuckyResult<Box<dyn Chunk>> {
        let mut buf = vec![];
        let mut reader = self.get_noc_chunk(chunk_id.object_id(), None).await?;
        reader.read_to_end(&mut buf).await?;

        Ok(Box::new(MemChunk::from(buf)))
    }

    async fn get_chunk_reader(&self, chunk_id: &ChunkId) -> BuckyResult<Box<dyn Unpin + Read + Send + Sync>> {
        let reader = self.get_noc_chunk(chunk_id.object_id(), None).await?;
        return Ok(reader);
    }

    async fn get_chunk_by_range(&self, chunk_id: &ChunkId, range: Range<u64>) -> BuckyResult<Vec<u8>> {
        let mut buf = vec![];

        let mut reader = self.get_noc_chunk(chunk_id.object_id(), Some(range)).await?;
        reader.read_to_end(&mut buf).await?;

        Ok(buf)
    }

    async fn chunk_exists(&self, chunk_id: &ChunkId) -> bool {
        if let Ok(_obj) = self.get_chunk(chunk_id).await {
            return true;
        }
        false
    }

}

