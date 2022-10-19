use std::{ops::Range};
use std::convert::TryFrom;
use async_std::io::Read;
use cyfs_base::*;
use cyfs_chunk_lib::Chunk;
use cyfs_dsg_client::*;
use crate::*;
use dmc_dsg_base::*;

#[derive(ProtobufEncode, ProtobufDecode, ProtobufTransformType)]
#[cyfs_protobuf_type(crate::protos::MetaData)]
pub struct MetaData {
    pub contract: DsgContractObject<DMCContractData>,
    pub state_list: Vec<DsgContractStateObject>,
}

impl ProtobufTransform<crate::protos::MetaData> for MetaData {
    fn transform(value: crate::protos::MetaData) -> BuckyResult<Self> {
        Ok(Self {
            contract: DsgContractObject::clone_from_slice(value.contract.as_slice())?,
            state_list: Vec::<DsgContractStateObject>::clone_from_slice(value.state_list.as_slice())?
        })
    }
}

impl ProtobufTransform<&MetaData> for crate::protos::MetaData {
    fn transform(value: &MetaData) -> BuckyResult<Self> {
        Ok(Self {
            contract: value.contract.to_vec()?,
            state_list: value.state_list.to_vec()?
        })
    }
}

#[async_trait::async_trait]
pub trait ContractMetaStore: Send + Sync + MetaConnection + 'static {
    async fn get_contract(&mut self, contract_id: &ObjectId) -> BuckyResult<Option<DsgContractObject<DMCContractData>>>;
    async fn get_contract_id_by_dmc_order(&mut self, dmc_order: &str) -> BuckyResult<Option<ObjectId>>;
    async fn save_contract(&mut self, contract: &DsgContractObject<DMCContractData>) -> BuckyResult<()>;
    async fn contract_sync_set(&mut self) -> BuckyResult<Vec<ObjectId>>;
    async fn contract_sync_set_add(&mut self, contract_list: &Vec<ObjectId>) -> BuckyResult<()>;
    async fn contract_sync_set_remove(&mut self, contract_list: &Vec<ObjectId>) -> BuckyResult<()>;
    async fn contract_set(&mut self) -> BuckyResult<Vec<ObjectId>>;
    async fn contract_set_add(&mut self, contract_list: &Vec<ObjectId>) -> BuckyResult<()>;
    async fn contract_set_remove(&mut self, contract_list: &Vec<ObjectId>) -> BuckyResult<()>;
    async fn get_contract_info(&mut self, contract_id: &ObjectId) -> BuckyResult<ContractInfo>;
    async fn set_contract_info(&mut self, contract_id: &ObjectId, contract_info: &ContractInfo) -> BuckyResult<()>;
    async fn contract_proof_set(&mut self) -> BuckyResult<Vec<ObjectId>>;
    async fn contract_proof_set_add(&mut self, contract_list: &Vec<ObjectId>) -> BuckyResult<()>;
    async fn contract_proof_set_remove(&mut self, contract_list: &Vec<ObjectId>) -> BuckyResult<()>;
    async fn get_contract_state(&mut self, contract_id: &ObjectId) -> BuckyResult<Option<DsgContractStateObject>>;
    async fn get_contract_state_id(&mut self, contract_id:& ObjectId) -> BuckyResult<Option<ObjectId>>;
    async fn get_syncing_contract_state(&mut self, contract_id: &ObjectId) -> BuckyResult<Option<DsgContractStateObject>>;
    async fn save_need_sync_contract_state(&mut self, contract_id: &ObjectId, state: &DsgContractStateObject) -> BuckyResult<()>;
    async fn set_contract_state_sync_complete(&mut self, contract_id: &ObjectId, state_id: &ObjectId) -> BuckyResult<()>;
    async fn save_state_id_by_path(&mut self, path: String, object_id: &ObjectId) -> BuckyResult<()>;
    async fn get_state_id_by_path(&mut self, path: String) -> BuckyResult<Option<ObjectId>>;
    async fn save_state(&mut self, state: &DsgContractStateObject) -> BuckyResult<()>;
    async fn get_state(&mut self, state_id: ObjectId) -> BuckyResult<Option<DsgContractStateObject>>;
    async fn get_chunks_by_path(&mut self, url_path: String) -> BuckyResult<Vec<ChunkId>>;
    async fn get_chunk_list(&mut self, contract_id: &ObjectId) -> BuckyResult<Vec<ChunkId>>;
    async fn save_chunk_list(&mut self, contract_id: &ObjectId, chunk_list: Vec<ChunkId>) -> BuckyResult<()>;
    async fn get_challenge(&mut self, contract_id: &ObjectId) -> BuckyResult<Option<DsgChallengeObject>>;
    async fn save_challenge(&mut self, contract_id: &ObjectId, challenge: &DsgChallengeObject) -> BuckyResult<()>;
    async fn chunk_ref_add(&mut self, contract_id: &ObjectId, chunk_list: &Vec<ChunkId>) -> BuckyResult<()>;
    async fn chunk_ref_del(&mut self, contract_id: &ObjectId, chunk_list: &Vec<ChunkId>) -> BuckyResult<()>;
    async fn chunk_del_list_del(&mut self, chunk_list: &Vec<ChunkId>) -> BuckyResult<()>;
    async fn get_del_chunk_list(&mut self) -> BuckyResult<Vec<ChunkId>>;
    async fn get_chunk_merkle_root(&mut self, chunk_list: &Vec<ChunkId>, chunk_size: u32) -> BuckyResult<Vec<(ChunkId, HashValue)>>;
    async fn get_chunk_merkle_data(&mut self, chunk_id: &ChunkId, merkle_chunk_size: u32) -> BuckyResult<(HashValue, Vec<u8>)>;

    async fn get_contract_meta_data(&mut self, contract_id: &ObjectId) -> BuckyResult<MetaData> {
        let contract = self.get_contract(contract_id).await?;
        if contract.is_none() {
            return Err(BuckyError::new(BuckyErrorCode::NotFound, format!("can't find contract {}", contract_id)));
        }
        let contract = contract.unwrap();
        let cur_state = self.get_contract_state(contract_id).await?;
        if cur_state.is_none() {
            return Err(BuckyError::new(BuckyErrorCode::NotFound, format!("can't find contract {}'s state", contract_id)));
        }
        let cur_state = cur_state.unwrap();
        let mut state_list = vec![cur_state];
        loop {
            let cur_state_ref = DsgContractStateObjectRef::from(state_list.get(state_list.len() - 1).unwrap());
            if let DsgContractState::DataSourceChanged(change) = cur_state_ref.state() {
                if change.prev_change.is_none() {
                    break;
                }
                let prev_state = self.get_state(change.prev_change.clone().unwrap()).await?;
                if prev_state.is_none() {
                    return Err(BuckyError::new(BuckyErrorCode::NotFound, format!("can't find contract {}'s state", contract_id)));
                }
                state_list.push(prev_state.unwrap());
            } else {
                assert!(false);
            }
        }
        Ok(MetaData {
            contract,
            state_list
        })
    }
}

#[async_trait::async_trait]
pub trait ContractChunkStore: Send + Sync + 'static {
    async fn save_chunk(&self, chunk_id: &ChunkId, buf: &[u8]) -> BuckyResult<()>;
    async fn get_chunk(&self, chunk_id: &ChunkId) -> BuckyResult<Box<dyn Chunk>>;
    async fn get_chunk_by_range(&self, chunk_id: &ChunkId, range: Range<u64>) -> BuckyResult<Vec<u8>>;
    async fn get_chunk_reader(&self, chunk_id: &ChunkId) -> BuckyResult<Box<dyn Unpin + Read + Send + Sync>>;
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
            let chunk_id = &chunk_list[idx as usize];
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
    async fn chunk_exists(&self, chunk_id: &ChunkId) -> bool;
}

#[derive(Debug,Clone, Copy, Eq, PartialEq)]
pub enum ContractStatus {
    Syncing,
    Storing,
}

impl TryFrom<i64> for ContractStatus {
    type Error = BuckyError;

    fn try_from( v: i64) -> Result<Self, Self::Error> {
        match v {
            1 => Ok(ContractStatus::Syncing),
            2 => Ok(ContractStatus::Storing),
            _ => {
                Err(cyfs_err!(BuckyErrorCode::UnSupport, "unknown value {}", v))
            }
        }
    }
}

impl Into<i64> for ContractStatus {
    fn into(self) -> i64 {
        match self {
            ContractStatus::Syncing => 1,
            ContractStatus::Storing => 2,
        }
    }
}
