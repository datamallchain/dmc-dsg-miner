use std::{ops::Range};
use async_std::io::Read;
use cyfs_base::*;
use cyfs_dsg_client::*;
use crate::*;

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
    async fn get_state(&mut self, state_id: ObjectId) -> BuckyResult<Option<DsgContractStateObject>>;
    async fn get_chunks_by_path(&mut self, url_path: String) -> BuckyResult<Vec<ChunkId>>;
    async fn get_chunk_list(&mut self, contract_id: &ObjectId) -> BuckyResult<Vec<ChunkId>>;
    async fn save_chunk_list(&mut self, contract_id: &ObjectId, chunk_list: Vec<ChunkId>) -> BuckyResult<()>;
    async fn get_challenge(&mut self, contract_id: &ObjectId) -> BuckyResult<DsgChallengeObject>;
    async fn save_challenge(&mut self, contract_id: &ObjectId, challenge: &DsgChallengeObject) -> BuckyResult<()>;
    async fn chunk_ref_add(&mut self, contract_id: &ObjectId, chunk_list: &Vec<ChunkId>) -> BuckyResult<()>;
    async fn chunk_ref_del(&mut self, contract_id: &ObjectId, chunk_list: &Vec<ChunkId>) -> BuckyResult<()>;
    async fn chunk_del_list_add(&mut self, chunk_list: &Vec<ChunkId>) -> BuckyResult<()>;
    async fn chunk_del_list_del(&mut self, chunk_list: &Vec<ChunkId>) -> BuckyResult<()>;
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
            let mut cur_state_ref = DsgContractStateObjectRef::from(state_list.get(state_list.len() - 1).unwrap());
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
    async fn save_chunk(&self, chunk_id: ChunkId, buf: Vec<u8>) -> BuckyResult<()>;
    async fn get_chunk(&self, chunk_id: ChunkId) -> BuckyResult<Vec<u8>>;
    async fn get_chunk_by_range(&self, chunk_id: ChunkId, range: Range<u64>) -> BuckyResult<Vec<u8>>;
    async fn get_chunk_reader(&self, chunk_id: ChunkId) -> BuckyResult<Box<dyn Unpin + Read + Send + Sync>>;
    async fn get_contract_data(&self, chunk_list: Vec<ChunkId>, range: Range<u64>, chunk_size: u32) -> BuckyResult<Vec<u8>>;
    async fn chunk_exists(&self, chunk_id: &ChunkId) -> bool;
}

#[derive(Debug,Clone, Copy, Eq, PartialEq)]
pub enum ContractStatus {
    Wait,
    Success,
    Proof,
    ProofFail,
    Down,
    ToProof,
    ChallengeOutTime,
    ContractOutTime,
    Complete,
    Other
}

impl From<i64> for ContractStatus {
    fn from( v: i64) -> Self {
        match v {
            1 => ContractStatus::Wait,
            2 => ContractStatus::Success,
            3 => ContractStatus::Proof,
            4 => ContractStatus::ProofFail,
            5 => ContractStatus::Down,
            6 => ContractStatus::ToProof,
            7 => ContractStatus::ChallengeOutTime,
            8 => ContractStatus::ContractOutTime,
            9 => ContractStatus::Complete,
            _ => ContractStatus::Other
        }
    }
}

impl Into<i64> for ContractStatus {
    fn into(self) -> i64 {
        match self {
            ContractStatus::Wait => 1,
            ContractStatus::Success => 2,
            ContractStatus::Proof => 3,
            ContractStatus::ProofFail => 4,
            ContractStatus::Down => 5,
            ContractStatus::ToProof => 6,
            ContractStatus::ChallengeOutTime => 7,
            ContractStatus::ContractOutTime => 8,
            ContractStatus::Complete => 9,
            ContractStatus::Other => 10
        }
    }
}
