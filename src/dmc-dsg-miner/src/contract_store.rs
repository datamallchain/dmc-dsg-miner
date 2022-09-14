use std::{ops::Range};
use std::sync::Arc;
use async_std::io::Read;
use cyfs_base::*;
use cyfs_dsg_client::*;
use crate::*;

#[async_trait::async_trait]
pub trait ContractMetaStore: Send + Sync {
    async fn get(&self, contract_id: &ObjectId) -> BuckyResult<DsgContractObject<DMCContractData>>;
    async fn save(&self, contract_id: &ObjectId, contract: &DsgContractObject<DMCContractData>) -> BuckyResult<()>;
    async fn get_wait_sync(&self) -> BuckyResult<Vec<(Vec<ChunkId>, ObjectId, ObjectId)>>;
    async fn get_wait_proof(&self) -> BuckyResult<Vec<(Vec<ChunkId>, ObjectId, ObjectId)>>;
    async fn update_down_status(&self, contract_id: &ObjectId, dstat: SyncStatus) -> BuckyResult<()>;
    async fn get_down_status(&self, contract_id: &ObjectId) -> BuckyResult<SyncStatus>;
    async fn get_stat(&self, contract_id: &ObjectId) -> BuckyResult<DsgContractStateObject>;
    async fn save_stat(&self, contract_id: &ObjectId, state: &DsgContractStateObject) -> BuckyResult<()>;
    async fn get_chunks_by_path(&self, url_path: String) -> BuckyResult<Vec<ChunkId>>;
    async fn get_chunk_list(&self, contract_id: &ObjectId) -> BuckyResult<Vec<ChunkId>>;
    async fn save_chunk_list(&self, contract_id: &ObjectId, chunk_list: Vec<ChunkId>) -> BuckyResult<()>;
    async fn get_challenge(&self, contract_id: &ObjectId) -> BuckyResult<DsgChallengeObject>;
    async fn save_challenge(&self, contract_id: &ObjectId, challenge: &DsgChallengeObject) -> BuckyResult<()>;
    async fn get_owner(&self, contract_id: &ObjectId) -> BuckyResult<ObjectId>;
    async fn save_owner(&self, contract_id: &ObjectId, owner_id: &ObjectId) -> BuckyResult<()>;
    async fn get_next_contract(&self, pos: usize) -> Option<(ObjectId,usize)>;
    async fn check_challenge_out_time(&self) -> BuckyResult<()>;
    async fn check_contract_out_time(&self) -> BuckyResult<()>;
    async fn chunk_ref_add(&self, contract_id: &ObjectId, chunk_id: &ChunkId) -> BuckyResult<()>;
    async fn chunk_ref_del(&self, contract_id: &ObjectId, chunk_id: &ChunkId) -> BuckyResult<()>;
    async fn chunk_del_list_add(&self, chunk_id: &ChunkId) -> BuckyResult<()>;
    async fn chunk_del_list_del(&self, chunk_id: &ChunkId) -> BuckyResult<()>;
    async fn get_end_contracts(&self) -> BuckyResult<Vec<(Vec<ChunkId>, ObjectId, ObjectId)>>;
    async fn repair_half_sync(&self) -> BuckyResult<()>;
    async fn get_need_check_end_contract_list(&self) -> BuckyResult<Vec<(DsgContractObject<DMCContractData>, u64)>>;
    async fn update_latest_check_time(&self, contract_id: &ObjectId, latest_check_time: u64) -> BuckyResult<()>;
    async fn update_contract_status(&self, contract_id: &ObjectId, dstat: SyncStatus, latest_check_time: u64) -> BuckyResult<()>;
}

#[async_trait::async_trait]
pub trait ContractChunkStore: Send + Sync {
    async fn save_chunk(&self, chunk_id: ChunkId, buf: Vec<u8>) -> BuckyResult<()>;
    async fn get_chunk(&self, chunk_id: ChunkId) -> BuckyResult<Vec<u8>>;
    async fn get_chunk_by_range(&self, chunk_id: ChunkId, range: Range<u64>) -> BuckyResult<Vec<u8>>;
    async fn get_chunk_reader(&self, chunk_id: ChunkId) -> BuckyResult<Box<dyn Unpin + Read + Send + Sync>>;
    async fn get_merkle(&self, chunk_list: Vec<ChunkId>, contract_id: &ObjectId, chunk_size: u32) -> BuckyResult<MerkleTree<AsyncMerkleChunkReader, HashVecStore<Vec<u8>>>>;
    async fn get_contract_data(&self, chunk_list: Vec<ChunkId>, range: Range<u64>, chunk_size: u32) -> BuckyResult<Vec<u8>>;
    async fn chunk_exists(&self, chunk_id: &ChunkId) -> bool;
}

pub struct ContractCursor {
    meta_store: Arc<Box<dyn ContractMetaStore>>,
    pos: usize
}

impl ContractCursor {
    pub fn new(meta_store: Arc<Box<dyn ContractMetaStore>>) -> Self {
        Self{ meta_store, pos: 0 }
    }

    pub async fn next(&mut self) -> Option<ObjectId> {
        if let Some((contract_id, cpos)) = self.meta_store.get_next_contract(self.pos).await {
            self.pos = cpos;
            Some(contract_id)
        } else {
            None
        }
    }
}

#[derive(Debug,Clone)]
pub enum SyncStatus {
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

impl From<i64> for SyncStatus {
    fn from( v: i64) -> Self {
        match v {
            1 => SyncStatus::Wait,
            2 => SyncStatus::Success,
            3 => SyncStatus::Proof,
            4 => SyncStatus::ProofFail,
            5 => SyncStatus::Down,
            6 => SyncStatus::ToProof,
            7 => SyncStatus::ChallengeOutTime,
            8 => SyncStatus::ContractOutTime,
            9 => SyncStatus::Complete,
            _ => SyncStatus::Other
        }
    }
}

impl Into<i64> for SyncStatus {
    fn into(self) -> i64 {
        match self {
            SyncStatus::Wait => 1,
            SyncStatus::Success => 2,
            SyncStatus::Proof => 3,
            SyncStatus::ProofFail => 4,
            SyncStatus::Down => 5,
            SyncStatus::ToProof => 6,
            SyncStatus::ChallengeOutTime => 7,
            SyncStatus::ContractOutTime => 8,
            SyncStatus::Complete => 9,
            SyncStatus::Other => 10
        }
    }
}
