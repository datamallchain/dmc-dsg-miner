use std::sync::Arc;
use cyfs_base::{BuckyResult, HashValue, NamedObject, ObjectDesc, ObjectId, OwnerObjectDesc, RawConvertTo};
use serde::{Deserialize, Serialize};
use dmc_dsg_base::{Authority, CyfsClient, CyfsInfo, CyfsPath, DMCTxSender, DSGJSON, JSONObject, TransResult};
use crate::RemoteProtocol::ChallengeResp;

pub enum RemoteProtocol {
    CreatePushTask,
    CreatePushTaskResp,
    CreatePullTask,
    CreatePullTaskResp,
    GetTaskState,
    GetTaskStateResp,
    GetDMCAccount,
    GetDMCAccountResp,
    SetDMCAccount,
    SetDMCAccountResp,
    ReportCyfsInfo,
    ReportCyfsInfoResp,
    AddMerkle,
    AddMerkleResp,
    ChallengeResp,
    ChallengeRespResp,
    Arbitration,
    ArbitrationResp,
    GetTaskList,
    GetTaskListResp,
    GetTaskChunkList,
    GetTaskChunkListResp,
    SetMinerDecId,
    SetMinerDecIdResp,
    Stack,
    StackResp,
    Bill,
    BillResp,
    Mint,
    MintResp,
}

#[derive(Serialize, Deserialize)]
pub struct AddMerkleReq {
    pub order_id: String,
    pub merkle_root: String,
    pub data_block_count: u64,
}

#[derive(Serialize, Deserialize)]
pub struct ChallengeRespReq {
    pub order_id: String,
    pub reply_hash: String,
}

#[derive(Serialize, Deserialize)]
pub struct ArbitrationReq {
    pub order_id: String,
    pub data: Vec<u8>,
    pub cut_merkle: Vec<String>
}

#[derive(Serialize, Deserialize)]
pub struct BillReq {
    pub asset: String,
    pub price: f64,
    pub memo: String,
}

pub struct RemoteDMCTxSender<CLIENT: CyfsClient> {
    client: Arc<CLIENT>,
    dec_id: ObjectId,
    owner_id: ObjectId,
    req_path: String,
}

impl<CLIENT: CyfsClient> RemoteDMCTxSender<CLIENT> {
    pub fn new(client: Arc<CLIENT>, dec_id: ObjectId) -> Self {
        let local_device = client.local_device();
        let local_id = local_device.desc().object_id();
        let owner_id = local_device.desc().owner().as_ref().unwrap().clone();
        let req_path = CyfsPath::new(local_id, dec_id, "dmc_chain_commands").to_path();
        Self {
            client,
            dec_id,
            owner_id,
            req_path
        }
    }
}

#[async_trait::async_trait]
impl<CLIENT: CyfsClient> DMCTxSender for RemoteDMCTxSender<CLIENT> {
    async fn update_auth(&self, _permission: String, _parent: String, _auth: Authority) -> BuckyResult<TransResult> {
        unreachable!()
    }

    async fn delete_auth(&self, _permission: String, _parent_permission: String) -> BuckyResult<TransResult> {
        unreachable!()
    }

    async fn link_auth(&self, _code: String, _ty: String, _permission: String) -> BuckyResult<TransResult> {
        unreachable!()
    }

    async fn unlink_auth(&self, _code: String, _ty: String, _permission: String) -> BuckyResult<TransResult> {
        unreachable!()
    }

    async fn stake(&self, amount: &str) -> BuckyResult<TransResult> {
        let req = JSONObject::new(
            self.dec_id.clone(),
            self.owner_id.clone(),
            RemoteProtocol::Stack as u16,
            &amount.to_string())?;
        let ret: JSONObject = self.client.put_object_with_resp2(self.req_path.as_str(), req.desc().object_id(), req.to_vec()?).await?;
        ret.get()
    }

    async fn bill(&self, asset: String, price: f64, memo: String) -> BuckyResult<TransResult> {
        let req = JSONObject::new(
            self.dec_id.clone(),
            self.owner_id.clone(),
            RemoteProtocol::Bill as u16,
            &BillReq {
                asset,
                price,
                memo
            })?;
        let ret: JSONObject = self.client.put_object_with_resp2(self.req_path.as_str(), req.desc().object_id(), req.to_vec()?).await?;
        ret.get()
    }

    async fn mint(&self, amount: &str) -> BuckyResult<TransResult> {
        let req = JSONObject::new(
            self.dec_id.clone(),
            self.owner_id.clone(),
            RemoteProtocol::Mint as u16,
            &amount.to_string())?;
        let ret: JSONObject = self.client.put_object_with_resp2(self.req_path.as_str(), req.desc().object_id(), req.to_vec()?).await?;
        ret.get()
    }

    async fn add_merkle(&self, order_id: &str, merkle_root: HashValue, data_block_count: u64) -> BuckyResult<TransResult> {
        let req = JSONObject::new(
            self.dec_id.clone(),
            self.owner_id.clone(),
            RemoteProtocol::AddMerkle as u16,
            &AddMerkleReq {
                order_id: order_id.to_string(),
                merkle_root: merkle_root.to_string(),
                data_block_count
            })?;
        let ret: JSONObject = self.client.put_object_with_resp2(self.req_path.as_str(), req.desc().object_id(), req.to_vec()?).await?;
        ret.get()
    }

    async fn challenge(&self, _order_id: &str, _data_id: u64, _hash_data: HashValue, _nonce: String) -> BuckyResult<TransResult> {
        unreachable!()
    }

    async fn add_challenge_resp(&self, order_id: &str, reply_hash: HashValue) -> BuckyResult<TransResult> {
        let req = JSONObject::new(
            self.dec_id.clone(),
            self.owner_id.clone(),
            RemoteProtocol::ChallengeResp as u16,
            &ChallengeRespReq {
                order_id: order_id.to_string(),
                reply_hash: reply_hash.to_string()
            })?;
        let ret: JSONObject = self.client.put_object_with_resp2(self.req_path.as_str(), req.desc().object_id(), req.to_vec()?).await?;
        ret.get()
    }

    async fn arbitration(&self, order_id: &str, data: Vec<u8>, cut_merkle: Vec<HashValue>) -> BuckyResult<TransResult> {
        let req = JSONObject::new(
            self.dec_id.clone(),
            self.owner_id.clone(),
            RemoteProtocol::Arbitration as u16,
            &ArbitrationReq {
                order_id: order_id.to_string(),
                data,
                cut_merkle: cut_merkle.iter().map(|v| v.to_string()).collect()
            })?;
        let ret: JSONObject = self.client.put_object_with_resp2(self.req_path.as_str(), req.desc().object_id(), req.to_vec()?).await?;
        ret.get()
    }

    async fn report_cyfs_info(&self, _info: &CyfsInfo) -> BuckyResult<TransResult> {
        unreachable!()
    }
}
