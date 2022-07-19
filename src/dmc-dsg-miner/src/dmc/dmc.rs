use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use cyfs_base::*;
use cyfs_chunk_lib::CHUNK_SIZE;
use cyfs_lib::SharedCyfsStack;
use cyfs_dsg_client::DsgContractObject;
use crate::{ContractChunkStore, ContractCursor, ContractMetaStore, CyfsInfo, DMCChallenge, DMCChallengeState, DMCClient, DSG_CHUNK_PIECE_SIZE, SimpleSignatureProvider};

#[derive(ProtobufEncode, ProtobufDecode, Clone, ProtobufTransform, Debug)]
#[cyfs_protobuf_type(crate::protos::DmcContractData)]
pub struct DMCContractData {
    pub order_id: String,
    pub miner_dmc_account: String,
    pub merkle_root: Option<HashValue>,
    pub chunk_size: Option<u32>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ChallengeState {
    RespChallenge,
    Arbitration,
}

pub struct DMC {
    dmc_client: DMCClient<SimpleSignatureProvider>,
    stack: Arc<SharedCyfsStack>,
    http_domain: String,
    contract_store: Arc<Box<dyn ContractMetaStore>>,
    raw_data_store: Arc<Box<dyn ContractChunkStore>>,
    challenge_state: Mutex<HashMap<String, ChallengeState>>,
}
pub type DMCRef = Arc<DMC>;

impl DMC {
    pub fn new(
        stack: Arc<SharedCyfsStack>,
        contract_store: Arc<Box<dyn ContractMetaStore>>,
        raw_data_store: Arc<Box<dyn ContractChunkStore>>,
        dmc_server: &str,
        dmc_account: &str,
        private_key: String,
        http_domain: String,
    ) -> BuckyResult<DMCRef> {
        let sign_provider = SimpleSignatureProvider::new(vec![private_key])?;
        let dmc_client = DMCClient::new(dmc_account, dmc_server, sign_provider);
        let dmc = DMCRef::new(Self {
            dmc_client,
            stack,
            http_domain,
            contract_store,
            raw_data_store,
            challenge_state: Mutex::new(Default::default())
        });

        let tmp_dmc = dmc.clone();
        async_std::task::spawn(async move {
            loop {
                if let Err(e) = tmp_dmc.check_challenge().await {
                    log::error!("check challenge err {}", e);
                }
                async_std::task::sleep(Duration::from_secs(60)).await;
            }
        });

        Ok(dmc)
    }

    pub async fn report_cyfs_info(&self) -> BuckyResult<()> {
        let addr = self.stack.local_device_id().to_string();
        self.dmc_client.report_cyfs_info(&CyfsInfo {
            addr,
            http: if self.http_domain.is_empty() {None} else {Some(self.http_domain.clone())}
        }).await?;
        Ok(())
    }

    async fn check_challenge(&self) -> BuckyResult<()> {
        let mut cursor = ContractCursor::new(self.contract_store.clone());
        loop {
            let contract_id = cursor.next().await;
            if contract_id.is_none() {
                break;
            }

            if let Ok(contract) = self.contract_store.get(contract_id.as_ref().unwrap()).await {
                let challenge_ret = self.dmc_client.get_challenge(contract.desc().content().witness.order_id.as_str(), None).await?;
                if challenge_ret.rows.len() == 0 {
                    continue;
                }

                let challenge = &challenge_ret.rows[0];
                if challenge.state == DMCChallengeState::ChallengeRequest as u32 {
                    let state = {
                        let state = self.challenge_state.lock().unwrap();
                        state.get(challenge.order_id.as_str()).map(|state| state.clone())
                    };
                    if state.is_none() {
                        if let Err(e) = self.resp_challenge(contract_id.as_ref().unwrap(), challenge).await {
                            log::info!("resp_challenge err {}", e);
                        }
                        let mut state = self.challenge_state.lock().unwrap();
                        state.insert(challenge.order_id.clone(), ChallengeState::RespChallenge);
                    } else if state.unwrap() == ChallengeState::RespChallenge {
                        if let Err(e) = self.arbitration(contract_id.as_ref().unwrap(), challenge).await {
                            log::info!("arbitration err {}", e);
                        }
                        let mut state = self.challenge_state.lock().unwrap();
                        state.insert(challenge.order_id.clone(), ChallengeState::Arbitration);
                    }
                }
            }
        }
        Ok(())
    }

    async fn resp_challenge(&self, contract_id: &ObjectId, challenge: &DMCChallenge) -> BuckyResult<()> {
        log::info!("resp_challenge {} {}", contract_id.to_string(), serde_json::to_string(challenge).unwrap());
        let contract = self.contract_store.get(contract_id).await?;
        let dmc_data = &contract.desc().content().witness;
        let chunk_list = self.contract_store.get_chunk_list(contract_id).await?;
        let data = self.raw_data_store.get_contract_data(chunk_list,
                                                         Range { start: challenge.data_id * DSG_CHUNK_PIECE_SIZE as u64, end: (challenge.data_id + 1) * DSG_CHUNK_PIECE_SIZE as u64 },
                                                         dmc_data.chunk_size.unwrap_or(CHUNK_SIZE as u32)).await?;
        let hash = hash_data(vec![data.as_slice(), challenge.nonce.as_bytes()].concat().as_slice());
        self.dmc_client.add_challenge_resp(challenge.order_id.as_str(), hash).await?;
        Ok(())
    }

    async fn arbitration(&self, contract_id: &ObjectId, challenge: &DMCChallenge) -> BuckyResult<()> {
        log::info!("arbitration {} {}", contract_id.to_string(), serde_json::to_string(challenge).unwrap());
        let contract = self.contract_store.get(contract_id).await?;
        let dmc_data = &contract.desc().content().witness;
        let chunk_list = self.contract_store.get_chunk_list(contract_id).await?;

        let mut merkle_tree = self.raw_data_store.get_merkle(
            chunk_list, contract_id, dmc_data.chunk_size.unwrap_or(CHUNK_SIZE as u32)).await?;
        let proof = merkle_tree.gen_proof(challenge.data_id).await?;
        self.dmc_client.arbitration(challenge.order_id.as_str(), proof.piece,
                                    proof.path_list.iter().map(|item|  HashValue::from(item)).collect()).await?;

        Ok(())
    }

    pub async fn check_contract(&self, source: &ObjectId, contract: &DsgContractObject<DMCContractData>) -> BuckyResult<bool> {
        let dmc_data = &contract.desc().content().witness;
        if dmc_data.merkle_root.is_some() {
            log::info!("recv contract dmc_order {} merkle_root {}", dmc_data.order_id.as_str(), dmc_data.merkle_root.as_ref().unwrap().to_string());
        } else {
            log::info!("recv contract dmc_order {}", dmc_data.order_id.as_str());
        }

        let order = self.dmc_client.get_order_of_miner(dmc_data.order_id.as_str()).await?;

        if order.is_some() {
            let cyfs_info = self.dmc_client.get_cyfs_info(order.as_ref().unwrap().user.clone()).await?;
            if cyfs_info.addr == source.to_string() {
                return Ok(true)
            } else {
                log::info!("address unmatch {} expect {}", source.to_string(), cyfs_info.addr.as_str());
            }
        } else {
            log::info!("can't find order {}", dmc_data.order_id.as_str());
        }
        Ok(false)
    }

    pub async fn report_merkle_hash(&self, contract_id: &ObjectId) -> BuckyResult<()> {
        let contract = self.contract_store.get(contract_id).await?;
        let dmc_data = &contract.desc().content().witness;
        let chunk_list = self.contract_store.get_chunk_list(contract_id).await?;
        let chunk_size = dmc_data.chunk_size.unwrap_or(CHUNK_SIZE as u32);
        let len = chunk_size as u64 * chunk_list.len() as u64;
        let piece_count = if len % DSG_CHUNK_PIECE_SIZE == 0 {len / DSG_CHUNK_PIECE_SIZE} else {len / DSG_CHUNK_PIECE_SIZE + 1};

        let merkle_tree = self.raw_data_store.get_merkle(chunk_list.clone(), contract_id, chunk_size).await?;
        let root = HashValue::from(merkle_tree.root());
        if dmc_data.merkle_root.is_some() {
            assert_eq!(&root, dmc_data.merkle_root.as_ref().unwrap());
        }

        self.dmc_client.add_merkle(dmc_data.order_id.as_str(), root, piece_count as u64).await?;

        Ok(())
    }
}
