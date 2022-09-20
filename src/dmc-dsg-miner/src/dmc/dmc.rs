use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use cyfs_base::*;
use cyfs_chunk_lib::{Chunk, CHUNK_SIZE, MemChunk};
use cyfs_dsg_client::{DsgContractObject, DsgContractObjectRef};
use crate::*;

#[derive(ProtobufEncode, ProtobufDecode, Clone, ProtobufTransform, Debug)]
#[cyfs_protobuf_type(crate::protos::DmcContractData)]
pub struct DMCContractData {
    pub order_id: String,
    pub miner_dmc_account: String,
    pub merkle_root: Option<HashValue>,
    pub chunk_size: Option<u32>,
}

pub enum DMCCommand {
    GetOrderInfo,
    GetOrderInfoResp
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ChallengeState {
    RespChallenge,
    Arbitration,
}

pub struct DMC<
    STACK: CyfsClient,
    CONN: ContractMetaStore,
    METASTORE: MetaStore<CONN>,
    CHUNKSTORE: ContractChunkStore,> {
    dmc_client: DMCClient<SimpleSignatureProvider>,
    stack: Arc<STACK>,
    http_domain: String,
    contract_store: Arc<METASTORE>,
    raw_data_store: Arc<CHUNKSTORE>,
    challenge_state: Mutex<HashMap<String, ChallengeState>>,
    _marker: std::marker::PhantomData<CONN>,
}
pub type DMCRef<STACK, CONN, METASTORE, CHUNKSTORE> = Arc<DMC<STACK, CONN, METASTORE, CHUNKSTORE>>;

impl<
    STACK: CyfsClient,
    CONN: ContractMetaStore,
    METASTORE: MetaStore<CONN>,
    CHUNKSTORE: ContractChunkStore,> DMC<STACK, CONN, METASTORE, CHUNKSTORE> {
    pub fn new(
        stack: Arc<STACK>,
        contract_store: Arc<METASTORE>,
        raw_data_store: Arc<CHUNKSTORE>,
        dmc_server: &str,
        dmc_account: &str,
        private_key: String,
        http_domain: String,
    ) -> BuckyResult<DMCRef<STACK, CONN, METASTORE, CHUNKSTORE>> {
        let sign_provider = SimpleSignatureProvider::new(vec![private_key])?;
        let dmc_client = DMCClient::new(dmc_account, dmc_server, sign_provider);
        let dmc = DMCRef::new(Self {
            dmc_client,
            stack,
            http_domain,
            contract_store,
            raw_data_store,
            challenge_state: Mutex::new(Default::default()),
            _marker: Default::default()
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
        let addr = self.stack.local_device().desc().owner().as_ref().unwrap().to_string();
        self.dmc_client.report_cyfs_info(&CyfsInfo {
            addr,
            http: if self.http_domain.is_empty() {None} else {Some(self.http_domain.clone())},
            v: Some(2)
        }).await?;
        Ok(())
    }

    async fn check_challenge(&self) -> BuckyResult<()> {
        let mut conn = self.contract_store.create_meta_connection().await?;
        let contract_list = conn.contract_set().await?;
        for contract_id in contract_list.iter() {
            if let Ok(Some(contract)) = conn.get_contract(contract_id).await {
                let contract_ref = DsgContractObjectRef::from(&contract);
                let witness = contract_ref.witness();
                let chunk_size = witness.chunk_size.unwrap_or(CHUNK_SIZE as u32);
                let challenge_ret = self.dmc_client.get_challenge_info(contract.desc().content().witness.order_id.as_str(), None).await?;
                if challenge_ret.rows.len() == 0 {
                    continue;
                }

                let challenge = &challenge_ret.rows[0];
                if challenge.state == DMCChallengeState::ChallengeRequest as u32 {
                    let state = {
                        let state = self.challenge_state.lock().unwrap();
                        state.get(challenge.order_id.as_str()).map(|state| state.clone())
                    };
                    let contract_info = conn.get_contract_info(contract_id).await?;
                    let meta_max_id = contract_info.meta_merkle.len() as u64 * chunk_size as u64 / DSG_CHUNK_PIECE_SIZE;
                    if state.is_none() {
                        let data = if challenge.data_id < meta_max_id {
                            let meta_data = conn.get_contract_meta_data(contract_id).await?;
                            let mut meta_buf = meta_data.to_vec()?;
                            meta_buf.resize(contract_info.meta_merkle.len() * chunk_size as usize, 0);
                            let start = (challenge.data_id * DSG_CHUNK_PIECE_SIZE) as usize;
                            let end = ((challenge.data_id + 1) * DSG_CHUNK_PIECE_SIZE) as usize;
                            let data = meta_buf[start..end].to_vec();
                            data
                        } else {
                            let chunk_list = conn.get_chunk_list(contract_id).await?;
                            let data = self.raw_data_store.get_contract_data(chunk_list,
                                                                             Range { start: (challenge.data_id - meta_max_id) * DSG_CHUNK_PIECE_SIZE as u64, end: ((challenge.data_id - meta_max_id) + 1) * DSG_CHUNK_PIECE_SIZE as u64 },
                                                                             chunk_size).await?;
                            data
                        };
                        let hash = hash_data(vec![data.as_slice(), challenge.nonce.as_bytes()].concat().as_slice());
                        self.dmc_client.add_challenge_resp(witness.order_id.as_str(), hash).await?;
                        let mut state = self.challenge_state.lock().unwrap();
                        state.insert(challenge.order_id.clone(), ChallengeState::RespChallenge);
                    } else if state.unwrap() == ChallengeState::RespChallenge {
                        let chunk_map = if challenge.data_id < meta_max_id {
                            let meta_data = conn.get_contract_meta_data(contract_id).await?.to_vec()?;
                            let mut chunk_list: HashMap<ChunkId, Box<dyn Chunk>> = HashMap::new();
                            let mut meta_ptr = meta_data.as_slice();
                            while meta_ptr.len() > chunk_size as usize {
                                let chunk = Box::new(MemChunk::from(meta_ptr[..chunk_size as usize].to_vec()));
                                chunk_list.insert(chunk.calculate_id(), chunk);
                                meta_ptr = &meta_ptr[chunk_size as usize..];
                            }
                            let chunk = Box::new(MemChunk::from(meta_ptr.to_vec()));
                            chunk_list.insert(chunk.calculate_id(), chunk);
                            Some(chunk_list)
                        } else {
                            None
                        };
                        let chunk_list = conn.get_chunk_list(contract_id).await?;
                        let mut chunk_hash_list = conn.get_chunk_merkle_root(&chunk_list, chunk_size).await?;
                        let mut hash_list: Vec<(ChunkId, HashValue)> = contract_info.meta_merkle.iter().map(|v| (ChunkId::default(), v.clone())).collect();
                        hash_list.append(&mut chunk_hash_list);
                        let chunk_list: Vec<ChunkId> = hash_list.iter().map(|v| v.0.clone()).collect();
                        let reader = AsyncMerkleChunkReader::new(MerkleChunkReader::new(
                            self.raw_data_store.clone(), chunk_list, chunk_size, chunk_map));
                        let mut merkle_tree = self.build_merkle_tree(reader, hash_list, chunk_size).await?;
                        let proof = merkle_tree.gen_proof(challenge.data_id).await?;
                        self.dmc_client.arbitration(challenge.order_id.as_str(), proof.piece,
                                                    proof.path_list.iter().map(|item|  HashValue::from(item)).collect()).await?;
                        let mut state = self.challenge_state.lock().unwrap();
                        state.insert(challenge.order_id.clone(), ChallengeState::Arbitration);
                    }
                }
            }
        }
        Ok(())
    }

    async fn build_merkle_tree<
        READ: async_std::io::Read + async_std::io::Seek + Send + Unpin>(&self, reader: READ, hash_list: Vec<(ChunkId, HashValue)>, chunk_size: u32) -> BuckyResult<MerkleTree<READ, MinerHashStore<Vec<u8>, CONN, METASTORE>>> {
        let leafs = chunk_size as u64 / DSG_CHUNK_PIECE_SIZE;
        let mut layer = 0;
        let mut count = 1;

        loop {
            if count == leafs {
                break;
            } else if count > leafs {
                let msg = format!("leafs count err {}", leafs);
                log::error!("{}", msg);
                return Err(BuckyError::new(BuckyErrorCode::Failed, msg));
            }
            count = count * 2;
            layer += 1;
        }

        let hash_store = MinerHashStore::<Vec<u8>, _, _>::new::<MemVecCache>(
            layer,
            chunk_size,
            hash_list,
            self.contract_store.clone())?;
        let merkle_tree = MerkleTree::create_from_base(Some(reader), hash_store, layer).await?;
        Ok(merkle_tree)
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

    pub async fn report_merkle_hash(&self, contract_id: &ObjectId, merkle_root: HashValue, piece_count: u64) -> BuckyResult<()> {
        let mut conn = self.contract_store.create_meta_connection().await?;
        let contract = conn.get_contract(contract_id).await?;
        assert!(contract.is_some());
        let contract_ref = DsgContractObjectRef::from(contract.as_ref().unwrap());
        let dmc_data = contract_ref.witness();

        let challenge_info = self.dmc_client.get_challenge_info(dmc_data.order_id.as_str(), None).await?;
        if challenge_info.rows.len() == 0 {
            return Err(app_err!(DMC_DSG_ERROR_MERKLE_ROOT_VERIFY_FAILED, "get order {} challenge info failed", dmc_data.order_id.as_str()));
        }

        let info = &challenge_info.rows[0];
        if info.state != DMCChallengeState::ChallengePrepare as u32 {
            return Err(app_err!(DMC_DSG_ERROR_MERKLE_ROOT_VERIFY_FAILED, "order {} state is {}, expect ChallengePrepare", dmc_data.order_id.as_str(), info.state));
        }

        if info.pre_merkle_root != merkle_root.to_string() || info.pre_merkle_block_count != piece_count {
            return Err(app_err!(DMC_DSG_ERROR_MERKLE_ROOT_VERIFY_FAILED, "order {} merkle root is unmatched.user committed {} {}, miner {} {}",
                dmc_data.order_id.as_str(), info.pre_merkle_root, info.pre_merkle_block_count, merkle_root.to_string(), piece_count));
        }

        self.dmc_client.add_merkle(dmc_data.order_id.as_str(), merkle_root, piece_count).await?;

        Ok(())
    }

    pub async fn get_order(&self, order_id: &str) -> BuckyResult<Option<DMCOrder>> {
        self.dmc_client.get_order_by_id(order_id).await
    }
}
