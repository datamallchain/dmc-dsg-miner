
use std::{sync::Arc, time::Duration, collections::{BTreeSet, BTreeMap, HashSet}};
use async_std::{task::{sleep,spawn}};
use cyfs_base::*;
use cyfs_bdt::*;
use cyfs_dsg_client::*;
use crate::*;
use std::convert::{TryFrom, TryInto};
use std::sync::Mutex;
use cyfs_chunk_lib::CHUNK_SIZE;

pub struct DmcDsgMiner<
    CLIENT: CyfsClient,
    CONN: ContractMetaStore,
    METASTORE: MetaStore<CONN>,
    CHUNKSTORE: ContractChunkStore,
    DOWNLOADER: FileDownloader> {
    client: Arc<CLIENT>,
    meta_store: Arc<METASTORE>,
    raw_data_store: Arc<CHUNKSTORE>,
    downloader: DOWNLOADER,
    dmc: DMCRef<CLIENT, CONN, METASTORE, CHUNKSTORE>,
    dec_id: ObjectId,
    syncing_contracts: Mutex<HashSet<ObjectId>>,
    proof_contracts: Mutex<HashSet<ObjectId>>,
}

impl<CLIENT: CyfsClient,
    CONN: ContractMetaStore,
    METASTORE: MetaStore<CONN>,
    CHUNKSTORE: ContractChunkStore,
    DOWNLOADER: FileDownloader> DmcDsgMiner<CLIENT, CONN, METASTORE, CHUNKSTORE, DOWNLOADER> {
    pub fn new(client: Arc<CLIENT>, meta_store: Arc<METASTORE>, raw_data_store: Arc<CHUNKSTORE>, dmc: DMCRef<CLIENT, CONN, METASTORE, CHUNKSTORE>, dec_id: ObjectId, downloader: DOWNLOADER) -> Self {
        Self{
            client,
            meta_store,
            raw_data_store,
            downloader,
            dmc,
            dec_id,
            syncing_contracts: Mutex::new(Default::default()),
            proof_contracts: Mutex::new(Default::default())
        }
    }

    fn get_contract_lock_name(contract_id: &ObjectId) -> String {
        format!("miner_contract_{}", contract_id)
    }

    pub async fn challenge(&self, contract_id: &ObjectId, state_id: &ObjectId, challenge: &DsgChallengeObject, owner_id: &ObjectId) -> BuckyResult<()> {
        info!("start challenge contract_id {} state_id {}", contract_id.to_string(), state_id.to_string());
        let mut conn = self.meta_store.create_meta_connection_named_locked(Self::get_contract_lock_name(contract_id)).await?;
        let contract_info = conn.get_contract_info(contract_id).await?;
        match contract_info.contract_status {
            ContractStatus::Proof => {
                let state: DsgContractStateObject = self.client.get_object(Some(owner_id.clone()), state_id.clone()).await?;

                let state_ref = DsgContractStateObjectRef::from(&state);
                let mut is_add_chunk = false;
                match state_ref.state() {
                    DsgContractState::Initial => {
                        info!("Initial");
                    },
                    DsgContractState::DataSourceSyncing => {info!("DataSourceSyncing");},
                    DsgContractState::ContractBroken => {info!("ContractBroken");},
                    DsgContractState::ContractExecuted => {info!("ContractExecuted");},
                    DsgContractState::Reserve(c) => {
                        let chunk_list = c.chunks.clone();
                        info!("DataSourcePrepared {:?}", chunk_list);
                    },
                    DsgContractState::DataSourceChanged(c) => {
                        let chunk_list = c.chunks.clone();
                        info!("DataSourceChanged {:?}", chunk_list);
                    },
                    DsgContractState::DataSourceStored => {
                        info!("DataSourceStored");
                    },
                }

                conn.begin().await?;
                conn.save_challenge(contract_id, challenge).await?;
                conn.contract_set_add(&vec![contract_id.clone()]).await?;
                conn.commit().await?;
            },
            _ => {
                info!("contract {} status err {:?}", contract_id.to_string(), contract_info.contract_status);
            }
        }

        Ok(())
    }

    async fn build_meta_chunk_merkle_root(&self, data: &[u8], merkle_chunk_size: u32) -> BuckyResult<HashValue> {
        let leafs = if merkle_chunk_size % DSG_CHUNK_PIECE_SIZE as u32 == 0 { merkle_chunk_size / DSG_CHUNK_PIECE_SIZE as u32 } else { merkle_chunk_size / DSG_CHUNK_PIECE_SIZE as u32 + 1};
        let merkle = MerkleTree::create_from_raw(
            MerkleMemoryChunkReader::new(data, merkle_chunk_size),
            HashVecStore::<Vec<u8>>::new::<MemVecCache>(leafs as u64)?).await?;
        Ok(HashValue::from(merkle.root()))
    }

    async fn sync_contract_data(&self, contract_id: &ObjectId, state_id: &ObjectId, challenge: &DsgChallengeObject, owner_id: &ObjectId) -> BuckyResult<()> {
        let (contract, is_saved) = {
            let mut conn = self.meta_store.create_meta_connection().await?;
            let contract = conn.get_contract(contract_id).await?;
            if contract.is_some() {
                (contract.unwrap(), true)
            } else {
                let contract = self.client.get_object(Some(owner_id.clone()), contract_id.clone()).await?;
                conn.begin().await?;
                (contract, false)
            }
        };
        if !self.dmc.check_contract(owner_id, &contract).await? {
            return Err(BuckyError::new(BuckyErrorCode::InvalidData, "check contract failed"));
        }
        let mut conn = self.meta_store.create_meta_connection_named_locked(Self::get_contract_lock_name(contract_id)).await?;
        let state: DsgContractStateObject = self.client.get_object(Some(owner_id.clone()), state_id.clone()).await?;

        let state_ref = DsgContractStateObjectRef::from(&state);
        if let DsgContractState::DataSourceChanged(changed) = state_ref.state() {
            let contract_info = if is_saved {
                let mut contract_info = conn.get_contract_info(contract_id).await?;
                if contract_info.contract_status != ContractStatus::Proof {
                    let msg = format!("contract {} status {:?} error.expect Proof", contract_id.to_string(), contract_info.contract_status);
                    log::info!("{}", msg);
                    return Err(BuckyError::new(BuckyErrorCode::ErrorState, msg));
                }
                contract_info.contract_status = ContractStatus::Wait;
                contract_info
            } else {
                ContractInfo {
                    contract_status: ContractStatus::Wait,
                    latest_check_time: 0,
                    meta_merkle: vec![]
                }
            };

            conn.begin().await?;
            if !is_saved {
                conn.save_contract(&contract).await?;
            }
            conn.set_contract_info(contract_id, &contract_info).await?;
            conn.save_need_sync_contract_state(contract_id, &state).await?;
            conn.save_challenge(contract_id, challenge).await?;
            conn.contract_sync_set_add(&vec![contract_id.clone()]).await?;

            let mut cur_chunk_list = conn.get_chunk_list(contract_id).await?;
            cur_chunk_list.append(&mut changed.chunks.clone());
            let hash = hash_data(cur_chunk_list.to_vec()?.as_slice());
            if changed.stored_hash.is_none() || &hash != changed.stored_hash.as_ref().unwrap() {
                log::error!("contract {} hash unmatch {} {}", contract_id.to_string(), hash.to_string(), changed.stored_hash.as_ref().unwrap_or(&HashValue::default()).to_string());
                return Err(BuckyError::new(BuckyErrorCode::InvalidData, "chunk hash unmatch"));
            }
            conn.commit().await?;
        } else {
            return Err(BuckyError::new(BuckyErrorCode::ErrorState, format!("state {:?}", state_ref.state())));
        }

        info!("first challenge sync contract success, wait sync chunk");

        Ok(())
    }

    async fn get_wait_sync(&self) -> BuckyResult<Vec<ObjectId>> {
        let mut conn = self.meta_store.create_meta_connection().await?;
        let list = conn.contract_set().await?;
        let mut wait_list = Vec::new();
        for contract_id in list.iter() {
            let syncing_set = self.syncing_contracts.lock().unwrap();
            if !syncing_set.contains(contract_id) {
                wait_list.push(contract_id.clone());
            }
        }
        Ok(wait_list)
    }

    async fn build_merkle_root(&self, chunk_merkle_root_list: &Vec<HashValue>) -> BuckyResult<HashValue> {
        let mut hash_store = HashVecStore::<Vec<u8>>::new::<MemVecCache>(chunk_merkle_root_list.len() as u64)?;
        for (index, hash) in chunk_merkle_root_list.iter().enumerate() {
            hash_store.set_node(0, index as u64, hash.as_slice().try_into().unwrap()).await?;
        }

        let merkle_tree = MerkleTree::<async_std::io::Cursor<Vec<u8>>, HashVecStore<Vec<u8>>>::create_from_base(
            None,
            hash_store,
            0).await?;
        Ok(HashValue::from(merkle_tree.root()))
    }

    async fn sync_contract_data_proc(&self, contract_id: ObjectId) -> BuckyResult<()> {
        app_call_log!("sync contract: {}", &contract_id);
        let mut conn = self.meta_store.create_meta_connection().await?;
        let contract_info = conn.get_contract_info(&contract_id).await?;
        if contract_info.contract_status != ContractStatus::Wait {
            return Ok(());
        }
        let contract = conn.get_contract(&contract_id).await?;
        assert!(contract.is_some());
        let contract_ref = DsgContractObjectRef::from(contract.as_ref().unwrap());
        let contract_state = conn.get_syncing_contract_state(&contract_id).await?;
        assert!(contract_state.is_some());
        let state_ref = DsgContractStateObjectRef::from(contract_state.as_ref().unwrap());
        if let DsgContractState::DataSourceChanged(change) = state_ref.state() {
            let dest_id = self.client.resolve_ood(contract_ref.consumer().clone()).await?;
            self.downloader.download(
                change.chunks.clone(),
                vec![DeviceId::try_from(dest_id)?],
                DownloadParams { padding_len: contract_ref.witness().chunk_size.unwrap_or(CHUNK_SIZE as u32 )}).await?;

            let mut conn = self.meta_store.create_meta_connection_named_locked(Self::get_contract_lock_name(&contract_id)).await?;
            conn.begin().await?;
            let mut contract_info = conn.get_contract_info(&contract_id).await?;
            let mut cur_chunk_list = conn.get_chunk_list(&contract_id).await?;
            cur_chunk_list.append(&mut change.chunks.clone());
            let hash = hash_data(cur_chunk_list.to_vec()?.as_slice());
            assert_eq!(&hash, change.stored_hash.as_ref().unwrap());
            let mut state_list = Vec::new();
            state_list.push(contract_state.as_ref().unwrap().clone());
            loop {
                let cur_state_ref = DsgContractStateObjectRef::from(state_list.get(state_list.len() - 1).unwrap());
                if let DsgContractState::DataSourceChanged(change) = cur_state_ref.state() {
                    if change.prev_change.is_none() {
                        break;
                    }
                    let prev_state = conn.get_state(change.prev_change.clone().unwrap()).await?;
                    if prev_state.is_none() {
                        conn.contract_sync_set_remove(&vec![contract_id.clone()]).await?;
                        conn.commit().await?;
                        return Ok(());
                    }
                    state_list.push(prev_state.unwrap());
                } else {
                    assert!(false);
                }
            }
            let meta_data = MetaData {
                contract: contract.clone().unwrap(),
                state_list
            };
            let meta_block = meta_data.to_vec()?;
            let mut meta_ref = &meta_block[..];
            let chunk_size = contract_ref.witness().chunk_size.unwrap_or(CHUNK_SIZE as u32 ) as usize;
            let mut chunk_hash_list = Vec::new();
            if meta_ref.len() > chunk_size {
                let hash = self.build_meta_chunk_merkle_root(&meta_ref[..chunk_size], chunk_size as u32).await?;
                chunk_hash_list.push(hash);
                meta_ref = &meta_ref[chunk_size..];
            }
            let hash = self.build_meta_chunk_merkle_root(meta_ref, chunk_size as u32).await?;
            chunk_hash_list.push(hash);
            contract_info.meta_merkle = chunk_hash_list.clone();

            let mut chunk_merkle_root_list = conn.get_chunk_merkle_root(
                &cur_chunk_list,
                chunk_size as u32).await?.into_iter().map(|v|v.1).collect();
            chunk_hash_list.append(&mut chunk_merkle_root_list);
            let file_size = (chunk_hash_list.len() * chunk_size) as u64;
            let data_block_count = if file_size % DSG_CHUNK_PIECE_SIZE == 0 { file_size / DSG_CHUNK_PIECE_SIZE} else { file_size / DSG_CHUNK_PIECE_SIZE + 1};
            let merkle_root = self.build_merkle_root(&chunk_hash_list).await?;
            if let Err(e) = self.dmc.report_merkle_hash(&contract_id, merkle_root, data_block_count as u64).await {
                if get_app_err_code(&e) == DMC_DSG_ERROR_MERKLE_ROOT_VERIFY_FAILED {
                    conn.contract_sync_set_remove(&vec![contract_id.clone()]).await?;
                } else {
                    return Err(e);
                }
            } else {
                conn.save_chunk_list(&contract_id, cur_chunk_list).await?;
                contract_info.contract_status = ContractStatus::Success;
                conn.set_contract_info(&contract_id, &contract_info).await?;
                conn.set_contract_state_sync_complete(&contract_id, &state_ref.id()).await?;
                conn.contract_sync_set_remove(&vec![contract_id.clone()]).await?;
                conn.contract_proof_set_add(&vec![contract_id.clone()]).await?;
                conn.chunk_ref_add(&contract_id, &change.chunks).await?;
                conn.chunk_del_list_del(&change.chunks).await?;
                if change.prev_change.is_none() {
                    conn.contract_set_add(&vec![contract_id.clone()]).await?;
                }
            }

        }
        conn.commit().await?;

        Ok(())
    }
    pub async fn start_chunk_sync(self: &Arc<Self>) -> BuckyResult<()> {
        let this = self.clone();

        spawn( async move {
            loop {
                trace!("start sync chunk data");
                match this.get_wait_sync().await {
                    Ok(vecs) => {
                        for contract_id in vecs {
                            {
                                let mut syncing_contract = this.syncing_contracts.lock().unwrap();
                                syncing_contract.insert(contract_id.clone());
                            }
                            let this = this.clone();
                            spawn( async move {
                                let ret = this.sync_contract_data_proc(contract_id).await;
                                {
                                    let mut syncing_contract = this.syncing_contracts.lock().unwrap();
                                    syncing_contract.remove(&contract_id);
                                }
                                if let Err(e) = ret {
                                    log::error!("sync contract {} err {}", contract_id.to_string(), e);
                                }
                            });
                        }
                    }
                    Err(_e) => {
                        //error!("{}", e);
                        info!("no data wait sync")
                    }
                }
                sleep(Duration::from_secs(5)).await;
            }
        });
        Ok(())
    }

    async fn check_contract_end(&self) -> BuckyResult<()> {
        let mut conn = self.meta_store.create_meta_connection().await?;
        let contract_list = conn.contract_set().await?;
        for contract_id in contract_list.iter() {
            let contract_info = conn.get_contract_info(contract_id).await?;
            if bucky_time_now() - contract_info.latest_check_time < 7*24*3600*1000000 {
                continue;
            }
            let contract = conn.get_contract(contract_id).await?;
            assert!(contract.is_some());
            let contract_ref = DsgContractObjectRef::from(contract.as_ref().unwrap());
            match self.dmc.get_order(contract_ref.witness().order_id.as_str()).await {
                Ok(order) => {
                    if order.is_some() {
                        let mut conn = self.meta_store.create_meta_connection_named_locked(Self::get_contract_lock_name(contract_id)).await?;
                        conn.begin().await?;
                        let mut contract_info = conn.get_contract_info(contract_id).await?;
                        if order.as_ref().unwrap().state == DMCOrderState::OrderStateEnd as u8 {
                            log::error!("contract {} end.dmc order {}", contract_id.to_string(), contract_ref.witness().order_id.as_str());
                            contract_info.contract_status = ContractStatus::ContractOutTime;
                            let chunk_list = conn.get_chunk_list(contract_id).await?;
                            conn.chunk_ref_del(contract_id, &chunk_list).await?;
                            conn.chunk_del_list_del(&chunk_list).await?;
                            conn.contract_set_remove(&vec![contract_id.clone()]).await?;
                        } else {
                            contract_info.latest_check_time += 7*24*3600*1000000;
                        }
                        conn.set_contract_info(contract_id, &contract_info).await?;
                        conn.commit().await?;
                    }
                },
                Err(e) => {
                    log::error!("get {} order err {}", contract_ref.witness().order_id.as_str(), e);
                }
            }

        }
        Ok(())
    }

    pub async fn start_contract_end_check(self: &Arc<Self>) {
        let this = self.clone();

        spawn( async move {
            loop {
                if let Err(e) = this.check_contract_end().await {
                    error!("check out time err: {}", e);
                }
                sleep(Duration::from_secs(600)).await;
            }
        });
    }

    async fn resp_contract_proof(&self, contract_id: ObjectId) -> BuckyResult<()> {
        app_call_log!("start proof contract: {}", &contract_id);
        let mut conn = self.meta_store.create_meta_connection_named_locked(Self::get_contract_lock_name(&contract_id)).await?;
        if let Ok(challenge) = conn.get_challenge(&contract_id).await {
            let challenge_ref = DsgChallengeObjectRef::from(&challenge);
            if challenge_ref.expire_at() < bucky_time_now() {
                conn.contract_sync_set_remove(&vec![contract_id.clone()]).await?;
                conn.commit().await?;
                return Ok(());
            }
            let contract = conn.get_contract(&contract_id).await?;
            assert!(contract.is_some());
            let contract = contract.unwrap();
            let contract_ref = DsgContractObjectRef::from(&contract);
            let chunk_list = match challenge_ref.challenge_type() {
                ChallengeType::Full => {
                    conn.get_chunk_list(&contract_id).await?
                }
                ChallengeType::State => {
                    let state = conn.get_state(challenge_ref.contract_state().clone()).await?;
                    if state.is_none() {
                        conn.contract_sync_set_remove(&vec![contract_id.clone()]).await?;
                        conn.commit().await?;
                        return Ok(());
                    }
                    let state_ref = DsgContractStateObjectRef::from(state.as_ref().unwrap());
                    if let DsgContractState::DataSourceChanged(change) = state_ref.state() {
                        change.chunks.clone()
                    } else {
                        assert!(false);
                        Vec::new()
                    }
                }
            };
            {
                conn.release_locker();
            }
            let owner_id = contract_ref.consumer();
            let chunk_reder = Box::new(MinerChunkReader::new(self.raw_data_store.clone()));
            if chunk_list.len() < 50 {
                info!("challenge: {} chunks: {:?}", &challenge_ref, &chunk_list);
            } else {
                info!("challenge: {} chunks: {:?}", &challenge_ref, &chunk_list[0..5]);
            }

            if let Ok(proof) = DsgProofObjectRef::proove(challenge_ref, &chunk_list, chunk_reder).await {
                let proof_ref = DsgProofObjectRef::from(&proof);
                let ood_id = self.client.resolve_ood(owner_id.clone()).await?;

                if let Err(e) = self.client.put_object_with_resp2::<DsgProofObject>(ood_id, proof_ref.id(), proof_ref.as_ref().to_vec()?).await {
                    error!("contract {} verify proof err: {:?}", contract_id.to_string(), e)
                } else {
                    info!("contract {} proof success", contract_id.to_string());
                    let mut conn = self.meta_store.create_meta_connection_named_locked(Self::get_contract_lock_name(&contract_id)).await?;
                    conn.begin().await?;
                    conn.contract_sync_set_remove(&vec![contract_id.clone()]).await?;
                    let mut contract_info = conn.get_contract_info(&contract_id).await?;
                    if contract_info.contract_status == ContractStatus::Success {
                        contract_info.contract_status = ContractStatus::Proof;
                        conn.set_contract_info(&contract_id, &contract_info).await?;
                    }
                    conn.commit().await?;
                }
            }
        }
        Ok(())
    }

    pub async fn start_proof_resp(self: &Arc<Self>) {
        let this = self.clone();
        spawn( async move {
            loop {
                trace!("start proof chunk data");
                if let Ok(mut conn) = this.meta_store.create_meta_connection().await {
                    match conn.contract_proof_set().await {
                        Ok(vecs) => {
                            for contract_id in vecs {
                                if let Err(e) = this.resp_contract_proof(contract_id).await {
                                    log::error!("resp contract {} proof failed {}", contract_id.to_string(), e);
                                }
                            }
                        }
                        Err(e) => {
                            //error!("{}", e);
                            info!("no data wait proof");
                        }
                    }
                }
                sleep(Duration::from_secs(5)).await;
            }
        });
    }

    pub async fn need_sync_chunk(&self, contract_id: &ObjectId, state_id: &ObjectId) -> BuckyResult<bool> {
        let mut conn = self.meta_store.create_meta_connection().await?;
        let state = conn.get_contract_state(contract_id).await?;
        if state.is_none() {
            Ok(true)
        } else {
            let state = state.unwrap();
            let cur_state_id = state.desc().calculate_id();
            if &cur_state_id == state_id {
                Ok(false)
            } else {
                Ok(true)
            }
        }
    }

    pub async fn on_challenge(&self, challenge: DsgChallengeObject, source: ObjectId) -> BuckyResult<()> {
        let challenge_ref = DsgChallengeObjectRef::from(&challenge);
        let contract_id = challenge_ref.contract_id();
        let state_id = challenge_ref.contract_state();
        match self.need_sync_chunk(contract_id, state_id).await? {
            true => {
                self.sync_contract_data(contract_id, state_id, &challenge, &source).await?;
            },
            false => {
                self.challenge(contract_id, state_id, &challenge, &source).await?;
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct MinerChunkReader<CHUNKSTORE: ContractChunkStore> {
    raw_data_store: Arc<CHUNKSTORE>
}
impl<CHUNKSTORE: ContractChunkStore> MinerChunkReader<CHUNKSTORE> {
    fn new(raw_data_store: Arc<CHUNKSTORE>) -> Self {
        Self{
            raw_data_store
        }
    }
}

#[async_trait::async_trait]
impl<CHUNKSTORE: ContractChunkStore> ChunkReader for MinerChunkReader<CHUNKSTORE> {
    fn clone_as_reader(&self) -> Box<dyn ChunkReader> {
        let reader = Self {
            raw_data_store: self.raw_data_store.clone()
        };
        Box::new(reader)
    }

    async fn exists(&self, chunk: &ChunkId) -> bool {
        self.raw_data_store.chunk_exists(chunk).await
    }

    async fn get(&self, chunk: &ChunkId) -> BuckyResult<Arc<Vec<u8>>> {
        Ok(Arc::new(self.raw_data_store.get_chunk(chunk.clone()).await?))
    }
}

#[derive(RawEncode, RawDecode, Clone, Debug)]
pub struct ContractChunkList(pub Vec<ChunkId>);

#[derive(RawEncode, RawDecode, Clone, Debug)]
pub struct ContractSyncStatus(pub i64);

#[derive(RawEncode, RawDecode, Clone, Debug)]
pub struct ContractList(pub BTreeSet<ObjectId>);

#[derive(RawEncode, RawDecode, Clone, Debug)]
pub struct ChunkRefMap(pub BTreeMap<ChunkId, BTreeSet<ObjectId>>);

#[derive(RawEncode, RawDecode, Clone, Debug)]
pub struct DelSet(pub BTreeSet<ChunkId>);

#[derive(RawEncode, RawDecode, Clone, Debug)]
pub struct LastCheckTime(pub u64);
