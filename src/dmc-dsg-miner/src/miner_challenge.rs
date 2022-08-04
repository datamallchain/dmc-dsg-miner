
use std::{path::PathBuf, sync::Arc, time::Duration, collections::{BTreeSet, BTreeMap, HashSet}};
use async_std::{task::{sleep,spawn}};
use cyfs_base::*;
use cyfs_lib::*;
use cyfs_bdt::*;
use cyfs_dsg_client::*;
use crate::*;
use cyfs_core::*;
use std::str::FromStr;
use std::convert::TryFrom;

pub struct MinerChallenge {
    stack: Arc<SharedCyfsStack>,
    meta_store: Arc<Box<dyn ContractMetaStore>>,
    raw_data_store: Arc<Box<dyn ContractChunkStore>>,
    dmc: DMCRef,
}

impl MinerChallenge {
    pub fn new(stack: Arc<SharedCyfsStack>, meta_store: Arc<Box<dyn ContractMetaStore>>, raw_data_store: Arc<Box<dyn ContractChunkStore>>, dmc: DMCRef) -> Self {
        Self{
            stack,
            meta_store,
            raw_data_store,
            dmc
        }
    }

    pub async fn challenge(&self, interface: &DsgMinerInterface, contract_id: &ObjectId, state_id: &ObjectId, challenge: &DsgChallengeObject, owner_id: &ObjectId) -> BuckyResult<()> {
        info!("start challenge");
        match self.meta_store.get_down_status(contract_id).await? {
            SyncStatus::Proof => {
                let state: DsgContractStateObject = self.get_object(Some(owner_id.clone()), state_id.clone()).await?;
                
                let state_ref = DsgContractStateObjectRef::from(&state);
                let mut is_add_chunk = false;
                match state_ref.state() {
                    DsgContractState::Initial => {
                        info!("Initial");
                    },
                    DsgContractState::DataSourceSyncing => {info!("DataSourceSyncing");},
                    DsgContractState::ContractBroken => {info!("ContractBroken");},
                    DsgContractState::ContractExecuted => {info!("ContractExecuted");},
                    DsgContractState::DataSourcePrepared(c) => {
                        let chunk_list = c.chunks.clone();
                        info!("DataSourcePrepared {:?}", chunk_list);
                        is_add_chunk = self.check_chunks_sync_state(contract_id, chunk_list).await?;
                    },
                    DsgContractState::DataSourceChanged(c) => {
                        let chunk_list = c.chunks.clone();
                        info!("DataSourceChanged {:?}", chunk_list);
                        is_add_chunk = self.check_chunks_sync_state(contract_id, chunk_list).await?;
                    },
                    DsgContractState::DataSourceStored => {
                        info!("DataSourceStored");
                    },
                }
                
                self.meta_store.save_stat(contract_id, &state).await?;
                self.meta_store.save_challenge(contract_id, challenge).await?;
                //self.save_chunks(contract_id, &state).await?;

                if !is_add_chunk {
                    self.to_challenge(interface, contract_id, challenge, owner_id).await?;
                    info!("{} challenge success", contract_id);
                }
                
            },
            _ => {
                info!("wait sync chunks");
            }
        }

        Ok(())
    }

    pub async fn check_chunks_sync_state(&self, contract_id: &ObjectId, chunk_list: Vec<ChunkId>) -> BuckyResult<bool> {
        let (is_mut_size, out_size) = self.check_contract_size(contract_id, chunk_list.clone()).await?;
        if is_mut_size && out_size {
            return Err(BuckyError::from("chunk total size out of contract limit"));
        }

        let mut cur_cks = HashSet::new();
        cur_cks.extend(chunk_list.clone());

        let list = self.meta_store.get_chunk_list(contract_id).await?;
        let mut old_cks = HashSet::new();
        old_cks.extend(list.clone());

        let mut is_change = false;
        let new_sync_chunks = cur_cks.difference(&old_cks).cloned().collect::<Vec<_>>();
        if new_sync_chunks.len() > 0 {
            is_change = true;
        }

        let del_chunks_list = old_cks.difference(&cur_cks).cloned().collect::<Vec<_>>();
        if del_chunks_list.len() > 0 {
            is_change = true;
            for chunk_id in &del_chunks_list {
                if let Err(e) = self.meta_store.chunk_ref_del(contract_id, chunk_id).await {
                    error!("chunk ref del err: {:?}", e);
                }
                if let Err(e) = self.meta_store.chunk_del_list_add(chunk_id).await {
                    error!("del list add err: {:?}", e);
                }
            }
        }

        if is_change {
            self.meta_store.save_chunk_list(contract_id, chunk_list).await?;
        }

        let mut is_add_chunk = false;
        if new_sync_chunks.len() > 0 {
            if let Err(e) = self.meta_store.update_down_status(&contract_id, SyncStatus::Wait).await {
                error!("change down status err: {:?}", e);
            }
            is_add_chunk = true;
        }
        
        Ok(is_add_chunk)
    }

    pub async fn check_contract_size(&self, contract_id: &ObjectId, chunk_list: Vec<ChunkId>) -> BuckyResult<(bool, bool)> {
        let contract: DsgContractObject<DMCContractData> = self.meta_store.get(contract_id).await?;
        let contract_ref = DsgContractObjectRef::from(&contract);
        let ds = contract_ref.data_source();
        let mut is_mut_size = false;
        match ds {
            DsgDataSource::Mutable(all_size) => {
                is_mut_size = true;
                let mut clen = 0;
                for chunk in chunk_list {
                    clen = chunk.len();
                }
                if (clen as u64) < all_size.clone() {
                    return Ok((is_mut_size, true));
                }
            },
            _ => ()
        }

        Ok((is_mut_size, false))
    }

    pub async fn first_sync_contract(&self, _interface: &DsgMinerInterface, contract_id: &ObjectId, state_id: &ObjectId, challenge: &DsgChallengeObject, owner_id: &ObjectId) -> BuckyResult<()> {
        let contract: DsgContractObject<DMCContractData> = self.get_object(Some(owner_id.clone()), contract_id.clone()).await?;
        if !self.dmc.check_contract(owner_id, &contract).await? {
            return Err(BuckyError::new(BuckyErrorCode::InvalidData, "check contract failed"));
        }
        let state: DsgContractStateObject = self.get_object(Some(owner_id.clone()), state_id.clone()).await?;
        
        let state_ref = DsgContractStateObjectRef::from(&state);
        //let mut chunk_list = vec![];
        match state_ref.state() {
            DsgContractState::Initial => {
                info!("Initial");
            },
            DsgContractState::DataSourceSyncing => {info!("DataSourceSyncing");},
            DsgContractState::ContractBroken => {info!("ContractBroken");},
            DsgContractState::ContractExecuted => {info!("ContractExecuted");},
            DsgContractState::DataSourcePrepared(c) => {
                let chunk_list = c.chunks.clone();
                info!("DataSourcePrepared {:?}", &chunk_list);
                
            },
            DsgContractState::DataSourceChanged(c) => {
                let chunk_list = c.chunks.clone();
                info!("DataSourceChanged {:?}", &chunk_list);
            },
            DsgContractState::DataSourceStored => {
                info!("DataSourceStored");
            },
        }

        self.meta_store.save(contract_id, &contract).await?;
        self.meta_store.save_stat(contract_id, &state).await?;
        self.meta_store.save_challenge(contract_id, challenge).await?;
        self.meta_store.save_owner(contract_id, owner_id).await?;
        //self.meta_store.save_chunk_list(contract_id, chunk_list).await?;
        //self.save_chunks(contract_id, &state).await?;

        //self.to_challenge(interface, contract_id, challenge, owner_id).await?;
        info!("first challenge sync contract success, wait sync chunk");

        Ok(())
    }

    pub async fn to_challenge(&self, interface: &DsgMinerInterface, contract_id: &ObjectId, challenge: &DsgChallengeObject, owner_id: &ObjectId) -> BuckyResult<()> {
       let chunk_list = self.meta_store.get_chunk_list(contract_id).await?;
        let challenge_ref = DsgChallengeObjectRef::from(challenge);
        let chunk_reder = Box::new(MinerChunkReader::new(self.raw_data_store.clone()));

        let proof = DsgProofObjectRef::proove(challenge_ref, &chunk_list, chunk_reder).await?;
        let proof_ref = DsgProofObjectRef::from(&proof);
        interface.verify_proof(proof_ref, DeviceId::try_from(owner_id.clone()).unwrap()).await?;

        Ok(())
    }

    pub async fn save_chunks(&self, contract_id: &ObjectId, state: &DsgContractStateObject) -> BuckyResult<()> {
        match state.desc().content().state {
            DsgContractState::DataSourceChanged(ref data_source) => {
                let chunk_list = data_source.chunks.clone();
                self.meta_store.save_chunk_list(contract_id, chunk_list).await?;
            },
            _ =>()
        }

        Ok(())
    }

    pub async fn get_contract_cursor(&self) -> ContractCursor {
        ContractCursor::new(self.meta_store.clone())
    }

    pub async fn sync_chunk_data(&self) {
        let meta_store = self.meta_store.clone();
        let stack = self.stack.clone();
        let dmc = self.dmc.clone();

        self.start_repair_sync_chunk_data().await;

        spawn( async move {
            loop {
                trace!("start sync chunk data");
                match meta_store.get_wait_sync().await {
                    Ok(vecs) => {
                        for (chunk_list, contract_id, owner_id) in vecs {

                            let meta_store = meta_store.clone();
                            let stack = stack.clone();
                            let dmc = dmc.clone();

                            spawn( async move {

                                info!("start sync contract: {}", &contract_id);
                                if let Err(e) = meta_store.update_down_status(&contract_id, SyncStatus::Down).await {
                                    error!("change down status err: {:?}", e);
                                }
                                if let Ok(_contract) = meta_store.get(&contract_id).await {
                                    let mut cdstat = true;

                                    if let Err(e) = Self::sync_chunk_to_local(stack.clone(), &chunk_list, vec![DeviceId::try_from(owner_id.clone()).unwrap()]).await {
                                        error!("sync chunk to local err {}", e);
                                        if let Err(e) = meta_store.update_down_status(&contract_id, SyncStatus::Wait).await {
                                            error!("change down status err: {:?}", e);
                                        }
                                        cdstat = false;
                                    }

                                    if cdstat {
                                        if let Err(e) = dmc.report_merkle_hash(&contract_id).await {
                                            log::error!("report_merkle_hash err {}", e);
                                        } else {
                                            if let Err(e) = meta_store.update_down_status(&contract_id, SyncStatus::Success).await {
                                                error!("change down status err: {:?}", e);
                                            }
                                            for chunk_id in &chunk_list {
                                                if let Err(e) = meta_store.chunk_ref_add(&contract_id, chunk_id).await {
                                                    info!("add chunk ref err: {:?}", e);
                                                }
                                                if let Err(e) = meta_store.chunk_del_list_del(chunk_id).await {
                                                    info!("add chunk ref err: {:?}", e);
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    if let Err(e) = meta_store.update_down_status(&contract_id, SyncStatus::Wait).await {
                                        error!("change down status err: {:?}", e);
                                    }
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
    }

    pub async fn start_repair_sync_chunk_data(&self) {
        loop {
            if let Err(e) = self.meta_store.repair_half_sync().await {
                error!("start repair err: {}", e);
                continue;
            }
            break;
        }
    }

    async fn check_contract_state(&self) -> BuckyResult<()> {
        let contract_list = self.meta_store.get_need_check_end_contract_list().await?;
        for (contract, latest_time) in contract_list.iter() {
            let contract_ref = DsgContractObjectRef::from(contract);
            let contract_id = contract_ref.id();
            match self.dmc.get_order(contract_ref.witness().order_id.as_str()).await {
                Ok(order) => {
                    if order.is_some() {
                        if order.as_ref().unwrap().state == DMCOrderState::OrderStateEnd as u8 {
                            log::error!("contract {} end.dmc order {}", contract_id.to_string(), contract_ref.witness().order_id.as_str());
                            self.meta_store.update_down_status(&contract_id, SyncStatus::ContractOutTime).await?;
                        } else {
                            self.meta_store.update_latest_check_time(&contract_id, latest_time + 7*24*3600*1000000).await?;
                        }
                    }
                },
                Err(e) => {
                    log::error!("get {} order err {}", contract_ref.witness().order_id.as_str(), e);
                }
            }

        }
        Ok(())
    }

    pub async fn contract_end_del(self: &Arc<Self>) {
        let meta_store = self.meta_store.clone();
        let this = self.clone();

        spawn( async move {
            loop {
                
                if let Err(e) = this.check_contract_state().await {
                    error!("check out time err: {}", e);
                }

                if let Err(e) = meta_store.check_challenge_out_time().await {
                    error!("challenge out time err: {}", e);
                }

                match meta_store.get_end_contracts().await {
                    Ok(vecs) => {
                        for (chunk_list, contract_id, _owner_id) in vecs {

                           let mut is_del = true;
                           for chunk_id in &chunk_list {
                                if let Err(e) = meta_store.chunk_ref_del(&contract_id, chunk_id).await {
                                    error!("chunk ref del err: {:?}", e);
                                    is_del = false;
                                }
                                if let Err(e) = meta_store.chunk_del_list_add(chunk_id).await {
                                    error!("del list add err: {:?}", e);
                                    is_del = false;
                                }
                           }

                           if is_del {
                                if let Err(e) = meta_store.update_down_status(&contract_id, SyncStatus::Complete).await {
                                    error!("change status err: {:?}", e);
                                }
                           }
                        }
                    }
                    Err(e) => {
                        info!("no data wait del: {}", e)
                    }
                }
                sleep(Duration::from_secs(5)).await;
            }
        });
    }

    pub async fn first_proof(&self) {
        let meta_store = self.meta_store.clone();
        let data_store = self.raw_data_store.clone();
        let stack = self.stack.clone();

        spawn( async move {
            loop {
                trace!("start proof chunk data");
                match meta_store.get_wait_proof().await {
                    Ok(vecs) => {
                        for (chunk_list, contract_id, owner_id) in vecs {
                            info!("start proof contract: {}", &contract_id);
                            if let Ok(challenge) = meta_store.get_challenge(&contract_id).await {
                                let challenge_ref = DsgChallengeObjectRef::from(&challenge);
                                let chunk_reder = Box::new(MinerChunkReader::new(data_store.clone()));
                                info!("challenge: {} chunks: {:?}", &challenge_ref, &chunk_list);
                        
                                if let Ok(proof) = DsgProofObjectRef::proove(challenge_ref, &chunk_list, chunk_reder).await {
                                    let proof_ref = DsgProofObjectRef::from(&proof);
                                    let interface = DsgMinerInterface::new(stack.clone());
                                    if let Err(e) = interface.verify_proof(proof_ref, DeviceId::try_from(owner_id.clone()).unwrap()).await {
                                        error!("verify proof err: {:?}", e)
                                    } else {
                                        info!("first proof success");
                                        if let Err(e) = meta_store.update_down_status(&contract_id, SyncStatus::Proof).await {
                                            error!("change proof status err: {:?}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(_e) => {
                        //error!("{}", e);
                        info!("no data wait proof")
                    }
                }
                sleep(Duration::from_secs(5)).await;
            }
        });
    }

    /*pub async fn build_merkle_tree(data_store: Arc<Box<dyn ContractChunkStore>>, chunk_list: &Vec<ChunkId>, contract_id: ObjectId) -> BuckyResult<()> {
        let list = chunk_list.iter().map(|chunk_id| {
            let cpath = PathBuf::from(format!("/dmc/{}", chunk_id));
            block_on(data_store.get_chunk_reader(cpath)).unwrap()
        }).collect::<Vec<_>>();

        let merge_reader = ReaderTool::merge(list).await;

        let merkle = MerkleTree::new();
        merkle.build(merge_reader).await?;

        let tmp_path = Self::get_tmp_path().await?;
        let writer = fs::File::create(&tmp_path).await?;
        merkle.write(writer).await?;

        data_store.save_chunk(tmp_path, format!("/dmc/{}_merkle", contract_id)).await?;

        Ok(())
    }*/

    pub async fn sync_chunk_to_local(stack: Arc<SharedCyfsStack>, chunk_list: &Vec<ChunkId>, source_list: Vec<DeviceId>) -> BuckyResult<()> {
        for chunk_id in chunk_list {
            Self::download(stack.clone(), chunk_id.object_id(), None, source_list.clone()).await?;
        }
        
        Ok(())
    }

    pub async fn get_tmp_path() -> BuckyResult<PathBuf> {
        let file = tempfile::NamedTempFile::new()?;
        let save_path = file.path();
        Ok(save_path.to_owned())
    }

    

    pub async fn download(stack: Arc<SharedCyfsStack>, chunk_id: ObjectId, save_path: Option<PathBuf>, source_list: Vec<DeviceId>) -> BuckyResult<()> {
        let dec_id = get_dec_id();
        let task_id = stack.trans().create_task(&TransCreateTaskOutputRequest {
            common: NDNOutputRequestCommon {
                req_path: None,
                dec_id: Some(dec_id),
                level: NDNAPILevel::NDC,
                target: None,
                referer_object: vec![],
                flags: 0
            },
            object_id: chunk_id,
            local_path: if save_path.is_none() {PathBuf::new()} else {save_path.unwrap()},
            device_list: source_list,
            context_id: None,
            auto_start: true
        }).await?.task_id;

        loop {
            let state = stack.trans().get_task_state(&TransGetTaskStateOutputRequest {
                common: NDNOutputRequestCommon {
                    req_path: None,
                    dec_id: Some(dec_id),
                    level: NDNAPILevel::NDC,
                    target: None,
                    referer_object: vec![],
                    flags: 0
                },
                task_id: task_id.clone()
            }).await?;

            match state {
                TransTaskState::Pending => {

                }
                TransTaskState::Downloading(_) => {

                }
                TransTaskState::Paused | TransTaskState::Canceled => {
                    let msg = format!("download {} task abnormal exit.", chunk_id.to_string());
                    log::error!("{}", msg.as_str());
                    return Err(BuckyError::new(BuckyErrorCode::Failed, msg))
                }
                TransTaskState::Finished(_) => {
                    break;
                }
                TransTaskState::Err(err) => {
                    let msg = format!("download {} failed.{}", chunk_id.to_string(), err);
                    log::error!("{}", msg.as_str());
                    return Err(BuckyError::new(err, msg))
                }
            }
            async_std::task::sleep(Duration::from_secs(1)).await;
        }
        stack.trans().delete_task(&TransTaskOutputRequest {
            common: NDNOutputRequestCommon {
                req_path: None,
                dec_id: Some(dec_id),
                level: NDNAPILevel::NDC,
                target: None,
                referer_object: vec![],
                flags: 0
            },
            task_id
        }).await?;
        Ok(())
    }

    pub async fn contract_exists(&self, contract_id: &ObjectId) -> bool {
        match self.meta_store.get(contract_id).await {
            Ok(_) => true,
            Err(_) => false
        }
    }

    pub async fn get_object<T: for <'a> RawDecode<'a>>(&self, target: Option<ObjectId>, object_id: ObjectId) -> BuckyResult<T> {
        let resp = self.stack.non_service().get_object(NONGetObjectOutputRequest {
            common: NONOutputRequestCommon {
                req_path: None,
                dec_id: None,
                level: if target.is_none() {NONAPILevel::NOC} else {NONAPILevel::Router},
                target,
                flags: 0
            },
            object_id,
            inner_path: None
        }).await?;

        T::clone_from_slice(resp.object.object_raw.as_slice())
    }
}

#[derive(Clone)]
pub struct MinerChunkReader {
    raw_data_store: Arc<Box<dyn ContractChunkStore>>
}
impl MinerChunkReader {
    fn new(raw_data_store: Arc<Box<dyn ContractChunkStore>>) -> Self {
        Self{
            raw_data_store
        }
    }
}

#[async_trait::async_trait]
impl ChunkReader for MinerChunkReader {
    fn clone_as_reader(&self) -> Box<dyn ChunkReader> {
        Box::new(self.clone())
    }

    async fn exists(&self, chunk: &ChunkId) -> bool {
        self.raw_data_store.chunk_exists(chunk).await
    }

    async fn get(&self, chunk: &ChunkId) -> BuckyResult<Arc<Vec<u8>>> {
        Ok(Arc::new(self.raw_data_store.get_chunk(chunk.clone()).await?))
    }
}


pub struct DelegateImpl {
    pub store: Arc<MinerChallenge>,
}


impl DelegateImpl {
    pub fn new(
        stack: Arc<SharedCyfsStack>,
        meta_store: Arc<Box<dyn ContractMetaStore>>,
        raw_data_store: Arc<Box<dyn ContractChunkStore>>,
        dmc: DMCRef) -> Self {
        Self {
            store: Arc::new(MinerChallenge::new(stack, meta_store, raw_data_store, dmc))
        }
    }

    fn store(&self) -> &MinerChallenge {
        self.store.as_ref()
    }
}

#[async_trait::async_trait]
impl DsgMinerDelegate for DelegateImpl {
    async fn on_challenge(
        &self,
        interface: &DsgMinerInterface,
        challenge: DsgChallengeObject,
        from: DeviceId
    ) -> BuckyResult<()> {
        let challenge_ref = DsgChallengeObjectRef::from(&challenge);
        let contract_id = challenge_ref.contract_id();
        let state_id = challenge_ref.contract_state();
        let owner_id = from.object_id();
        info!("start challenge # contract_id: {} state_id: {} owner_id: {} saveples: {:?}", contract_id, state_id, owner_id, challenge_ref.samples());

        match self.store().contract_exists(contract_id).await {
            true => {
                self.store().challenge(interface, contract_id, state_id, &challenge, owner_id).await?;
            },
            false => {
                self.store().first_sync_contract(interface, contract_id, state_id, &challenge, owner_id).await?;
            }
        }

        Ok(())
    }
}

pub fn get_dec_id() -> ObjectId {
    DecApp::generate_id(ObjectId::from_str("5r4MYfFVckFsxFfb1D5SbdJ1TAfrVzjWpPCLovTHu4zC").unwrap(), "ood-miner")
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
