use std::collections::{BTreeSet, BTreeMap};
use std::{sync::Arc};
use cyfs_base::*;
use cyfs_lib::*;
use cyfs_dsg_client::*;
use super::*;

pub struct StackStore {
    stack: Arc<SharedCyfsStack>
}

impl StackStore {
    pub fn new(stack: Arc<SharedCyfsStack>) -> Self {
        Self{
            stack
        }
    }

    pub async fn get_contract_id_by_path(&self, path: String) -> BuckyResult<Option<ObjectId>> {
        let key = hash(path).await;
        let op_env = self.stack.root_state_stub(None, None).create_path_op_env().await?;
        let rt = if let Some(object_id) = op_env.get_by_key(format!("/miner/contracts/chunk_contract_id_path/"), key).await? {
            Ok(Some(object_id))
        } else {
            Ok(None)
        };
        if let Err(e) = op_env.commit().await {
            error!("commit err: {}", e);
        }
        rt
    }

    pub async fn save_contract_id_by_path(&self, path: String, object_id: &ObjectId) -> BuckyResult<()> {
        let key = hash(path).await;
        let op_env = self.stack.root_state_stub(None, None).create_path_op_env().await?;
        op_env.set_with_key(format!("/miner/contracts/chunk_contract_id_path/"), key, object_id, None, true).await?;
        if let Err(e) = op_env.commit().await {
            error!("save err: {}", e);
        }

        Ok(())
    }

    pub async fn get_down_stat(&self, contract_id: &ObjectId) -> BuckyResult<SyncStatus> {
        if let Some(set_id) = self.get_by_path(format!("/miner/contracts/{}/", contract_id), "down_stat").await? {
            let cobj: ContractSyncStatus = self.get_object_from_noc::<RawObject>(set_id).await?.get()?;
            Ok(SyncStatus::from(cobj.0))
        } else {
            Err(BuckyError::from("not found"))
        }
    }

    pub async fn save_down_stat(&self, contract_id: &ObjectId, state: SyncStatus) -> BuckyResult<()> {
        let down_stat = ContractSyncStatus(state.into());
        self.save_by_path(format!("/miner/contracts/{}/", contract_id), "down_stat", None, None, Some(&down_stat)).await?;

        Ok(())
    }

    pub async fn contract_set(&self) -> BuckyResult<BTreeSet<ObjectId>> {
        let mut set = BTreeSet::new();
        if let Some(set_id) = self.get_by_path("/miner/contracts/list/", "contract_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id).await?.get()?;
            set = list.0;
        }

        Ok(set)
    }

    pub async fn contract_set_add(&self, contract_id: &ObjectId) -> BuckyResult<()> {
        let list = if let Some(set_id) = self.get_by_path("/miner/contracts/list/", "contract_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id).await?.get()?;
            let mut cset = list.0;
            cset.insert(contract_id.clone());
            cset
        } else {
            let mut cset = BTreeSet::new();
            cset.insert(contract_id.clone());
            cset
        };

        let set = ContractList(list);
        self.save_by_path("/miner/contracts/list/", "contract_set", None, None, Some(&set)).await?;

        Ok(())
    }

    pub async fn contract_set_remove(&self, contract_id: &ObjectId) -> BuckyResult<()> {
        if let Some(set_id) = self.get_by_path("/miner/contracts/list/", "contract_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id.clone()).await?.get()?;
            let mut cset = list.0;
            cset.remove(contract_id);

            let set = ContractList(cset);
            self.save_by_path("/miner/contracts/list/", "contract_set", Some(&set_id), None, Some(&set)).await?;
        }

        Ok(())
    }

    pub async fn chunk_ref(&self) -> BuckyResult<BTreeMap<ChunkId, BTreeSet<ObjectId>>> {
        let mut map = BTreeMap::new();
        if let Some(set_id) = self.get_by_path("/miner/contracts/chunk_ref/", "chunk_ref").await? {
            let list: ChunkRefMap = self.get_object_from_noc::<RawObject>(set_id).await?.get()?;
            map = list.0;
        }

        Ok(map)
    }

    pub async fn chunk_ref_create(&self, contract_id: &ObjectId, chunk_id: &ChunkId) -> BuckyResult<()> {
        let cmap = if let Ok(mut map) = self.chunk_ref().await {
            let set = if let Some(oset) = map.get(chunk_id) {
                let mut cset = oset.clone();
                cset.insert(contract_id.clone());
                cset
            } else {
                let mut cset = BTreeSet::new();
                cset.insert(contract_id.clone());
                cset
            };
            map.insert(chunk_id.clone(), set);
            map
        } else {
            let mut map = BTreeMap::new();
            let mut set = BTreeSet::new();
            set.insert(contract_id.clone());
            map.insert(chunk_id.clone(), set);
            map
        };

        let ck_ref = ChunkRefMap(cmap);
        self.save_by_path("/miner/contracts/chunk_ref/", "chunk_ref", None, None, Some(&ck_ref)).await?;

        Ok(())
    }

    pub async fn chunk_ref_remove(&self, contract_id: &ObjectId, chunk_id: &ChunkId) -> BuckyResult<()> {
        if let Ok(mut cmap) = self.chunk_ref().await {
            if let Some(cset) = cmap.get(chunk_id) {
                if cset.contains(contract_id) {
                    let mut set = cset.clone();
                    set.remove(contract_id);
                    cmap.insert(chunk_id.clone(), set);

                    let ck_ref = ChunkRefMap(cmap);
                    self.save_by_path("/miner/contracts/chunk_ref/", "chunk_ref", None, None, Some(&ck_ref)).await?;            
                }
            }
        }

        Ok(())
    }

    pub async fn chunk_ref_exist(&self, contract_id: &ObjectId, chunk_id: &ChunkId) -> BuckyResult<bool> {
        if let Ok(cmap) = self.chunk_ref().await {
            if let Some(cset) = cmap.get(chunk_id) {
                if cset.contains(contract_id) {
                    return Ok(true);            
                }
            }
        }

        Ok(false)
    }

    pub async fn del_list(&self) -> BuckyResult<BTreeSet<ChunkId>> {
        let mut set = BTreeSet::new();
        if let Some(set_id) = self.get_by_path("/miner/contracts/del_list/", "del_list").await? {
            let list: DelSet = self.get_object_from_noc::<RawObject>(set_id).await?.get()?;
            set = list.0;
        }

        Ok(set)
    }

    pub async fn del_list_create(&self, chunk_id: &ChunkId) -> BuckyResult<()> {
        let mut is_save = true;
        let cset = if let Ok(mut dset) = self.del_list().await {
            if dset.contains(chunk_id) {
                is_save = false;
            } else {
                dset.insert(chunk_id.clone());
            };
            dset
        } else {
            let mut set = BTreeSet::new();
            set.insert(chunk_id.clone());
            set
        };

        if is_save {
            let del_set = DelSet(cset);
            self.save_by_path("/miner/contracts/del_list/", "del_list", None, None, Some(&del_set)).await?;    
        }
        
        Ok(())
    }

    pub async fn del_list_remove(&self, chunk_id: &ChunkId) -> BuckyResult<()> {
        if let Ok(mut dset) = self.del_list().await {
            if dset.contains(chunk_id) {
                dset.remove(chunk_id);

                let del_set = DelSet(dset);
                self.save_by_path("/miner/contracts/del_list/", "del_list", None, None, Some(&del_set)).await?;            
            }
        }

        Ok(())
    }

    pub async fn del_list_exist(&self, chunk_id: &ChunkId) -> BuckyResult<bool> {
        if let Ok(dset) = self.del_list().await {
            if dset.contains(chunk_id) {
                return Ok(true);            
            }
        }

        Ok(false)
    }

    pub async fn contract_sync_set(&self) -> BuckyResult<BTreeSet<ObjectId>> {
        let mut syn_set = BTreeSet::new();
        match self.get_by_path("/miner/contracts/list_syn/", "contract_syn_set").await {
            Ok(cset_id) => {
                if let Some(set_id) = cset_id {
                    let list: ContractList = self.get_object_from_noc::<RawObject>(set_id).await?.get()?;
                    syn_set = list.0;
                }
            },
            Err(_) => {
                info!("contract sync set empty")
            }
        }

        Ok(syn_set)
    }

    pub async fn contract_sync_set_add(&self, contract_id: &ObjectId) -> BuckyResult<()> {
        let list = if let Some(set_id) = self.get_by_path("/miner/contracts/list_syn/", "contract_syn_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id).await?.get()?;
            let mut cset = list.0;
            cset.insert(contract_id.clone());
            cset
        } else {
            let mut cset = BTreeSet::new();
            cset.insert(contract_id.clone());
            cset
        };

        let set = ContractList(list);
        self.save_by_path("/miner/contracts/list_syn/", "contract_syn_set", None, None, Some(&set)).await?;

        Ok(())
    }

    pub async fn contract_sync_set_remove(&self, contract_id: &ObjectId) -> BuckyResult<()> {
        if let Some(set_id) = self.get_by_path("/miner/contracts/list_syn/", "contract_syn_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id.clone()).await?.get()?;
            let mut cset = list.0;
            cset.remove(contract_id);

            let set = ContractList(cset);
            self.save_by_path("/miner/contracts/list_syn/", "contract_syn_set", Some(&set_id), None, Some(&set)).await?;
        }

        Ok(())
    }

    pub async fn contract_proof_set(&self) -> BuckyResult<BTreeSet<ObjectId>> {
        let mut proof_set = BTreeSet::new();
        match self.get_by_path("/miner/contracts/list_proof/", "contract_proof_set").await {
            Ok(cset_id) => {
                if let Some(set_id) = cset_id {
                    let list: ContractList = self.get_object_from_noc::<RawObject>(set_id).await?.get()?;
                    proof_set = list.0;
                }
            },
            Err(_) => {
                info!("contract proof set empty");
            }
        }

        Ok(proof_set)
    }

    pub async fn contract_proof_set_add(&self, contract_id: &ObjectId) -> BuckyResult<()> {
        let list = if let Some(set_id) = self.get_by_path("/miner/contracts/list_proof/", "contract_proof_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id).await?.get()?;
            let mut cset = list.0;
            cset.insert(contract_id.clone());
            cset
        } else {
            let mut cset = BTreeSet::new();
            cset.insert(contract_id.clone());
            cset
        };

        let set = ContractList(list);
        self.save_by_path("/miner/contracts/list_proof/", "contract_proof_set", None, None, Some(&set)).await?;

        Ok(())
    }

    pub async fn contract_proof_set_remove(&self, contract_id: &ObjectId) -> BuckyResult<()> {
        if let Some(set_id) = self.get_by_path("/miner/contracts/list_proof/", "contract_proof_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id.clone()).await?.get()?;
            let mut cset = list.0;
            cset.remove(contract_id);

            let set = ContractList(cset);
            self.save_by_path("/miner/contracts/list_proof/", "contract_proof_set", Some(&set_id), None, Some(&set)).await?;
        }

        Ok(())
    }

    async fn get_by_path(&self, path: impl Into<String>, key: impl Into<String>) -> BuckyResult<Option<ObjectId>> {
        let mut coid = None;
        let op_env = self.stack.root_state_stub(None, None).create_path_op_env().await?;
        if let Some(obj_id) = op_env.get_by_key(path, key).await? {
            coid = Some(obj_id);
        }
        if let Err(e) = op_env.commit().await {
            error!("commit err: {}", e);
        }
        Ok(coid)
    }

    async fn save_by_path<T: RawEncode + for<'a> RawDecode<'a>>(&self, path: impl Into<String>, key: impl Into<String>, object_id: Option<&ObjectId>, object: Option<&T>, buf: Option<&T>) -> BuckyResult<ObjectId> {
        let mut cur_obj_id = ObjectId::default();

        if let Some(obj_id) = object_id {
            cur_obj_id = obj_id.clone();
        }

        if let Some(obj) = object {
            self.put_object_to_noc(cur_obj_id.clone(), obj).await?;
        }

        if let Some(raw_data) = buf {
            let raw_obj = RawObject::new(ObjectId::default(), ObjectId::default(), 0, raw_data)?;
            let new_obj_id = raw_obj.desc().object_id();
            if new_obj_id != cur_obj_id {
                if cur_obj_id != ObjectId::default() {
                    self.delete_object_from_noc(cur_obj_id.clone()).await?;
                }
                cur_obj_id = new_obj_id;
                self.put_object_to_noc(cur_obj_id.clone(), &raw_obj).await?;
            }
        }

        let op_env = self.stack.root_state_stub(None, None).create_path_op_env().await?;
        op_env.set_with_key(path, key, &cur_obj_id, None, true).await?;
        if let Err(e) = op_env.commit().await {
            error!("commit err: {}", e);
        }

        Ok(cur_obj_id)
    }

    async fn get_object_from_noc<T: for <'de> RawDecode<'de>>(&self, id: ObjectId) -> BuckyResult<T> {
        let resp = self.stack.non_service().get_object(NONGetObjectOutputRequest::new(NONAPILevel::NOC, id, None)).await?;
        T::clone_from_slice(resp.object.object_raw.as_slice())
    }

    async fn put_object_to_noc<T: RawEncode>(&self, id: ObjectId, object: &T) -> BuckyResult<()> {
        let _ = self.stack.non_service().put_object(NONPutObjectOutputRequest::new(NONAPILevel::NOC, id, object.to_vec()?)).await?;
        Ok(())
    }

    async fn delete_object_from_noc(&self, id: ObjectId) -> BuckyResult<()> {
        self.stack.non_service().delete_object(NONDeleteObjectOutputRequest::new(NONAPILevel::NOC, id, None)).await?;

        Ok(())
    }

}

#[async_trait::async_trait]
impl ContractMetaStore for StackStore {
    async fn get(&self, contract_id: &ObjectId) -> BuckyResult<DsgContractObject<DMCContractData>> {
        if let Some(obj_id) = self.get_by_path(format!("/miner/contracts/{}/", contract_id), "contract").await? {
           self.get_object_from_noc(obj_id).await
        } else {
            Err(BuckyError::from("not found contract"))
        }
    }

    async fn save(&self, contract_id: &ObjectId, contract: &DsgContractObject<DMCContractData>) -> BuckyResult<()> {
        let contract_ref = DsgContractObjectRef::from(contract);
        let st = contract_ref.storage();
        let data = contract_ref.data_source();
        let mut url_path = String::new();

        match st {
            DsgStorage::Cache(cst) => {
                if let Some(purl) = &cst.pub_http {
                    url_path = purl.clone();
                }
            },
            _ => ()
        };

        let mut down_stat = SyncStatus::Wait;
        let mut chunks = vec![];
        match data {
            DsgDataSource::Immutable(cks) => {
                chunks = cks.clone();
            },
            DsgDataSource::Mutable(_) => {
                down_stat = SyncStatus::Proof;
            }
        };

        self.save_by_path(format!("/miner/contracts/{}/", contract_id), "contract", Some(contract_id), Some(contract), None).await?;
        self.contract_sync_set_add(contract_id).await?;
        self.save_down_stat(contract_id, down_stat).await?;
        self.contract_set_add(contract_id).await?;
        self.save_contract_id_by_path(url_path, contract_id).await?;
        self.save_chunk_list(contract_id, chunks).await?;

        Ok(())
    }

    async fn get_wait_sync(&self) -> BuckyResult<Vec<(Vec<ChunkId>, ObjectId, ObjectId)>> {
        let mut wait_sync = vec![];
        for contract_id in self.contract_sync_set().await? {
            match self.get_chunk_list(&contract_id).await {
                Ok(chunks) => {
                    match self.get_owner(&contract_id).await {
                        Ok(owner_id) => {
                            wait_sync.push((chunks, contract_id, owner_id));
                        },
                        Err(e) => {
                            error!("get_wait_sync err: {}", e);
                        }
                    }
                },
                Err(e) => {
                    error!("get_wait_sync err: {}", e);
                }
            }
        }
        Ok(wait_sync)
    }

    async fn get_wait_proof(&self) -> BuckyResult<Vec<(Vec<ChunkId>, ObjectId, ObjectId)>> {
        let mut wait_proof = vec![];
        for contract_id in self.contract_proof_set().await? {
            match self.get_chunk_list(&contract_id).await {
                Ok(chunks) => {
                    match self.get_owner(&contract_id).await {
                        Ok(owner_id) => {
                            wait_proof.push((chunks, contract_id, owner_id));
                        },
                        Err(e) => {
                            error!("get_wait_proof err: {}", e);
                        }
                    }
                },
                Err(e) => {
                    error!("get_wait_proof err: {}", e);
                }
            }
        }
        Ok(wait_proof)
    }

    async fn update_down_status(&self, contract_id: &ObjectId, down_stat: SyncStatus) -> BuckyResult<()> {
        self.save_down_stat(contract_id, down_stat.clone()).await?;

        match down_stat {
            SyncStatus::Wait => {
                self.contract_sync_set_add(contract_id).await?;
            },
            SyncStatus::Down => {
                self.contract_sync_set_remove(contract_id).await?;
            },
            SyncStatus::Success => {
                self.contract_sync_set_remove(contract_id).await?;
                self.contract_proof_set_add(contract_id).await?;
            },
            SyncStatus::Proof => {
                self.contract_proof_set_remove(contract_id).await?;
            },
            _ => ()
        }

        Ok(())
    }

    async fn get_next_contract(&self, pos: usize) -> Option<(ObjectId,usize)> {
        if let Ok(list) = self.contract_set().await {
            if let Some(contract_id) = list.iter().nth(pos) {
                return Some((contract_id.clone(), pos+1));
            }
        }
        None
    }

    async fn get_down_status(&self, contract_id: &ObjectId) -> BuckyResult<SyncStatus> {
        Ok(self.get_down_stat(contract_id).await?)
    }

    async fn get_stat(&self, contract_id: &ObjectId) -> BuckyResult<DsgContractStateObject> {
        if let Some(obj_id) = self.get_by_path(format!("/miner/contracts/{}/", contract_id), "state").await? {
            self.get_object_from_noc(obj_id).await
         } else {
             Err(BuckyError::from("not found contract state"))
         }
    }

    async fn save_stat(&self, contract_id: &ObjectId, state: &DsgContractStateObject) -> BuckyResult<()> {
        let state_ref = DsgContractStateObjectRef::from(state);
        let state_id = state_ref.id();
        self.save_by_path(format!("/miner/contracts/{}/", contract_id), "state", Some(&state_id), Some(state), None).await?;

        Ok(())
    }

    async fn get_chunk_list(&self, contract_id: &ObjectId) -> BuckyResult<Vec<ChunkId>> {
        if let Some(obj_id) = self.get_by_path(format!("/miner/contracts/{}/", contract_id), "chunk_list").await? {
            let chunk_list: ContractChunkList = self.get_object_from_noc::<RawObject>(obj_id).await?.get()?;
            Ok(chunk_list.0)
        } else {
            Err(BuckyError::from("not found chunk list"))
        }
    }

    async fn get_chunks_by_path(&self, url_path: String) -> BuckyResult<Vec<ChunkId>> {
        let mut chunks = vec![];
        if let Some(contract_id) = self.get_contract_id_by_path(url_path).await? {
            chunks = self.get_chunk_list(&contract_id).await?;
        }
        Ok(chunks)
    }

    async fn save_chunk_list(&self, contract_id: &ObjectId, chunk_list: Vec<ChunkId>) -> BuckyResult<()> {
        let set = ContractChunkList(chunk_list);
        self.save_by_path(format!("/miner/contracts/{}/", contract_id), "chunk_list", None, None, Some(&set)).await?;

        Ok(())
    }

    async fn get_challenge(&self, contract_id: &ObjectId) -> BuckyResult<DsgChallengeObject> {
        if let Some(obj_id) = self.get_by_path(format!("/miner/contracts/{}/", contract_id), "challenge").await? {
            self.get_object_from_noc(obj_id).await
         } else {
             Err(BuckyError::from("not found contract"))
         }
    }

    async fn save_challenge(&self, contract_id: &ObjectId, challenge: &DsgChallengeObject) -> BuckyResult<()> {
        let challenge_obj = DsgChallengeObjectRef::from(challenge);
        let challenge_id = challenge_obj.id();
        self.save_by_path(format!("/miner/contracts/{}/", contract_id), "challenge", Some(&challenge_id), Some(challenge), None).await?;

        Ok(())
    }

    async fn get_owner(&self, contract_id: &ObjectId) -> BuckyResult<ObjectId> {
        let op_env = self.stack.root_state_stub(None, None).create_path_op_env().await?;
        let result = match op_env.get_by_key(format!("/miner/contracts/{}/", contract_id), "owner").await? {
            Some(owner_id) => Ok(owner_id),
            None => Err(BuckyError::from("not found owner"))
        };
        if let Err(e) = op_env.commit().await {
            error!("commit err: {}", e);
        }

        result
    }

    async fn save_owner(&self, contract_id: &ObjectId, owner_id: &ObjectId) -> BuckyResult<()> {
        let op_env = self.stack.root_state_stub(None, None).create_path_op_env().await?;
        op_env.set_with_key(format!("/miner/contracts/{}/", contract_id), "owner", &owner_id, None, true).await?;
        if let Err(e) = op_env.commit().await {
            error!("save contract owner err: {}", e);
        }

        Ok(())
    }

    async fn chunk_ref_add(&self, contract_id: &ObjectId, chunk_id: &ChunkId) -> BuckyResult<()> {
        self.chunk_ref_create(contract_id, chunk_id).await
    }

    async fn chunk_ref_del(&self, contract_id: &ObjectId, chunk_id: &ChunkId) -> BuckyResult<()> {
        self.chunk_ref_remove(contract_id, chunk_id).await
    }

    async fn check_challenge_out_time(&self) -> BuckyResult<()> {
        let contracts = self.contract_set().await?;
        for contract_id in contracts {
            let challenge = self.get_challenge(&contract_id).await?;
            let challenge_ref = DsgChallengeObjectRef::from(&challenge);

            let exp = challenge_ref.expire_at();
            let now = bucky_time_now();

            if now > exp {
                self.update_down_status(&contract_id, SyncStatus::ChallengeOutTime).await?;
            }
        }

        Ok(())
    }

    async fn check_contract_out_time(&self) -> BuckyResult<()> {
        let contracts = self.contract_set().await?;
        for contract_id in contracts {
            let contract: DsgContractObject<DMCContractData> = self.get(&contract_id).await?;
            let contract_ref = DsgContractObjectRef::from(&contract);

            let exp = contract_ref.end_at();
            let now = bucky_time_now();

            if now > exp {
                self.update_down_status(&contract_id, SyncStatus::ContractOutTime).await?;
            }
        }

        Ok(())
    }

    async fn get_end_contracts(&self) -> BuckyResult<Vec<(Vec<ChunkId>, ObjectId, ObjectId)>> {
        let mut list = vec![];
        let contracts = self.contract_set().await?;
        for contract_id in contracts {
            let state = self.get_down_stat(&contract_id).await?;
            match state {
                SyncStatus::ContractOutTime => {
                    let owner_id = self.get_owner(&contract_id).await?;
                    let chunks = self.get_chunk_list(&contract_id).await?;
                    
                    list.push((chunks, contract_id, owner_id));
                },
                _ => ()
            }
        }

        Ok(list)
    }

    async fn chunk_del_list_add(&self, chunk_id: &ChunkId) -> BuckyResult<()> {
        self.del_list_create(chunk_id).await
    }

    async fn chunk_del_list_del(&self, chunk_id: &ChunkId) -> BuckyResult<()> {
        self.del_list_remove(chunk_id).await
    }

    async fn repair_half_sync(&self) -> BuckyResult<()> {
        let contracts = self.contract_set().await?;
        for contract_id in contracts {
            let state = self.get_down_stat(&contract_id).await?;
            match state {
                SyncStatus::Down => {
                    self.update_down_status(&contract_id, SyncStatus::Wait).await?;
                },
                _ => ()
            } 
        }

        Ok(())
    }

    async fn get_need_check_end_contract_list(&self) -> BuckyResult<Vec<(DsgContractObject<DMCContractData>, u64)>> {
        let mut list = Vec::new();
        let contracts = self.contract_set().await?;
        for contract_id in contracts {
            let state = self.get_down_stat(&contract_id).await?;
            match state {
                SyncStatus::ContractOutTime => {
                    if let Some(obj_id) = self.get_by_path(format!("/miner/contracts/{}/", contract_id), "latest_check_time").await? {
                        let LastCheckTime(latest_check_time) = self.get_object_from_noc(obj_id).await?;
                        let contract: DsgContractObject<DMCContractData> = self.get(&contract_id).await?;
                        list.push((contract, latest_check_time as u64));
                    }
                },
                _ => ()
            }
        }

        Ok(list)
    }

    async fn update_latest_check_time(&self, contract_id: &ObjectId, latest_check_time: u64) -> BuckyResult<()> {
        let last_time = LastCheckTime(latest_check_time);
        self.save_by_path(format!("/miner/contracts/{}/", contract_id), "latest_check_time", None, Some(&last_time), None).await?;

        Ok(())
    }

    async fn update_contract_status(&self, contract_id: &ObjectId, dstat: SyncStatus, latest_check_time: u64) -> BuckyResult<()> {
        self.update_latest_check_time(contract_id, latest_check_time).await?;
        self.save_down_stat(contract_id, dstat).await?;
        Ok(())
    }

}

pub async fn hash(data: impl AsRef<[u8]>) -> String {
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}",hasher.finalize())
}
