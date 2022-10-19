use std::collections::{BTreeSet, BTreeMap};
use std::{sync::Arc};
use std::convert::TryFrom;
use async_trait::async_trait;
use cyfs_base::*;
use cyfs_lib::*;
use cyfs_dsg_client::*;
use super::*;
use dmc_dsg_base::*;

pub const META_UPDATE_LOCKER: &str = "meta_update_locker";

pub struct CyfsStackMetaStore {
    stack: SharedCyfsStackRef,
    settings: SettingRef,
}

impl CyfsStackMetaStore {
    pub async fn create(stack: SharedCyfsStackRef) -> BuckyResult<Arc<CyfsStackMetaStore>> {
        let settings = Setting::new(stack.clone());
        settings.load().await?;

        Ok(Arc::new(Self {
            stack,
            settings
        }))
    }
}

#[async_trait]
impl MetaStore<CyfsStackMetaConnection> for CyfsStackMetaStore {
    async fn get_setting(&self, key: &str, default: &str) -> BuckyResult<String> {
        Ok(self.settings.get_setting(key, default))
    }

    async fn set_setting(&self, key: String, value: String) -> BuckyResult<()> {
        self.settings.set_setting(key, value);
        self.settings.save().await
    }

    async fn create_meta_connection(&self) -> BuckyResult<MetaConnectionProxy<CyfsStackMetaConnection>> {
        let op_env = self.stack.root_state_stub(None, None).create_path_op_env().await?;
        Ok(MetaConnectionProxy::new(CyfsStackMetaConnection::new(op_env, self.stack.clone())))
    }
}

pub struct CyfsStackMetaConnection {
    op_env: PathOpEnvStub,
    stack: Arc<SharedCyfsStack>,
}

impl CyfsStackMetaConnection {
    pub fn new(
        op_env: PathOpEnvStub,
        stack: Arc<SharedCyfsStack>,) -> Self {
        Self {
            op_env,
            stack
        }
    }
}

#[async_trait::async_trait]
impl MetaConnection for CyfsStackMetaConnection {
    async fn begin_trans(&mut self) -> BuckyResult<()> {
        Ok(())
    }

    async fn commit_trans(&mut self) -> BuckyResult<()> {
        self.op_env.clone().commit().await?;
        Ok(())
    }

    async fn rollback_trans(&mut self) -> BuckyResult<()> {
        Ok(())
    }
}

impl CyfsStackMetaConnection {
    pub async fn get_down_stat(&self, contract_id: &ObjectId) -> BuckyResult<ContractStatus> {
        if let Some(set_id) = self.get_by_path(format!("/miner/contracts/{}/", contract_id), "down_stat").await? {
            let cobj: ContractSyncStatus = self.get_object_from_noc::<RawObject>(set_id).await?.get()?;
            ContractStatus::try_from(cobj.0)
        } else {
            Err(BuckyError::from("not found"))
        }
    }

    pub async fn save_down_stat(&self, contract_id: &ObjectId, state: ContractStatus) -> BuckyResult<()> {
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

    pub async fn contract_set_add(&self, contract_list: &Vec<ObjectId>) -> BuckyResult<()> {
        self.op_env.lock(vec!["/miner/contracts/list/contract_set".to_string()], 10000).await?;
        let list = if let Some(set_id) = self.get_by_path("/miner/contracts/list/", "contract_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id).await?.get()?;
            let mut cset = list.0;
            for contract_id in contract_list.iter() {
                cset.insert(contract_id.clone());
            }
            cset
        } else {
            let mut cset = BTreeSet::new();
            for contract_id in contract_list.iter() {
                cset.insert(contract_id.clone());
            }
            cset
        };

        let set = ContractList(list);
        self.save_by_path("/miner/contracts/list/", "contract_set", None, None, Some(&set)).await?;

        Ok(())
    }

    pub async fn contract_set_remove(&self, contract_list: &Vec<ObjectId>) -> BuckyResult<()> {
        self.op_env.lock(vec!["/miner/contracts/list/contract_set".to_string()], 10000).await?;
        if let Some(set_id) = self.get_by_path("/miner/contracts/list/", "contract_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id.clone()).await?.get()?;
            let mut cset = list.0;
            for contract_id in contract_list.iter() {
                cset.remove(contract_id);
            }

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

    pub async fn chunk_ref_create(&self, contract_id: &ObjectId, chunk_list: &Vec<ChunkId>) -> BuckyResult<()> {
        self.op_env.lock(vec!["/miner/contracts/chunk_ref/chunk_ref".to_string()], 10000).await?;
        let cmap = if let Ok(mut map) = self.chunk_ref().await {
            for chunk_id in chunk_list.iter() {
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
            }
            map
        } else {
            let mut map = BTreeMap::new();
            for chunk_id in chunk_list.iter() {
                let mut set = BTreeSet::new();
                set.insert(contract_id.clone());
                map.insert(chunk_id.clone(), set);
            }
            map
        };

        let ck_ref = ChunkRefMap(cmap);
        self.save_by_path("/miner/contracts/chunk_ref/", "chunk_ref", None, None, Some(&ck_ref)).await?;

        Ok(())
    }

    pub async fn chunk_ref_remove(&self, contract_id: &ObjectId, chunk_list: &Vec<ChunkId>) -> BuckyResult<()> {
        self.op_env.lock(vec!["/miner/contracts/chunk_ref/chunk_ref".to_string()], 10000).await?;
        if let Ok(mut cmap) = self.chunk_ref().await {
            for chunk_id in chunk_list.iter() {
                if let Some(cset) = cmap.get(chunk_id) {
                    if cset.contains(contract_id) {
                        let mut set = cset.clone();
                        set.remove(contract_id);
                        cmap.insert(chunk_id.clone(), set);

                    }
                }
            }
            let ck_ref = ChunkRefMap(cmap);
            self.save_by_path("/miner/contracts/chunk_ref/", "chunk_ref", None, None, Some(&ck_ref)).await?;
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

    pub async fn del_list_create(&self, chunk_list: &Vec<ChunkId>) -> BuckyResult<()> {
        let mut is_save = false;
        self.op_env.lock(vec!["/miner/contracts/del_list/del_list".to_string()], 10000).await?;
        let cset = if let Ok(mut dset) = self.del_list().await {
            for chunk_id in chunk_list.iter() {
                if !dset.contains(chunk_id) {
                    is_save = true;
                    dset.insert(chunk_id.clone());
                };
            }
            dset
        } else {
            is_save = true;
            let mut set = BTreeSet::new();
            for chunk_id in chunk_list.iter() {
                set.insert(chunk_id.clone());
            }
            set
        };

        if is_save {
            let del_set = DelSet(cset);
            self.save_by_path("/miner/contracts/del_list/", "del_list", None, None, Some(&del_set)).await?;
        }

        Ok(())
    }

    pub async fn del_list_remove(&self, chunk_list: &Vec<ChunkId>) -> BuckyResult<()> {
        self.op_env.lock(vec!["/miner/contracts/del_list/del_list".to_string()], 10000).await?;
        if let Ok(mut dset) = self.del_list().await {
            for chunk_id in chunk_list.iter() {
                dset.remove(chunk_id);
            }
            let del_set = DelSet(dset);
            self.save_by_path("/miner/contracts/del_list/", "del_list", None, None, Some(&del_set)).await?;
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

    pub async fn contract_proof_set_add(&self, contract_list: &Vec<ObjectId>) -> BuckyResult<()> {
        self.op_env.lock(vec!["/miner/contracts/list_proof/contract_proof_set".to_string()], 10000).await?;
        let list = if let Some(set_id) = self.get_by_path("/miner/contracts/list_proof/", "contract_proof_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id).await?.get()?;
            let mut cset = list.0;
            for contract_id in contract_list.iter() {
                cset.insert(contract_id.clone());
            }
            cset
        } else {
            let mut cset = BTreeSet::new();
            for contract_id in contract_list.iter() {
                cset.insert(contract_id.clone());
            }
            cset
        };

        let set = ContractList(list);
        self.save_by_path("/miner/contracts/list_proof/", "contract_proof_set", None, None, Some(&set)).await?;

        Ok(())
    }

    pub async fn contract_proof_set_remove(&self, contract_list: &Vec<ObjectId>) -> BuckyResult<()> {
        self.op_env.lock(vec!["/miner/contracts/list_proof/contract_proof_set".to_string()], 10000).await?;
        if let Some(set_id) = self.get_by_path("/miner/contracts/list_proof/", "contract_proof_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id.clone()).await?.get()?;
            let mut cset = list.0;
            for contract_id in contract_list.iter() {
                cset.remove(contract_id);
            }

            let set = ContractList(cset);
            self.save_by_path("/miner/contracts/list_proof/", "contract_proof_set", Some(&set_id), None, Some(&set)).await?;
        }

        Ok(())
    }

    async fn get_by_path(&self, path: impl Into<String>, key: impl Into<String>) -> BuckyResult<Option<ObjectId>> {
        let mut coid = None;
        if let Some(obj_id) = self.op_env.get_by_key(path, key).await? {
            coid = Some(obj_id);
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
                cur_obj_id = new_obj_id;
                self.put_object_to_noc(cur_obj_id.clone(), &raw_obj).await?;
            }
        }

        self.op_env.set_with_key(path, key, &cur_obj_id, None, true).await?;

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
}

#[async_trait::async_trait]
impl ContractMetaStore for CyfsStackMetaConnection {
    async fn get_contract(&mut self, contract_id: &ObjectId) -> BuckyResult<Option<DsgContractObject<DMCContractData>>> {
        if let Some(obj_id) = self.get_by_path(format!("/miner/contracts/{}/", contract_id), "contract").await? {
           Ok(Some(self.get_object_from_noc(obj_id).await?))
        } else {
            Ok(None)
        }
    }

    async fn get_contract_id_by_dmc_order(&mut self, dmc_order: &str) -> BuckyResult<Option<ObjectId>> {
        self.op_env.get_by_path(format!("/miner/dmc_orders/{}", dmc_order)).await
    }

    async fn save_contract(&mut self, contract: &DsgContractObject<DMCContractData>) -> BuckyResult<()> {
        let contract_id = self.stack.put_object_to_noc(&contract).await?;
        self.op_env.set_with_path(format!("/miner/contracts/{}/contract", contract_id.to_string()), &contract_id, None, true).await?;
        let contract_ref = DsgContractObjectRef::from(contract);
        self.op_env.set_with_path(format!("/miner/dmc_orders/{}", contract_ref.witness().order_id.as_str()), &contract_id, None, true).await?;
        Ok(())
    }

    async fn contract_sync_set(&mut self) -> BuckyResult<Vec<ObjectId>> {
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

        Ok(syn_set.into_iter().collect())
    }

    async fn contract_sync_set_add(&mut self, contract_list: &Vec<ObjectId>) -> BuckyResult<()> {
        self.op_env.lock(vec!["/miner/contracts/list_syn/contract_syn_set".to_string()], 10000).await?;
        let list = if let Some(set_id) = self.get_by_path("/miner/contracts/list_syn/", "contract_syn_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id).await?.get()?;
            let mut cset = list.0;
            for contract_id in contract_list.iter() {
                cset.insert(contract_id.clone());
            }
            cset
        } else {
            let mut cset = BTreeSet::new();
            for contract_id in contract_list.iter() {
                cset.insert(contract_id.clone());
            }
            cset
        };

        let set = ContractList(list);
        self.save_by_path("/miner/contracts/list_syn/", "contract_syn_set", None, None, Some(&set)).await?;

        Ok(())
    }

    async fn contract_sync_set_remove(&mut self, contract_list: &Vec<ObjectId>) -> BuckyResult<()> {
        self.op_env.lock(vec!["/miner/contracts/list_syn/contract_syn_set".to_string()], 10000).await?;
        if let Some(set_id) = self.get_by_path("/miner/contracts/list_syn/", "contract_syn_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id.clone()).await?.get()?;
            let mut cset = list.0;
            for contract_id in contract_list.iter() {
                cset.remove(contract_id);
            }

            let set = ContractList(cset);
            self.save_by_path("/miner/contracts/list_syn/", "contract_syn_set", None, None, Some(&set)).await?;
        }

        Ok(())
    }

    async fn contract_set(&mut self) -> BuckyResult<Vec<ObjectId>> {
        let mut set = BTreeSet::new();
        if let Some(set_id) = self.get_by_path("/miner/contracts/list/", "contract_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id).await?.get()?;
            set = list.0;
        }

        Ok(set.into_iter().collect())
    }

    async fn contract_set_add(&mut self, contract_list: &Vec<ObjectId>) -> BuckyResult<()> {
        self.op_env.lock(vec!["/miner/contracts/list/contract_set".to_string()], 10000).await?;
        let list = if let Some(set_id) = self.get_by_path("/miner/contracts/list/", "contract_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id).await?.get()?;
            let mut cset = list.0;
            for contract_id in contract_list.iter() {
                cset.insert(contract_id.clone());
            }
            cset
        } else {
            let mut cset = BTreeSet::new();
            for contract_id in contract_list.iter() {
                cset.insert(contract_id.clone());
            }
            cset
        };

        let set = ContractList(list);
        self.save_by_path("/miner/contracts/list/", "contract_set", None, None, Some(&set)).await?;

        Ok(())
    }

    async fn contract_set_remove(&mut self, contract_list: &Vec<ObjectId>) -> BuckyResult<()> {
        self.op_env.lock(vec!["/miner/contracts/list/contract_set".to_string()], 10000).await?;
        if let Some(set_id) = self.get_by_path("/miner/contracts/list/", "contract_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id.clone()).await?.get()?;
            let mut cset = list.0;
            for contract_id in contract_list.iter() {
                cset.remove(contract_id);
            }

            let set = ContractList(cset);
            self.save_by_path("/miner/contracts/list/", "contract_set", Some(&set_id), None, Some(&set)).await?;
        }

        Ok(())
    }

    async fn get_contract_info(&mut self, contract_id: &ObjectId) -> BuckyResult<ContractInfo> {
        let path = format!("/miner/contracts/{}/info", contract_id.to_string());
        let info_id = self.op_env.get_by_path(path.as_str()).await?;
        if info_id.is_none() {
            let contract_status = self.get_down_stat(&contract_id).await?;
            let latest_check_time = if let Some(obj_id) = self.get_by_path(format!("/miner/contracts/{}/", contract_id), "latest_check_time").await? {
                let LastCheckTime(latest_check_time) = self.get_object_from_noc::<RawObject>(obj_id).await?.get()?;
                latest_check_time
            } else {
                0
            };
            Ok(ContractInfo {
                contract_status,
                latest_check_time,
                meta_merkle: vec![]
            })
        } else {
            let contract_info: ContractInfo = self.stack.get_object_from_noc::<RawObject>(info_id.unwrap()).await?.get()?;
            Ok(contract_info)
        }
    }

    async fn set_contract_info(&mut self, contract_id: &ObjectId, contract_info: &ContractInfo) -> BuckyResult<()> {
        let path = format!("/miner/contracts/{}/info", contract_id.to_string());
        let raw_obj = RawObject::new(ObjectId::default(), ObjectId::default(), 34532, contract_info)?;
        let obj_id = self.stack.put_object_to_noc(&raw_obj).await?;
        self.op_env.set_with_path(path, &obj_id, None, true).await?;
        Ok(())
    }

    async fn contract_proof_set(&mut self) -> BuckyResult<Vec<ObjectId>> {
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

        Ok(proof_set.into_iter().collect())
    }

    async fn contract_proof_set_add(&mut self, contract_list: &Vec<ObjectId>) -> BuckyResult<()> {
        self.op_env.lock(vec!["/miner/contracts/list_proof/contract_proof_set".to_string()], 10000).await?;
        let list = if let Some(set_id) = self.get_by_path("/miner/contracts/list_proof/", "contract_proof_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id).await?.get()?;
            let mut cset = list.0;
            for contract_id in contract_list.iter() {
                cset.insert(contract_id.clone());
            }
            cset
        } else {
            let mut cset = BTreeSet::new();
            for contract_id in contract_list.iter() {
                cset.insert(contract_id.clone());
            }
            cset
        };

        let set = ContractList(list);
        self.save_by_path("/miner/contracts/list_proof/", "contract_proof_set", None, None, Some(&set)).await?;

        Ok(())
    }

    async fn contract_proof_set_remove(&mut self, contract_list: &Vec<ObjectId>) -> BuckyResult<()> {
        self.op_env.lock(vec!["/miner/contracts/list_proof/contract_proof_set".to_string()], 10000).await?;
        if let Some(set_id) = self.get_by_path("/miner/contracts/list_proof/", "contract_proof_set").await? {
            let list: ContractList = self.get_object_from_noc::<RawObject>(set_id.clone()).await?.get()?;
            let mut cset = list.0;
            for contract_id in contract_list.iter() {
                cset.remove(contract_id);
            }

            let set = ContractList(cset);
            self.save_by_path("/miner/contracts/list_proof/", "contract_proof_set", Some(&set_id), None, Some(&set)).await?;
        }

        Ok(())
    }

    async fn get_contract_state(&mut self, contract_id: &ObjectId) -> BuckyResult<Option<DsgContractStateObject>> {
        if let Some(obj_id) = self.get_by_path(format!("/miner/contracts/{}/", contract_id), "state").await? {
            Ok(Some(self.get_object_from_noc(obj_id).await?))
         } else {
             Ok(None)
         }
    }

    async fn get_contract_state_id(&mut self, contract_id: &ObjectId) -> BuckyResult<Option<ObjectId>> {
        self.get_by_path(format!("/miner/contracts/{}/", contract_id), "state").await
    }

    async fn get_syncing_contract_state(&mut self, contract_id: &ObjectId) -> BuckyResult<Option<DsgContractStateObject>> {
        let path = format!("/miner/contracts/{}/syncing_state", contract_id.to_string());
        let syncing_id = self.op_env.get_by_path(path.as_str()).await?;
        if syncing_id.is_some() {
            Ok(Some(self.stack.get_object_from_noc(syncing_id.unwrap()).await?))
        } else {
            Ok(None)
        }
    }

    async fn save_need_sync_contract_state(&mut self, contract_id: &ObjectId, state: &DsgContractStateObject) -> BuckyResult<()> {
        let state_ref = DsgContractStateObjectRef::from(state);
        let state_id = state_ref.id();
        self.save_by_path(format!("/miner/contracts/{}/", contract_id), "syncing_state", Some(&state_id), Some(state), None).await?;
        self.op_env.insert(format!("/miner/contracts/{}/states", contract_id), &state_id).await?;

        Ok(())
    }

    async fn set_contract_state_sync_complete(&mut self, contract_id: &ObjectId, state_id: &ObjectId) -> BuckyResult<()> {
        let path = format!("/miner/contracts/{}/syncing_state", contract_id.to_string());
        let syncing_id = self.op_env.get_by_path(path.as_str()).await?;
        assert!(syncing_id.is_some());
        assert_eq!(syncing_id.as_ref().unwrap(), state_id);
        self.op_env.remove_with_path(path.as_str(), None).await?;
        self.op_env.set_with_path(format!("/miner/contracts/{}/state", contract_id.to_string()), state_id, None, true).await?;
        Ok(())
    }

    async fn save_state_id_by_path(&mut self, path: String, object_id: &ObjectId) -> BuckyResult<()> {
        let key = hash(path).await;
        self.op_env.set_with_key(format!("/miner/contracts/http_path/"), key, object_id, None, true).await?;

        Ok(())
    }

    async fn get_state_id_by_path(&mut self, path: String) -> BuckyResult<Option<ObjectId>> {
        let key = hash(path).await;
        let rt = if let Some(object_id) = self.op_env.get_by_key(format!("/miner/contracts/http_path/"), key).await? {
            Ok(Some(object_id))
        } else {
            Ok(None)
        };
        rt
    }

    async fn save_state(&mut self, state: &DsgContractStateObject) -> BuckyResult<()> {
        let state_ref = DsgContractStateObjectRef::from(state);
        self.op_env.insert("/miner/states/", &state_ref.id()).await?;
        Ok(())
    }

    async fn get_state(&mut self, state_id: ObjectId) -> BuckyResult<Option<DsgContractStateObject>> {
        match self.stack.get_object_from_noc(state_id).await {
            Ok(obj) => Ok(Some(obj)),
            Err(e) => {
                if e.code() == BuckyErrorCode::NotFound {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn get_chunks_by_path(&mut self, url_path: String) -> BuckyResult<Vec<ChunkId>> {
        let chunks = vec![];
        if let Some(state_id) = self.get_state_id_by_path(url_path).await? {
            let state = self.get_state(state_id).await?;
            if state.is_some() {
                let state_ref = DsgContractStateObjectRef::from(state.as_ref().unwrap());
                if let DsgContractState::DataSourceChanged(change) = state_ref.state() {
                    return Ok(change.chunks.clone())
                }
            }
        }
        Ok(chunks)
    }

    async fn get_chunk_list(&mut self, contract_id: &ObjectId) -> BuckyResult<Vec<ChunkId>> {
        if let Some(obj_id) = self.get_by_path(format!("/miner/contracts/{}/", contract_id), "chunk_list").await? {
            let chunk_list: ContractChunkList = self.get_object_from_noc::<RawObject>(obj_id).await?.get()?;
            Ok(chunk_list.0)
        } else {
            Ok(Vec::new())
        }
    }

    async fn save_chunk_list(&mut self, contract_id: &ObjectId, chunk_list: Vec<ChunkId>) -> BuckyResult<()> {
        let set = ContractChunkList(chunk_list);
        self.save_by_path(format!("/miner/contracts/{}/", contract_id), "chunk_list", None, None, Some(&set)).await?;

        Ok(())
    }

    async fn get_challenge(&mut self, contract_id: &ObjectId) -> BuckyResult<Option<DsgChallengeObject>> {
        if let Some(obj_id) = self.get_by_path(format!("/miner/contracts/{}/", contract_id), "challenge").await? {
            match self.get_object_from_noc::<DsgChallengeObject>(obj_id).await {
                Ok(obj) => {
                    Ok(Some(obj))
                }
                Err(e) => {
                    if e.code() == BuckyErrorCode::NotFound {
                        Ok(None)
                    } else {
                        Err(e)
                    }
                }
            }
         } else {
             Ok(None)
         }
    }

    async fn save_challenge(&mut self, contract_id: &ObjectId, challenge: &DsgChallengeObject) -> BuckyResult<()> {
        let challenge_obj = DsgChallengeObjectRef::from(challenge);
        let challenge_id = challenge_obj.id();
        self.save_by_path(format!("/miner/contracts/{}/", contract_id), "challenge", Some(&challenge_id), Some(challenge), None).await?;

        Ok(())
    }

    async fn chunk_ref_add(&mut self, contract_id: &ObjectId, chunk_list: &Vec<ChunkId>) -> BuckyResult<()> {
        self.chunk_ref_create(contract_id, chunk_list).await
    }

    async fn chunk_ref_del(&mut self, contract_id: &ObjectId, chunk_list: &Vec<ChunkId>) -> BuckyResult<()> {
        self.chunk_ref_remove(contract_id, chunk_list).await
    }

    async fn chunk_del_list_del(&mut self, chunk_list: &Vec<ChunkId>) -> BuckyResult<()> {
        self.del_list_remove(chunk_list).await
    }

    async fn get_del_chunk_list(&mut self) -> BuckyResult<Vec<ChunkId>> {
        Ok(self.del_list().await?.iter().map(|v| v.clone()).collect())
    }

    async fn get_chunk_merkle_root(&mut self, chunk_list: &Vec<ChunkId>, merkle_chunk_size: u32) -> BuckyResult<Vec<(ChunkId, HashValue)>> {
        let leafs = if merkle_chunk_size % DSG_CHUNK_PIECE_SIZE as u32 == 0 { merkle_chunk_size / DSG_CHUNK_PIECE_SIZE as u32 } else { merkle_chunk_size / DSG_CHUNK_PIECE_SIZE as u32 + 1};
        let mut hash_list = Vec::new();
        let chunk_store = Arc::new(NocChunkStore::new(self.stack.clone()));
        for chunk_id in chunk_list.iter() {
            let merkle = MerkleTree::create_from_raw(
                AsyncMerkleChunkReader::new(MerkleChunkReader::new(chunk_store.clone(), vec![chunk_id.clone()], merkle_chunk_size, None)),
                HashVecStore::<Vec<u8>>::new::<MemVecCache>(leafs as u64)?).await?;
            hash_list.push((chunk_id.clone(), HashValue::from(merkle.root())))
        }
        Ok(hash_list)
    }

    async fn get_chunk_merkle_data(&mut self, chunk_id: &ChunkId, merkle_chunk_size: u32) -> BuckyResult<(HashValue, Vec<u8>)> {
        let leafs = if merkle_chunk_size % DSG_CHUNK_PIECE_SIZE as u32 == 0 { merkle_chunk_size / DSG_CHUNK_PIECE_SIZE as u32 } else { merkle_chunk_size / DSG_CHUNK_PIECE_SIZE as u32 + 1};
        let chunk_store = Arc::new(NocChunkStore::new(self.stack.clone()));
        let merkle = MerkleTree::create_from_raw(
            AsyncMerkleChunkReader::new(MerkleChunkReader::new(chunk_store.clone(), vec![chunk_id.clone()], merkle_chunk_size, None)),
            HashVecStore::<Vec<u8>>::new::<MemVecCache>(leafs as u64)?).await?;
        let root = merkle.root();
        let data = merkle.get_cache().get_data(0)?;
        Ok((HashValue::from(root), data.to_vec()))
    }
}

pub async fn hash(data: impl AsRef<[u8]>) -> String {
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}",hasher.finalize())
}
