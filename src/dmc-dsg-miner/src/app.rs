use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use cyfs_base::*;
use cyfs_core::{DecApp, DecAppObj};
use cyfs_dsg_client::{DsgContractState, DsgContractStateObjectRef};
use cyfs_lib::SharedCyfsStack;
use dmc_dsg_base::{Setting, SettingRef, DMCDsgConfig, CyfsPath, JSONObject, DSGJSON, CyfsClient, CyfsNOC};
use crate::{ContractMetaStore, CyfsStackFileDownloader, CyfsStackMetaStore, DMC, DmcDsgMiner, MetaStore, MinerStat, NocChunkStore, OodMiner, RemoteDMCTxSender, RemoteProtocol};

pub struct App {
    setting: SettingRef,
    chunk_meta: Arc<CyfsStackMetaStore>,
    raw_data_store: Arc<NocChunkStore>,
    stack: Arc<SharedCyfsStack>,
    miner: Mutex<Option<OodMiner>>,
    dmc_server: String,
    dmc_tracker_server: String,
    dec_id: ObjectId,
    dmc_dsg_dec_id: ObjectId,
    challenge_check_interval: u64,
}
pub type AppRef = Arc<App>;

impl App {
    pub async fn new(
        stack: Arc<SharedCyfsStack>,
        chunk_meta: Arc<CyfsStackMetaStore>,
        raw_data_store: Arc<NocChunkStore>,
        dmc_server: String,
        dmc_tracker_server: String,
        dec_id: ObjectId,
        challenge_check_interval: u64,
    ) -> BuckyResult<AppRef> {
        let setting = Setting::new(stack.clone());
        setting.load().await?;

        let dmc_dsg_dec_id = DecApp::generate_id(ObjectId::from_str(DMCDsgConfig::PUB_PEOPLE_ID).unwrap(), "DMC DSG service");
        Ok(AppRef::new(Self {
            setting,
            chunk_meta,
            raw_data_store,
            stack,
            miner: Mutex::new(None),
            dmc_server,
            dmc_tracker_server,
            dec_id,
            dmc_dsg_dec_id,
            challenge_check_interval
        }))
    }

    pub fn get_stack(&self) -> &Arc<SharedCyfsStack> {
        &self.stack
    }

    async fn set_object_access(&self) -> BuckyResult<()> {
        let mut conn = self.chunk_meta.create_meta_connection().await?;
        conn.begin().await?;
        let contract_set = conn.contract_set().await?;
        for contract_id in contract_set.iter() {
            let contract = conn.get_contract(contract_id).await?;
            if contract.is_none() {
                continue;
            }
            conn.save_contract(contract.as_ref().unwrap()).await?;
            let state = conn.get_contract_state(contract_id).await?;
            if state.is_none() {
                continue;
            }
            conn.save_state(state.as_ref().unwrap()).await?;
            let mut cur_state = state.unwrap();
            loop {
                let state_ref = DsgContractStateObjectRef::from(&cur_state);
                if let DsgContractState::DataSourceChanged(changed) = state_ref.state() {
                    if changed.prev_change.is_none() {
                        break;
                    }
                    cur_state = self.stack.get_object_from_noc(changed.prev_change.unwrap()).await?;
                    conn.save_state(&cur_state).await?;
                } else {
                    break;
                }
            }
        }
        conn.commit().await?;
        Ok(())
    }

    pub async fn init(&self) -> BuckyResult<()> {
        {
            if self.miner.lock().unwrap().is_some() {
                return Ok(());
            }
        }

        let has_changed_obj_access = self.chunk_meta.get_setting("has_changed_obj_access", "0").await?;
        if has_changed_obj_access == "0".to_string() {
            self.set_object_access().await?;
            self.chunk_meta.set_setting("has_changed_obj_access".to_string(), "1".to_string()).await?;
        }

        loop {
            let dmc_account = self.get_dmc_account().await?;
            if dmc_account.is_some() {
                let dmc_sender = RemoteDMCTxSender::new(self.stack.clone(), self.dmc_dsg_dec_id.clone());
                let dmc = DMC::new(
                    self.stack.clone(),
                    self.dec_id.clone(),
                    self.chunk_meta.clone(),
                    self.raw_data_store.clone(),
                    self.dmc_server.as_str(),
                    self.dmc_tracker_server.as_str(),
                    dmc_account.as_ref().unwrap().as_str(),
                    self.get_http_domain().await?,
                    dmc_sender,
                    self.challenge_check_interval)?;
                loop {
                    if let Err(e) = self.set_miner_dec_id().await {
                        log::error!("set_miner_dec_id err {}", e);
                        async_std::task::sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                    break;
                }

                let miner = DmcDsgMiner::new(
                    self.stack.clone(),
                    self.chunk_meta.clone(),
                    self.raw_data_store.clone(),
                    dmc.clone(),
                    CyfsStackFileDownloader::new(self.stack.clone(), self.dec_id.clone()));
                miner.start_chunk_sync().await?;
                miner.start_proof_resp().await;
                miner.start_contract_end_check().await;

                let service = OodMiner::new(self.stack.clone(), miner.clone()).await?;
                *self.miner.lock().unwrap() = Some(service);
                break;
            }
            async_std::task::sleep(Duration::from_secs(5)).await
        }
        Ok(())
    }

    async fn get_dmc_account(&self) -> BuckyResult<Option<String>> {
        let device = self.stack.local_device();
        let local_id = device.desc().object_id();
        let owner_id = device.desc().owner().as_ref().unwrap().clone();
        let req_path = CyfsPath::new(local_id, self.dmc_dsg_dec_id.clone(), "commands").to_path();
        let req = JSONObject::new(self.dec_id.clone(), owner_id, RemoteProtocol::GetDMCAccount as u16, &"".to_string())?;
        match self.stack.put_object_with_resp2::<JSONObject>(req_path.as_str(), req.desc().object_id(), req.to_vec()?).await {
            Ok(ret) => {
                let account: String = ret.get()?;
                if account.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(account))
                }
            }
            Err(e) => {
                log::error!("get dmc account error {}", e);
                Ok(None)
            }
        }
    }

    pub async fn get_stat(&self) -> BuckyResult<MinerStat> {
        let miner = self.miner.lock().unwrap().clone();
        if miner.is_none() {
            Ok(MinerStat {
                bill_count: 0,
                order_count: 0,
                billed_space: 0,
                selled_space: 0,
                used_space: 0,
            })
        } else {
            miner.as_ref().unwrap().get_dmc_miner().get_dsg_stat().await
        }
    }

    async fn set_miner_dec_id(&self) -> BuckyResult<()> {
        let device = self.stack.local_device();
        let local_id = device.desc().object_id();
        let owner_id = device.desc().owner().as_ref().unwrap().clone();
        let req_path = CyfsPath::new(local_id, self.dmc_dsg_dec_id.clone(), "dmc_chain_commands").to_path();
        let req = JSONObject::new(self.dec_id.clone(), owner_id, RemoteProtocol::SetMinerDecId as u16, &"".to_string())?;
        self.stack.put_object_with_resp2::<JSONObject>(req_path.as_str(), req.desc().object_id(), req.to_vec()?).await?;
        Ok(())
    }

    pub async fn get_http_domain(&self) -> BuckyResult<String> {
        let domain = self.setting.get_setting("http_domain", "");
        Ok(domain)
    }

    pub async fn set_http_domain(&self, domain: String) -> BuckyResult<()> {
        self.setting.set_setting("http_domain".to_string(), domain);
        self.setting.save().await?;
        Ok(())
    }
}
