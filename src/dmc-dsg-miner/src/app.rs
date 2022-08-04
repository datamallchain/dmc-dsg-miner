use std::str::FromStr;
use std::sync::{Arc, Mutex};
use cyfs_base::*;
use cyfs_lib::SharedCyfsStack;
use dmc_dsg_base::{app_err2, app_msg, DMCPrivateKey, Setting, SettingRef, DMC_DSG_ERROR_REPORT_FAILED};
use crate::{ContractChunkStore, ContractMetaStore, DelegateImpl, DMC, OodMiner};

pub struct App {
    setting: SettingRef,
    chunk_meta: Arc<Box<dyn ContractMetaStore>>,
    raw_data_store: Arc<Box<dyn ContractChunkStore>>,
    stack: Arc<SharedCyfsStack>,
    miner: Mutex<Option<OodMiner<DelegateImpl>>>,
    dmc_server: String,
}
pub type AppRef = Arc<App>;

impl App {
    pub async fn new(
        stack: Arc<SharedCyfsStack>,
        chunk_meta: Arc<Box<dyn ContractMetaStore>>,
        raw_data_store: Arc<Box<dyn ContractChunkStore>>,
        dmc_server: String,
    ) -> BuckyResult<AppRef> {
        let setting = Setting::new(stack.clone());
        setting.load().await?;
        Ok(AppRef::new(Self {
            setting,
            chunk_meta,
            raw_data_store,
            stack,
            miner: Mutex::new(None),
            dmc_server
        }))
    }

    pub fn get_stack(&self) -> &Arc<SharedCyfsStack> {
        &self.stack
    }

    pub async fn init(&self) -> BuckyResult<()> {
        {
            if self.miner.lock().unwrap().is_some() {
                return Ok(());
            }
        }

        let dmc_account = self.get_dmc_account().await?;
        if !dmc_account.is_empty() {
            let dmc_account = self.get_dmc_account().await?;
            let dmc_key = self.get_dmc_key(dmc_account.clone()).await?;
            let dmc = DMC::new(
                self.stack.clone(),
                self.chunk_meta.clone(),
                self.raw_data_store.clone(),
                self.dmc_server.as_str(),
                dmc_account.as_str(),
                dmc_key.clone(),
                self.get_http_domain().await?)?;
            if let Err(e) = dmc.report_cyfs_info().await {
                return if e.code() == BuckyErrorCode::InvalidData {
                    Err(app_err2!(DMC_DSG_ERROR_REPORT_FAILED, app_msg!("{}", e)))
                } else {
                    Err(e)
                }
            }

            let delegate =DelegateImpl::new(self.stack.clone(), self.chunk_meta.clone(), self.raw_data_store.clone(), dmc.clone());
            delegate.store.sync_chunk_data().await;
            delegate.store.first_proof().await;
            delegate.store.contract_end_del().await;

            let service = OodMiner::new(self.stack.clone(), delegate).await?;
            *self.miner.lock().unwrap() = Some(service);
        }
        Ok(())
    }

    pub async fn set_dmc_account(&self, dmc_account: String) -> BuckyResult<()> {
        self.setting.set_setting("dmc_account".to_string(), dmc_account);
        self.setting.save().await?;
        self.init().await?;
        Ok(())
    }

    pub async fn get_dmc_account(&self) -> BuckyResult<String> {
        Ok(self.setting.get_setting("dmc_account", ""))
    }

    pub async fn get_dmc_key(&self, dmc_account: String) -> BuckyResult<String> {
        let key_name = format!("{}_dmc_key", dmc_account);
        let private_key = self.setting.get_setting(key_name.as_str(), "");
        let dmc_private_key =if private_key.is_empty() {
            let dmc_private_key = DMCPrivateKey::gen_key();
            let private_key = dmc_private_key.to_legacy_string()?;
            self.setting.set_setting(key_name, private_key.clone());
            self.setting.save().await?;
            private_key
        } else {
            private_key
        };

        Ok(dmc_private_key)
    }

    pub async fn get_dmc_public_key(&self, dmc_account: String) -> BuckyResult<String> {
        let private_key = self.get_dmc_key(dmc_account).await?;
        DMCPrivateKey::from_str(private_key.as_str())?.get_public_key().to_legacy_string()
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
