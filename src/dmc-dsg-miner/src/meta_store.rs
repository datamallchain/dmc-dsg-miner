use async_trait::async_trait;
use cyfs_base::BuckyResult;
use dmc_dsg_base::{GuardObject, Locker, MetaConnectionProxy};
use crate::ContractMetaStore;

#[async_trait]
pub trait MetaStore<T: ContractMetaStore>: 'static + Send + Sync {
    async fn get_setting(&self, key: &str, default: &str) -> BuckyResult<String>;
    async fn set_setting(&self, key: String, value: String) -> BuckyResult<()>;
    async fn create_meta_connection_named_locked(&self, name: String) -> BuckyResult<GuardObject<MetaConnectionProxy<T>>> {
        let locker = Locker::get_locker(name).await;
        let conn = self.create_meta_connection().await?;
        Ok(GuardObject::new(locker, conn))
    }

    async fn create_meta_connection(&self) -> BuckyResult<MetaConnectionProxy<T>>;
}
