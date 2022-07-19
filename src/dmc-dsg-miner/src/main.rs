use dmc_dsg_miner::*;
use anyhow::Result;
use cyfs_lib::*;
use std::{sync::Arc};
use std::str::FromStr;
use cyfs_base::{BuckyResult, ObjectId};
use cyfs_core::{DecApp, DecAppObj};
use cyfs_util::process::ProcessAction;

#[async_std::main]
async fn main() -> Result<()> {
    let status = cyfs_util::process::check_cmd_and_exec(DMCDsgConfig::APP_NAME);
    if status == ProcessAction::Install {
        std::process::exit(0);
    }

    cyfs_debug::CyfsLoggerBuilder::new_app(DMCDsgConfig::APP_NAME)
        .level("debug")
        .console("info")
        .build()
        .unwrap()
        .start();

    cyfs_debug::PanicBuilder::new(DMCDsgConfig::APP_NAME, DMCDsgConfig::APP_NAME)
        .exit_on_panic(true)
        .build()
        .start();

    let dec_id = DecApp::generate_id(ObjectId::from_str(DMCDsgConfig::PUB_PEOPLE_ID).unwrap(), DMCDsgConfig::PRODUCT_NAME);
    let stack = Arc::new(SharedCyfsStack::open_default(Some(dec_id.clone())).await.unwrap());
    stack.wait_online(None).await.unwrap();

    let meta_store: Arc<Box<dyn ContractMetaStore>> = Arc::new(Box::new(StackStore::new(stack.clone())));

    let raw_data_store: Arc<Box<dyn ContractChunkStore>> = Arc::new(Box::new(NocChunkStore::new(stack.clone())));

    let app = App::new(
        stack.clone(),
        meta_store.clone(),
        raw_data_store.clone(),
        "http://154.39.158.47:8870".to_string()).await?;
    if let Err(e) = app.init().await {
        if get_app_err_code(&e) != DMC_DSG_ERROR_REPORT_FAILED {
            BuckyResult::<()>::Err(e).unwrap();
        }
    }

    let service = DMCDsgService::new(app, dec_id);
    service.listen().await.unwrap();

    CacheHttp::run(meta_store, raw_data_store).await.unwrap();

    Ok(())
}

