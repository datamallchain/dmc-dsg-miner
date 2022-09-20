use dmc_dsg_miner::*;
use anyhow::Result;
use cyfs_lib::*;
use std::{sync::Arc};
use std::str::FromStr;
use cyfs_base::{BuckyResult, ObjectId};
use cyfs_core::{DecApp, DecAppObj};
use cyfs_util::get_app_data_dir;
use cyfs_util::process::ProcessAction;
use config::builder::DefaultState;
use config::ConfigBuilder;

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

    let mut builder = ConfigBuilder::<DefaultState>::default();
    builder = builder.set_default("dmc_server", "http://154.22.122.40:8870").unwrap();

    let data_dir = get_app_data_dir(DMCDsgConfig::APP_NAME);
    let config_path = data_dir.join("config.toml");
    if config_path.exists() {
        builder = builder.add_source(config::File::new(&config_path.display().to_string(), config::FileFormat::Toml));
    }
    let config = builder.build().unwrap();

    let dec_id = DecApp::generate_id(ObjectId::from_str(DMCDsgConfig::PUB_PEOPLE_ID).unwrap(), DMCDsgConfig::PRODUCT_NAME);
    log::info!("----> dec_id: {}", &dec_id);
    let stack = Arc::new(SharedCyfsStack::open_default(Some(dec_id.clone())).await.unwrap());
    stack.wait_online(None).await.unwrap();

    let meta_store = CyfsStackMetaStore::create(stack.clone()).await.unwrap();

    let raw_data_store = Arc::new(NocChunkStore::new(stack.clone()));

    let app = App::new(
        stack.clone(),
        meta_store.clone(),
        raw_data_store.clone(),
        config.get_string("dmc_server").unwrap(),
        dec_id.clone()).await?;
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

