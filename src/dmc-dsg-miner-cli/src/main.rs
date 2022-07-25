use std::str::FromStr;
use std::sync::Arc;
use clap::{SubCommand};
use config::builder::DefaultState;
use config::ConfigBuilder;
use cyfs_base::{ObjectId};
use cyfs_core::{DecApp, DecAppObj};
use cyfs_lib::SharedCyfsStack;
use cyfs_util::get_app_data_dir;
use dmc_dsg_base::DMCDsgConfig;
use dmc_dsg_miner_cli::{App, RuntimeLauncher};

#[async_std::main]
async fn main() {
    let matches = clap::App::new("dmc-dsg-client")
        .subcommand(SubCommand::with_name("create_light_auth").about("Create the low-privilege private key needed to run the DSG")
            .arg(clap::Arg::with_name("dmc_account").required(true))
            .arg(clap::Arg::with_name("private_key").required(true)))
        .subcommand(SubCommand::with_name("stake").about("Enter the amount of DMC you want to stake")
            .arg(clap::Arg::with_name("dmc_account").required(true))
            .arg(clap::Arg::with_name("private_key").required(true))
            .arg(clap::Arg::with_name("amount").required(true)))
        .subcommand(SubCommand::with_name("mint").about("Enter the amount of PST you want to mint")
            .arg(clap::Arg::with_name("dmc_account").required(true))
            .arg(clap::Arg::with_name("private_key").required(true))
            .arg(clap::Arg::with_name("amount").required(true)))
        .subcommand(SubCommand::with_name("bill").about("Enter the amount of PST you want to sell")
            .arg(clap::Arg::with_name("dmc_account").required(true))
            .arg(clap::Arg::with_name("private_key").required(true))
            .arg(clap::Arg::with_name("amount").required(true))
            .arg(clap::Arg::with_name("price").required(true)))
        .subcommand(SubCommand::with_name("info").about("Get info about miner")
            .arg(clap::Arg::with_name("dmc_account").required(true))).get_matches();

    if cfg!(not(debug_assertions)) {
        async_std::task::block_on(RuntimeLauncher::launch());
    }

    cyfs_debug::CyfsLoggerBuilder::new_app(DMCDsgConfig::APP_SERVER_NAME)
        .level("debug")
        .console("off")
        .enable_bdt(Some("off"), Some("off"))
        .disable_file_config(true)
        .module("non-lib", Some("off"), Some("off"))
        .build()
        .unwrap()
        .start();

    cyfs_debug::PanicBuilder::new(DMCDsgConfig::PRODUCT_NAME, DMCDsgConfig::APP_SERVER_NAME)
        .build()
        .start();

    let mut config = ConfigBuilder::<DefaultState>::default()
        .set_default("dmc_server", "http://154.22.122.40:8870").unwrap()
        .build().unwrap();
    let data_dir = get_app_data_dir(DMCDsgConfig::APP_NAME);
    let config_path = data_dir.join("config.toml");
    if config_path.exists() {
        let file = config::File::from(config_path.as_path());
        config.merge(file).unwrap();
    }

    let dec_id = DecApp::generate_id(ObjectId::from_str(DMCDsgConfig::PUB_PEOPLE_ID).unwrap(), DMCDsgConfig::PRODUCT_NAME);
    log::info!("dec_id:{} product_name:{}", dec_id.to_string(), DMCDsgConfig::PRODUCT_NAME);
    let object_stack = Arc::new(SharedCyfsStack::open_runtime(Some(dec_id.clone())).await.unwrap());
    object_stack.wait_online(None).await.unwrap();

    let app = App::new(object_stack, dec_id, config.get_string("dmc_server").unwrap()).await.unwrap();
    match matches.subcommand() {
        ("create_light_auth", matches) => {
            let dmc_account = matches.as_ref().unwrap().value_of("dmc_account").unwrap();
            let private_key = matches.as_ref().unwrap().value_of("private_key").unwrap();
            if let Err(e) = app.create_light_auth(dmc_account, private_key).await {
                log::error!("create light auth err {}", e);
                println!("create light auth err {}", e);
            }
        }
        ("stake", matches) => {
            let dmc_account = matches.as_ref().unwrap().value_of("dmc_account").unwrap();
            let private_key = matches.as_ref().unwrap().value_of("private_key").unwrap();
            let amount = matches.as_ref().unwrap().value_of("amount").unwrap();
            if let Err(e) = app.stake(dmc_account, private_key, amount).await {
                log::error!("stack err {}", e);
                println!("stack err {}", e);
            }
        }
        ("mint", matches) => {
            let dmc_account = matches.as_ref().unwrap().value_of("dmc_account").unwrap();
            let private_key = matches.as_ref().unwrap().value_of("private_key").unwrap();
            let amount = matches.as_ref().unwrap().value_of("amount").unwrap();
            if let Err(e) = app.mint(dmc_account, private_key, amount).await {
                log::error!("mint err {}", e);
                println!("mint err {}", e);
            }
        }
        ("bill", matches) => {
            let dmc_account = matches.as_ref().unwrap().value_of("dmc_account").unwrap();
            let private_key = matches.as_ref().unwrap().value_of("private_key").unwrap();
            let amount = matches.as_ref().unwrap().value_of("amount").unwrap();
            let price = matches.as_ref().unwrap().value_of("price").unwrap();
            if let Err(e) = app.bill(dmc_account, private_key, amount, price.parse().unwrap()).await {
                log::error!("bill err {}", e);
                println!("bill err {}", e);
            }
        }
        ("info", matches) => {
            let dmc_account = matches.as_ref().unwrap().value_of("dmc_account").unwrap();
            match app.get_info(dmc_account).await {
                Ok(info) => {
                    println!("account:{}", info.dmc_account);
                    println!("stake DMC:{}", info.stake_dmc);
                    println!("PST:{}", info.pst);
                    println!("max PST amount:{}", info.max_mint_pst);
                    println!("price:{}", info.price);
                },
                Err(e) => {
                    log::error!("bill err {}", e);
                    println!("bill err {}", e);
                }
            }
        }
        _ => {}
    }
}
