#[macro_use]
extern crate log;

macro_rules! bucky_result {
    ($x:expr) => ($x.map_err(|err|BuckyError::from(format!("{}",err))));
}

mod miner_config;
mod miner;
mod http_cache;
mod contract_store;
mod noc_store;
mod miner_challenge;
mod stack_store;
mod merkle;
mod reader_tool;
mod dmc;
mod protos;
mod raw_obj;
mod service;
mod app;
mod merkle_chunk_reader;
mod contract_info;
mod meta_store;
mod file_downloader;

pub use miner_config::*;
pub use miner::*;
pub use http_cache::*;
pub use contract_store::*;
pub use noc_store::*;
pub use miner_challenge::*;
pub use stack_store::*;
pub use merkle::*;
pub use reader_tool::*;
pub use dmc::*;
pub use raw_obj::*;
pub use dmc_dsg_base::*;
pub use service::*;
pub use app::*;
pub use merkle_chunk_reader::*;
pub use contract_info::*;
pub use meta_store::*;
pub use file_downloader::*;
