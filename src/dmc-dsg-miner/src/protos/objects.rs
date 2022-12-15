#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DmcContractData {
    #[prost(string, tag="1")]
    pub order_id: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub miner_dmc_account: ::prost::alloc::string::String,
    #[prost(bytes="vec", optional, tag="3")]
    pub merkle_root: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    #[prost(uint32, optional, tag="4")]
    pub chunk_size: ::core::option::Option<u32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ContractInfo {
    #[prost(uint32, tag="1")]
    pub contract_status: u32,
    #[prost(uint64, tag="2")]
    pub latest_check_time: u64,
    #[prost(bytes="vec", repeated, tag="3")]
    pub meta_merkle: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    #[prost(uint64, optional, tag="4")]
    pub stored_size: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag="5")]
    pub sum_size: ::core::option::Option<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MetaData {
    #[prost(bytes="vec", tag="1")]
    pub contract: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes="vec", tag="2")]
    pub state_list: ::prost::alloc::vec::Vec<u8>,
}
