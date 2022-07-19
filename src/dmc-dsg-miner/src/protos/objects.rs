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
