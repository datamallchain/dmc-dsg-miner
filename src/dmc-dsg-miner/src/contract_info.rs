use crate::{ContractStatus};
use cyfs_base::*;

#[derive(Clone, ProtobufEncode, ProtobufDecode, ProtobufTransformType)]
#[cyfs_protobuf_type(crate::protos::ContractInfo)]
pub struct ContractInfo {
    pub contract_status: ContractStatus,
    pub latest_check_time: u64,
    pub meta_merkle: Vec<HashValue>,
}

impl ProtobufTransform<crate::protos::ContractInfo> for ContractInfo {
    fn transform(value: crate::protos::ContractInfo) -> BuckyResult<Self> {
        Ok(Self {
            contract_status: ContractStatus::from(value.contract_status as i64),
            latest_check_time: value.latest_check_time,
            meta_merkle: value.meta_merkle.iter().map(|v| HashValue::from(v.as_slice())).collect()
        })
    }
}

impl ProtobufTransform<&ContractInfo> for crate::protos::ContractInfo {
    fn transform(value: &ContractInfo) -> BuckyResult<Self> {
        let contract_status: i64 = value.contract_status.into();
        Ok(Self {
            contract_status: contract_status as u32,
            latest_check_time: value.latest_check_time,
            meta_merkle: value.meta_merkle.iter().map(|v|v.as_slice().to_vec()).collect()
        })
    }
}
