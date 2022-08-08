use serde::{Serialize, Deserialize};

pub enum JsonProtocol {
    GetDMCKey,
    GetDMCKeyResp,
    GetDMCAccount,
    GetDMCAccountResp,
    SetDMCAccount,
    SetDMCAccountResp,
    SetHttpDomain,
    SetHttpDomainResp,
}

#[derive(Serialize, Deserialize)]
pub struct SetDMCAccount {
    pub dmc_account: String,
    pub dmc_key: String,
}
