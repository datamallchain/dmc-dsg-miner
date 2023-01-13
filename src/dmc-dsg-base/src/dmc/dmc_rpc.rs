use cyfs_base::{bucky_time_now, BuckyErrorCode, BuckyResult, js_time_to_bucky_time};
use json::{JsonValue, object};
use serde::{Serialize, Deserialize};
use crate::*;

#[derive(Serialize, Deserialize)]
struct CreateAccountParam<'a> {
    account: &'a str,
    hash: &'a str,
    pubkey: &'a str,
    t: u64,
}

#[derive(Serialize, Deserialize)]
struct CreateAccountResp {
    pub message: Option<String>,
    pub pubkey: Option<String>,
    pub account: Option<String>,
    pub code: Option<String>,
    pub error: Option<String>,
}

pub struct TransactionHeader {
    pub expiration: TimePointSec,
    pub ref_block_num: u16,
    pub ref_block_prefix: u32,
}

#[derive(Serialize, Deserialize)]
pub struct AccountDelta {
    pub account: String,
    pub delta: i64,
}

#[derive(Serialize, Deserialize)]
pub struct ProcessedAction {
    pub account: String,
    pub name: String,
    pub authorization: Vec<PermissionLevel>,
    // pub data: Option<D>,
    pub hex_data: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ActionReceipt {
    pub receiver: String,
    pub act_digest: String,
    pub global_sequence: i64,
    pub recv_sequence: i64,
    pub auth_sequence: Vec<(String, i64)>,
    pub code_sequence: i64,
    pub abi_sequence: i64,
}

#[derive(Serialize, Deserialize)]
pub struct ActionTrace {
    pub action_ordinal: i64,
    pub creator_action_ordinal: i64,
    pub closest_unnotified_ancestor_action_ordinal: i64,
    pub receipt: ActionReceipt,
    pub receiver: String,
    pub act: ProcessedAction,
    pub context_free: bool,
    pub elapsed: i64,
    pub console: String,
    pub trx_id: String,
    pub block_num: i64,
    pub block_time: String,
    pub producer_block_id: Option<String>,
    //pub account_ram_deltas: Vec<AccountDelta>,
    //pub account_disk_deltas: Vec<AccountDelta>,
    pub error_code: Option<i64>,
    pub return_value_hex_data: Option<String>,
    pub inline_traces: Option<Vec<ActionTrace>>
}

#[derive(Serialize, Deserialize)]
pub struct TransactionReceiptHeader {
    pub status: String,
    pub cpu_usage_us: u64,
    pub net_usage_words: u64,
}

pub struct BinaryAbi {
    pub account_name: String,
    pub abi: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct TransactionTrace {
    pub id: String,
    pub block_num: i64,
    pub block_time: String,
    pub producer_block_id: Option<String>,
    pub receipt: Option<TransactionReceiptHeader>,
    pub elapsed: i64,
    pub net_usage: i64,
    pub scheduled: bool,
    pub action_traces: Vec<ActionTrace>,
    pub account_ram_delta: Option<AccountDelta>,
    pub except: Option<String>,
    pub error_code: Option<String>,
    pub bill_to_accounts: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
pub struct TransactResult {
    pub transaction_id: String,
    // pub processed: TransactionTrace,
}

#[derive(Serialize, Deserialize)]
pub struct ReadOnlyTransactResult {
    pub head_block_num: i64,
    pub head_block_id: String,
    pub last_irreversible_block_num: i64,
    pub last_irreversible_block_id: String,
    pub code_hash: String,
    pub pending_transactions: Vec<String>,
    // pub result: TransactionTrace
}

#[derive(Serialize, Deserialize)]
pub struct PushTransactionArgs {
    pub signatures: Vec<String>,
    pub compression: Option<bool>,
    pub serialized_transaction: Vec<u8>,
    pub serialized_context_free_data: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
pub struct GetRawAbiResult {
    pub account_name: String,
    pub code_hash: String,
    pub abi_hash: String,
    pub abi: String,
}

#[derive(Serialize, Deserialize)]
pub struct GetInfoResult {
    // pub server_version: String,
    pub chain_id: String,
    pub head_block_num: i64,
    pub last_irreversible_block_num: i64,
    pub last_irreversible_block_id: String,
    pub last_irreversible_block_time: Option<String>,
    // pub head_block_id: String,
    // pub head_block_time: String,
    // pub head_block_producer: String,
    // pub virtual_block_cpu_limit: i64,
    // pub virtual_block_net_limit: i64,
    // pub block_cpu_limit: i64,
    // pub block_net_limit: i64,
    // pub server_version_string: Option<String>,
    // pub fork_db_head_block_num: Option<i64>,
    // pub fork_db_head_block_id: Option<String>,
    // pub server_full_version_string: Option<String>,
    // pub first_block_num: Option<i64>,
}

#[derive(Serialize, Deserialize)]
pub struct AccountResourceInfo {
    pub used: i64,
    pub available: i64,
    pub max: i64,
    pub last_usage_update_time: Option<String>,
    pub current_used: Option<i64>,
}

#[derive(Serialize, Deserialize)]
pub struct KeyWeight {
    pub key: String,
    pub weight: u16,
}

impl DMCSerialize for KeyWeight {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_public_key(self.key.as_str())?;
        buf.push_u16(self.weight);
        Ok(())
    }
}

impl DMCDeserialize for KeyWeight {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {key: buf.get_public_key()?, weight: buf.get_u16()?})
    }
}

#[derive(Serialize, Deserialize)]
pub struct PermissionLevelWeight {
    pub permission: PermissionLevel,
    pub weight: u16,
}

impl DMCSerialize for PermissionLevelWeight {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        self.permission.dmc_serialize(buf)?;
        buf.push_u16(self.weight);
        Ok(())
    }
}

impl DMCDeserialize for PermissionLevelWeight {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            permission: PermissionLevel::dmc_deserialize(buf)?,
            weight: buf.get_u16()?
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct WaitWeight {
    pub wait_sec: u32,
    pub weight: u16,
}

impl DMCSerialize for WaitWeight {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_u32(self.wait_sec);
        buf.push_u16(self.weight);
        Ok(())
    }
}

impl DMCDeserialize for WaitWeight {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            wait_sec: buf.get_u32()?,
            weight: buf.get_u16()?
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct Authority {
    pub threshold: u32,
    pub keys: Vec<KeyWeight>,
    pub accounts: Vec<PermissionLevelWeight>,
    pub waits: Vec<WaitWeight>,
}

impl DMCSerialize for Authority {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_u32(self.threshold);
        buf.push_var_u32(self.keys.len() as u32);
        for key in self.keys.iter() {
            key.dmc_serialize(buf)?;
        }
        buf.push_var_u32(self.accounts.len() as u32);
        for account in self.accounts.iter() {
            account.dmc_serialize(buf)?;
        }
        buf.push_var_u32(self.waits.len() as u32);
        for wait in self.waits.iter() {
            wait.dmc_serialize(buf)?;
        }
        Ok(())
    }
}

impl DMCDeserialize for Authority {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        let threshold = buf.get_u32()?;
        let len = buf.get_var_u32()?;
        let mut keys = Vec::new();
        for _ in 0..len {
            keys.push(KeyWeight::dmc_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut accounts = Vec::new();
        for _ in 0..len {
            accounts.push(PermissionLevelWeight::dmc_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut waits = Vec::new();
        for _ in 0..len {
            waits.push(WaitWeight::dmc_deserialize(buf)?);
        }
        Ok(Self {
            threshold,
            keys,
            accounts,
            waits
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct Permission {
    pub perm_name: String,
    pub parent: String,
    pub required_auth: Authority,
}

#[derive(Serialize, Deserialize)]
pub struct ResourceOverview {
    pub owner: String,
    pub ram_bytes: i64,
    pub net_weight: String,
    pub cpu_weight: String,
}

#[derive(Serialize, Deserialize)]
pub struct ResourceDelegation {
    pub from: String,
    pub to: String,
    pub net_weight: String,
    pub cpu_weight: String,
}

#[derive(Serialize, Deserialize)]
pub struct RefundRequest {
    pub owner: String,
    pub request_time: String,
    pub net_amount: String,
    pub cpu_amount: String,
}

#[derive(Serialize, Deserialize)]
pub struct GetAccountResult<VOTER, REX> {
    pub account_name: String,
    pub head_block_num: i64,
    pub head_block_time: String,
    pub privileged: bool,
    pub last_code_update: String,
    pub created: String,
    pub core_liquid_balance: Option<String>,
    pub ram_quota: i64,
    pub net_weight: i64,
    pub cpu_weight: i64,
    pub net_limit: AccountResourceInfo,
    pub cpu_limit: AccountResourceInfo,
    pub ram_usage: i64,
    pub permissions: Vec<Permission>,
    pub total_resources: Option<ResourceOverview>,
    pub self_delegated_bandwidth: Option<ResourceDelegation>,
    pub refund_request: Option<RefundRequest>,
    pub voter_info: VOTER,
    pub rex_info: REX,
}

#[derive(Serialize, Deserialize)]
pub struct GetAbiResult {
    pub account_name: String,
    pub abi: Option<AbiDef>,
}

#[derive(Serialize, Deserialize)]
pub struct AccountResult {
    pub account_name: String,
    pub permission_name: String,
    pub authorizing_account: Option<PermissionLevel>,
    pub authorizing_key: Option<String>,
    pub weight: i64,
    pub threshold: i64,
}

#[derive(Serialize, Deserialize)]
pub struct GetAccountsByAuthorizersResult {
    pub accounts: Vec<AccountResult>
}

#[derive(Serialize, Deserialize)]
pub struct GetBlockInfoResult {
    pub timestamp: String,
    pub producer: String,
    pub confirmed: i64,
    pub previous: String,
    pub transaction_mroot: String,
    pub action_mroot: String,
    pub schedule_version: i64,
    pub producer_signature: String,
    pub id: String,
    pub block_num: i64,
    pub ref_block_num: i64,
    pub ref_block_prefix: i64,
}

#[derive(Serialize, Deserialize)]
pub struct SignedBlockHeader {
    pub timestamp: String,
    pub producer: String,
    pub confirmed: i64,
    pub previous: String,
    pub transaction_mroot: String,
    pub action_mroot: String,
    pub schedule_version: i64,
    // pub new_producers: Option<ProducerScheduleType>,
    // pub header_extensions: Vec<(i64, String)>,
    // pub producer_signature: String,
}

#[derive(Serialize, Deserialize)]
pub struct ScheduleInfo {
    schedule_lib_num: i64,
    schedule_hash: String,
    schedule: ProducerScheduleType,
}

#[derive(Serialize, Deserialize)]
pub struct ProtocolFeatureActivationSet {
    protocol_features: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct BlockSigningAuthority {
    threshold: i64,
    keys: Vec<KeyWeight>
}

#[derive(Serialize, Deserialize)]
pub struct ProducerAuthority {
    producer_name: String,
    authority: (i64, BlockSigningAuthority)
}

#[derive(Serialize, Deserialize)]
pub struct ProducerAuthoritySchedule {
    version: i64,
    producers: Vec<ProducerAuthority>,
}

#[derive(Serialize, Deserialize)]
pub struct IncrementalMerkle {
    _active_nodes: Vec<String>,
    _node_count: i64,
}

#[derive(Serialize, Deserialize)]
pub struct SecurityGroupInfo {
    version: i64,
    participants: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct StateExtension {
    security_group_info: SecurityGroupInfo
}

#[derive(Serialize, Deserialize)]
pub struct GetBlockHeaderStateResult {
    pub id: String,
    pub header: SignedBlockHeader,
    // pub pending_schedule: ScheduleInfo,
    // pub activated_protocol_features: ProtocolFeatureActivationSet,
    // pub additional_signatures: Vec<String>,
    pub block_num: i64,
    pub dpos_proposed_irreversible_blocknum: i64,
    pub dpos_irreversible_blocknum: i64,
    // pub active_schedule: ProducerAuthoritySchedule,
    // pub blockroot_merkle: IncrementalMerkle,
    // pub producer_to_last_produced: Vec<(String, i64)>,
    // pub producer_to_last_implied_irb: Vec<(String, i64)>,
    // pub confirm_count: Vec<i64>,
    // pub state_extension: Option<(i64, StateExtension)>,
}

#[derive(Serialize, Deserialize)]
pub struct GetBlockResult {
    pub timestamp: String,
    pub producer: String,
    pub confirmed: i64,
    pub previous: String,
    pub transaction_mroot: String,
    pub action_mroot: String,
    pub schedule_version: i64,
    // pub new_producers: Option<ProducerScheduleType>,
    pub producer_signature: String,
    pub id: String,
    pub block_num: i64,
    pub ref_block_prefix: i64,
}

#[derive(Serialize, Deserialize)]
pub struct GetCodeResult {
    pub account_name: String,
    pub code_hash: String,
    pub wast: String,
    pub wasm: String,
    pub abi: Option<AbiDef>,
}

#[derive(Serialize, Deserialize)]
pub struct GetTableRowsResult<T> {
    pub rows: Vec<T>,
    pub more: bool,
    pub next_key: String,
    pub next_key_bytes: String,
}

#[derive(Serialize, Deserialize)]
pub struct GetTableByScopeResultRow {
    pub code: String,
    pub scope: String,
    pub table: String,
    pub payer: String,
    pub count: String,
}

#[derive(Serialize, Deserialize)]
pub struct GetTableByScopeResult {
    pub rows: Vec<GetTableByScopeResultRow>,
    pub more: String,
}

#[derive(Serialize, Deserialize)]
pub struct GetAccountsByAuthorizers {
    pub accounts: Vec<PermissionLevel>,
    pub keys: Vec<String>
}

#[derive(Serialize, Deserialize)]
pub struct StakedInfo {
    pub quantity: String,
    pub contract: String,
}

#[derive(Serialize, Deserialize)]
pub struct MinerInfo {
    pub miner: String,
    pub current_rate: String,
    pub miner_rate: String,
    pub total_weight: String,
    pub total_staked: StakedInfo,
}

#[derive(Serialize, Deserialize)]
pub struct ProducerKey {
    pub producer_name: String,
    pub block_signing_key: String,
}

#[derive(Serialize, Deserialize)]
pub struct ProducerScheduleType {
    pub version: i64,
    pub producers: Vec<ProducerKey>,
}

#[derive(Serialize, Deserialize)]
pub struct BlockHeader {
    pub timestamp: String,
    pub producer: String,
    pub confirmed: i64,
    pub previous: String,
    pub transaction_mroot: String,
    pub action_mroot: String,
    pub schedule_version: i64,
    pub new_producers: Option<ProducerScheduleType>,
    pub header_extensions: Vec<(i64, String)>,
}

#[derive(Serialize, Deserialize)]
pub struct GetTableRowsReq<'a> {
    pub json: bool,
    pub code: &'a str,
    pub table: &'a str,
    pub scope: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_position: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encode_type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lower_bound: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upper_bound: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reverse: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub show_payer: Option<bool>
}

#[derive(Serialize, Deserialize)]
pub struct GetKVTableRowsReq<'a> {
    pub json: bool,
    pub code: &'a str,
    pub table: &'a str,
    pub index_name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encode_type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_value: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lower_bound: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upper_bound: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reverse: Option<bool>
}

#[derive(Serialize, Deserialize)]
pub struct GetTableByScopeReq<'a> {
    pub json: bool,
    pub code: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lower_bound: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upper_bound: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reverse: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub show_payer: Option<bool>
}

pub struct DMCRpc {
    server: String,
}

impl DMCRpc {
    pub fn new(server: &str) -> Self {
        let server = if server.ends_with("/") {
            server[0..server.len() - 1].to_string()
        } else {
            server.to_string()
        };
        Self {
            server
        }
    }

    pub async fn create_account(&self, account: &str, public_key: &str, create_key: &str) -> BuckyResult<()> {
        let url = format!("{}/1.0/app/token/create", self.server.as_str());
        let timestamp = js_time_to_bucky_time(bucky_time_now()) / 1000;
        let hash = md5::compute(format!("{}{}{}{}", account, public_key, create_key, timestamp));
        let hash = format!("{:x}", hash);
        let params = CreateAccountParam {
            account,
            hash: hash.as_str(),
            pubkey: public_key,
            t: timestamp
        };

        let str = serde_json::to_string(&params).map_err(|e| {
            cyfs_err!(BuckyErrorCode::CryptoError, "encode to json failed {}", e)
        })?;
        let resp = http_post_request(url.as_str(), str.as_bytes(), Some("application/json")).await?;

        let resp_str = String::from_utf8_lossy(resp.as_slice()).to_string();
        log::info!("create account resp {}", resp_str.as_str());

        let result: CreateAccountResp = serde_json::from_slice(resp.as_slice()).map_err(|e| {
            cyfs_err!(BuckyErrorCode::CryptoError, "parse {} err {}", String::from_utf8_lossy(resp.as_slice()).to_string(), e)
        })?;

        if result.code.is_some() {
            Err(cyfs_err!(BuckyErrorCode::Failed, "code:{} msg:{}", result.code.as_ref().unwrap(), result.error.as_ref().unwrap()))
        } else {
            Ok(())
        }
    }

    pub async fn abi_bin_to_json(&self, code: &str, action: &str, binargs: &str) -> BuckyResult<JsonValue> {
        let url = format!("{}/v1/chain/abi_bin_to_json", self.server);
        let data = object! {
            code: code,
            action: action,
            binargs: binargs
        };
        for _ in 0..3 {
            return match http_post_json(url.as_str(), data.clone()).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "abi_bin_to_json failed"))

    }

    pub async fn get_abi(&self, account_name: &str) -> BuckyResult<GetAbiResult> {
        let url = format!("{}/v1/chain/get_abi", self.server.as_str());
        let data = object! {
            account_name: account_name
        };

        for _ in 0..3 {
            return match http_post_json2(url.as_str(), data.clone()).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "get_abi failed"))
    }

    pub async fn get_account<VOTER: for <'de> Deserialize<'de>, REX: for <'de> Deserialize<'de>>(&self, account_name: &str) -> BuckyResult<GetAccountResult<VOTER, REX>> {
        let url = format!("{}/v1/chain/get_account", self.server.as_str());
        let data = object! {
            account_name: account_name
        };
        for _ in 0..3 {
            return match http_post_json2(url.as_str(), data.clone()).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "get_account failed"))
    }

    pub async fn get_accounts_by_authorizers(&self, accounts: Vec<PermissionLevel>, keys: Vec<String>) -> BuckyResult<GetAccountsByAuthorizersResult> {
        let url = format!("{}/v1/chain/get_accounts_by_authorizers", self.server.as_str());
        let req = GetAccountsByAuthorizers {
            accounts,
            keys
        };

        for _ in 0..3 {
            return match http_post_request3(url.as_str(), serde_json::to_string(&req).unwrap().as_bytes(), Some("application/json")).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "get_accounts_by_authorizers failed"))
    }

    pub async fn get_block_header_state(&self, block_num_or_id: String) -> BuckyResult<GetBlockHeaderStateResult> {
        let url = format!("{}/v1/chain/get_block_header_state", self.server.as_str());
        let data = object! {
            block_num_or_id: block_num_or_id
        };

        for _ in 0..3 {
            return match http_post_json2(url.as_str(), data.clone()).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "get_block_header_state failed"))
    }

    pub async fn get_block_info(&self, block_num: i64) -> BuckyResult<GetBlockInfoResult> {
        let url = format!("{}/v1/chain/get_block_info", self.server.as_str());
        let data = object! {
            block_num: block_num
        };
        for _ in 0..3 {
            return match http_post_json2(url.as_str(), data.clone()).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "get_block_info failed"))
    }

    pub async fn get_block(&self, block_num_or_id: String) -> BuckyResult<GetBlockResult> {
        let url = format!("{}/v1/chain/get_block", self.server.as_str());
        let data = object! {
            block_num_or_id: block_num_or_id
        };

        for _ in 0..3 {
            return match http_post_json2(url.as_str(), data.clone()).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "get_block failed"))
    }

    pub async fn get_code(&self, account_name: &str) -> BuckyResult<GetCodeResult> {
        let url = format!("{}/v1/chain/get_code", self.server.as_str());
        let data = object! {
            account_name: account_name,
            code_as_wasm: true,
        };

        for _ in 0..3 {
            return match http_post_json2(url.as_str(), data.clone()).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "get_code failed"))
    }

    pub async fn get_currency_balance(&self, code: &str, account: &str, symbol: Option<String>) -> BuckyResult<Vec<String>> {
        let url = format!("{}/v1/chain/get_currency_balance", self.server.as_str());
        let data = object! {
            code: code,
            account: account,
            symbol: symbol,
        };

        for _ in 0..3 {
            return match http_post_json2(url.as_str(), data.clone()).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "get_currency_balance failed"))
    }

    pub async fn get_table_rows<'a, T: for <'de> Deserialize<'de>>(&self,
                                                                   req: &GetTableRowsReq<'a>) -> BuckyResult<GetTableRowsResult<T>> {
        let url = format!("{}/v1/chain/get_table_rows", self.server.as_str());
        let data = serde_json::to_string(req).map_err(|e| {
            cyfs_err!(BuckyErrorCode::Failed, "encode json err {}", e)
        })?;

        for _ in 0..3 {
            return match http_post_request3(url.as_str(), data.as_bytes(), Some("application/json")).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "get_table_rows failed"))
    }

    pub async fn get_kv_table_rows<'a, T: for <'de> Deserialize<'de>>(&self,
                                                                      req: &GetKVTableRowsReq<'a>) -> BuckyResult<GetTableRowsResult<T>> {
        let url = format!("{}/v1/chain/get_kv_table_rows", self.server.as_str());
        let data = serde_json::to_string(req).map_err(|e| {
            cyfs_err!(BuckyErrorCode::Failed, "encode json err {}", e)
        })?;

        for _ in 0..3 {
            return match http_post_request3(url.as_str(), data.as_bytes(), Some("application/json")).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "get_kv_table_rows failed"))
    }

    pub async fn get_table_by_scope<'a>(&self,
                                        req: &GetTableByScopeReq<'a>) -> BuckyResult<GetTableByScopeResult> {
        let url = format!("{}/v1/chain/get_table_by_scope", self.server.as_str());
        let data = serde_json::to_string(req).map_err(|e| {
            cyfs_err!(BuckyErrorCode::Failed, "encode json err {}", e)
        })?;

        for _ in 0..3 {
            return match http_post_request3(url.as_str(), data.as_bytes(), Some("application/json")).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "get_table_by_scope failed"))
    }

    pub async fn get_raw_abi(&self, account_name: &str) -> BuckyResult<GetRawAbiResult> {
        let url = format!("{}/v1/chain/get_raw_abi", self.server);
        let data = object! {
            account_name: account_name
        };

        for _ in 0..3 {
            return match http_post_json2(url.as_str(), data.clone()).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "get_raw_abi failed"))
    }

    pub async fn get_bin_abi(&self, account_name: &str) -> BuckyResult<BinaryAbi> {
        let raw_abi = self.get_raw_abi(account_name).await?;
        let mut len = raw_abi.abi.len();
        if len & 3 == 1 && raw_abi.abi.ends_with('=') {
            len -= 1;
        }

        let abi = base64::decode(&raw_abi.abi[..len]).map_err(|e| {
            cyfs_err!(BuckyErrorCode::Failed, "decode {} failed. {}", raw_abi.abi.as_str(), e)
        })?;
        Ok(BinaryAbi {
            account_name: account_name.to_string(),
            abi
        })
    }

    pub async fn get_info(&self) -> BuckyResult<GetInfoResult> {
        let url = format!("{}/v1/chain/get_info", self.server);
        let data = object! {
        };

        for _ in 0..3 {
            return match http_post_json2(url.as_str(), data.clone()).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "get_info failed"))
    }

    pub async fn push_ro_transaction(
        &self,
        signatures: Vec<String>,
        compression: bool,
        serialized_transaction: &[u8],
        return_failure_traces: bool
    ) -> BuckyResult<ReadOnlyTransactResult> {
        let url = format!("{}/v1/chain/push_ro_transaction", self.server);

        let data = object! {
            transaction: {
                signatures: signatures,
                compression: if compression {1} else {0},
                packed_context_free_data: "".to_string(),
                packed_trx: hex::encode(serialized_transaction).to_uppercase(),
            },
            return_failure_traces: return_failure_traces
        };

        for _ in 0..3 {
            return match http_post_json2(url.as_str(), data.clone()).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "push_ro_transaction failed"))
    }

    pub async fn push_transactions(&self, transactions: Vec<PushTransactionArgs>) -> BuckyResult<Vec<TransactResult>> {
        let url = format!("{}/v1/chain/push_transactions", self.server);

        let mut data = JsonValue::new_array();
        for item in transactions.into_iter() {
            let packed_context_free_data = if item.serialized_context_free_data.is_some() {
                hex::encode(item.serialized_context_free_data.as_ref().unwrap()).to_uppercase()
            } else {
                "".to_string()
            };

            let compression = if item.compression.is_some() && item.compression.unwrap() {
                1
            } else {
                0
            };
            data.push(object! {
                signatures: item.signatures,
                compression: compression,
                packed_context_free_data: packed_context_free_data,
                packed_trx: hex::encode(item.serialized_transaction.as_slice()).to_uppercase()
            }).map_err(|e| {
                cyfs_err!(BuckyErrorCode::Failed, "{}", e)
            })?;
        }
        for _ in 0..3 {
            return match http_post_json2(url.as_str(), data.clone()).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "push_transactions failed"))
    }

    pub async fn send_transaction(&self, transaction: PushTransactionArgs) -> BuckyResult<TransactResult> {
        let url = format!("{}/v1/chain/send_transaction", self.server);

        let packed_context_free_data = if transaction.serialized_context_free_data.is_some() {
            hex::encode(transaction.serialized_context_free_data.as_ref().unwrap()).to_uppercase()
        } else {
            "".to_string()
        };

        let compression = if transaction.compression.is_some() && transaction.compression.unwrap() {
            1
        } else {
            0
        };

        let data = object! {
                signatures: transaction.signatures,
                compression: compression,
                packed_context_free_data: packed_context_free_data,
                packed_trx: hex::encode(transaction.serialized_transaction).to_uppercase()
        };

        for _ in 0..3 {
            return match http_post_json2(url.as_str(), data.clone()).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "send_transaction failed"))
    }

    pub async fn push_transaction(&self, transaction: PushTransactionArgs) -> BuckyResult<TransactResult> {
        let url = format!("{}/v1/chain/push_transaction", self.server);

        let packed_context_free_data = if transaction.serialized_context_free_data.is_some() {
            hex::encode(transaction.serialized_context_free_data.as_ref().unwrap())
        } else {
            "".to_string()
        };

        let compression = if transaction.compression.is_some() && transaction.compression.unwrap() {
            1
        } else {
            0
        };

        let packed_trx = hex::encode(transaction.serialized_transaction.as_slice());

        let data = object! {
                signatures: transaction.signatures,
                compression: compression,
                packed_context_free_data: packed_context_free_data,
                packed_trx: packed_trx
        };

        for _ in 0..3 {
            return match http_post_json2(url.as_str(), data.clone()).await {
                Ok(resp) => Ok(resp),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "push_transaction failed"))
    }
}
