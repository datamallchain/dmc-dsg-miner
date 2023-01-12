use std::ops::{Deref};
use std::sync::Arc;
use cyfs_base::{BuckyErrorCode, BuckyResult, HashValue, js_time_to_bucky_time};
use serde::{Serialize, Deserialize};
use crate::*;

struct UpdateAuth {
    pub account: Name,
    pub permission: Name,
    pub parent: Name,
    pub auth: Authority
}

impl DMCSerialize for UpdateAuth {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_name(self.account.as_str())?;
        buf.push_name(self.permission.as_str())?;
        buf.push_name(self.parent.as_str())?;
        self.auth.dmc_serialize(buf)?;
        Ok(())
    }
}

impl DMCDeserialize for UpdateAuth {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            account: Name::dmc_deserialize(buf)?,
            permission: Name::dmc_deserialize(buf)?,
            parent: Name::dmc_deserialize(buf)?,
            auth: Authority::dmc_deserialize(buf)?
        })
    }
}

struct DeleteAuth {
    pub account: Name,
    pub permission: Name
}

impl DMCSerialize for DeleteAuth {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        Name::dmc_serialize(&self.account, buf)?;
        Name::dmc_serialize(&self.permission, buf)?;
        Ok(())
    }
}

impl DMCDeserialize for DeleteAuth {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            account: Name::dmc_deserialize(buf)?,
            permission: Name::dmc_deserialize(buf)?
        })
    }
}

type Asset = String;

struct ExtendedAsset {
    pub quantity: Asset,
    pub contract: Name,
}

impl DMCSerialize for ExtendedAsset {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_assert(self.quantity.as_str())?;
        buf.push_name(self.contract.as_str())?;
        Ok(())
    }
}

impl DMCDeserialize for ExtendedAsset {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            quantity: buf.get_asset()?,
            contract: buf.get_name()?
        })
    }
}

struct Increase {
    owner: Name,
    asset: ExtendedAsset,
    miner: Name,
}

impl DMCSerialize for Increase {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        Name::dmc_serialize(&self.owner, buf)?;
        self.asset.dmc_serialize(buf)?;
        Name::dmc_serialize(&self.miner, buf)?;
        Ok(())
    }
}

impl DMCDeserialize for Increase {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            owner: Name::dmc_deserialize(buf)?,
            asset: ExtendedAsset::dmc_deserialize(buf)?,
            miner: Name::dmc_deserialize(buf)?
        })
    }
}

struct Mint {
    owner: Name,
    asset: ExtendedAsset,
}

impl DMCSerialize for Mint {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_name(self.owner.as_str())?;
        self.asset.dmc_serialize(buf)?;
        Ok(())
    }
}

impl DMCDeserialize for Mint {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            owner: buf.get_name()?,
            asset: ExtendedAsset::dmc_deserialize(buf)?
        })
    }
}

struct Bill {
    owner: Name,
    asset: ExtendedAsset,
    price: f64,
    memo: String,
}

impl DMCSerialize for Bill {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        Name::dmc_serialize(&self.owner, buf)?;
        self.asset.dmc_serialize(buf)?;
        buf.push_f64(self.price);
        buf.push_string(self.memo.as_str());
        Ok(())
    }
}

impl DMCDeserialize for Bill {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            owner: Name::dmc_deserialize(buf)?,
            asset: ExtendedAsset::dmc_deserialize(buf)?,
            price: buf.get_f64()?,
            memo: buf.get_string()?
        })
    }
}

struct AddMerkle {
    sender: Name,
    order_id: u64,
    merkle_root: HashValue,
    data_block_count: u64,
}

impl DMCSerialize for AddMerkle {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        Name::dmc_serialize(&self.sender, buf)?;
        buf.push_u64(self.order_id);
        buf.push_array(self.merkle_root.as_slice());
        buf.push_u64(self.data_block_count);
        Ok(())
    }
}

impl DMCDeserialize for AddMerkle {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            sender: Name::dmc_deserialize(buf)?,
            order_id: buf.get_u64()?,
            merkle_root: HashValue::from(buf.get_array(32)?),
            data_block_count: buf.get_u64()?
        })
    }
}

struct ChallengeReq {
    sender: Name,
    order_id: u64,
    data_id: u64,
    hash_data: HashValue,
    nonce: String,
}

impl DMCSerialize for ChallengeReq {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        Name::dmc_serialize(&self.sender, buf)?;
        buf.push_u64(self.order_id);
        buf.push_u64(self.data_id);
        buf.push_array(self.hash_data.as_slice());
        buf.push_string(self.nonce.as_str());
        Ok(())
    }
}

impl DMCDeserialize for ChallengeReq {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            sender: Name::dmc_deserialize(buf)?,
            order_id: buf.get_u64()?,
            data_id: buf.get_u64()?,
            hash_data: HashValue::from(buf.get_array(32)?),
            nonce: buf.get_string()?
        })
    }
}

struct Arbitration {
    sender: Name,
    order_id: u64,
    data: Vec<u8>,
    cut_merkle: Vec<HashValue>,
}

impl DMCSerialize for Arbitration {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        Name::dmc_serialize(&self.sender, buf)?;
        buf.push_u64(self.order_id);
        buf.push_bytes(self.data.as_slice());
        buf.push_var_u32(self.cut_merkle.len() as u32);
        for hash in self.cut_merkle.iter() {
            buf.push_array(hash.as_slice());
        }
        Ok(())
    }
}

impl DMCDeserialize for Arbitration {
    fn dmc_deserialize(_buf: &mut SerialBuffer) -> BuckyResult<Self> {
        todo!()
    }
}
struct ChallengeResp {
    sender: Name,
    order_id: u64,
    reply_hash: HashValue,
}

impl DMCSerialize for ChallengeResp {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        Name::dmc_serialize(&self.sender, buf)?;
        buf.push_u64(self.order_id);
        buf.push_array(self.reply_hash.as_slice());
        Ok(())
    }
}

impl DMCDeserialize for ChallengeResp {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            sender: Name::dmc_deserialize(buf)?,
            order_id: buf.get_u64()?,
            reply_hash: HashValue::from(buf.get_array(32)?)
        })
    }
}

struct LinkAuth {
    account: Name,
    code: Name,
    ty: Name,
    requirement: Name,
}

impl DMCSerialize for LinkAuth {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        Name::dmc_serialize(&self.account, buf)?;
        Name::dmc_serialize(&self.code, buf)?;
        Name::dmc_serialize(&self.ty, buf)?;
        Name::dmc_serialize(&self.requirement, buf)?;
        Ok(())
    }
}

impl DMCDeserialize for LinkAuth {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            account: Name::dmc_deserialize(buf)?,
            code: Name::dmc_deserialize(buf)?,
            ty: Name::dmc_deserialize(buf)?,
            requirement: Name::dmc_deserialize(buf)?
        })
    }
}

struct UnlinkAuth {
    account: Name,
    code: Name,
    ty: Name,
}

impl DMCSerialize for UnlinkAuth {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        Name::dmc_serialize(&self.account, buf)?;
        Name::dmc_serialize(&self.code, buf)?;
        Name::dmc_serialize(&self.ty, buf)?;
        Ok(())
    }
}

impl DMCDeserialize for UnlinkAuth {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            account: Name::dmc_deserialize(buf)?,
            code: Name::dmc_deserialize(buf)?,
            ty: Name::dmc_deserialize(buf)?,
        })
    }
}

struct CyfsBind {
    owner: Name,
    address: String,
}

impl DMCSerialize for CyfsBind {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        Name::dmc_serialize(&self.owner, buf)?;
        buf.push_string(self.address.as_str());
        Ok(())
    }
}

impl DMCDeserialize for CyfsBind {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            owner: Name::dmc_deserialize(buf)?,
            address: buf.get_string()?
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct CyfsInfo {
    pub addr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub v: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mid: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Pledge {
    pub quantity: String,
    pub contract: String,
}

impl Pledge {
    pub fn get_quantity(&self) -> String {
        let list: Vec<_> = self.quantity.split(" ").collect();
        list[0].to_string()
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DMCOrder {
    pub order_id: String,
    pub user: String,
    pub miner: String,
    pub bill_id: String,
    pub user_pledge: Pledge,
    pub miner_pledge: Pledge,
    pub price: Pledge,
    pub settlement_pledge: Pledge,
    pub lock_pledge: Pledge,
    pub state: u8,
    pub latest_settlement_date: String,
    pub deliver_start_date: String,
}

#[repr(u8)]
pub enum DMCOrderState {
    OrderStateWaiting = 0, //0: 订单未共识，等待中
    OrderStateDeliver = 1, //1: 订单状态交付中
    OrderStatePreEnd = 2, //2: 没有⾜够的预存⾦，订单即将结束
    OrderStatePreCont = 3, //3: 有⾜够的预存⾦，订单下个周期依然处于交付中
    OrderStateEnd = 4 // 4: 订单已经结束
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DMCChallengeInfo {
    pub order_id: String,
    pub pre_merkle_root: String,
    pub pre_data_block_count: u64,
    pub merkle_root: String,
    pub data_block_count: u64,
    pub merkle_submitter: String,
    pub data_id: u64,
    pub hash_data: String,
    pub challenge_times: u64,
    pub nonce: String,
    pub state: u32,
    pub challenge_date: String,
}

#[repr(u32)]
pub enum DMCChallengeState {
    ChallengePrepare = 0, //未提交默克尔树根，等待阶段
    ChallengeConsistent = 1,//提交了一致的默克尔树根
    ChallengeCancel = 2, //挑战取消，同时订单应该同时被取消
    ChallengeRequest = 3, // 用户发起存储挑战
    ChallengeAnswer = 4, // 矿工成功响应挑战
    ChallengeArbitrationMinerPay = 5, //矿工仲裁成功，矿工为过错方
    ChallengeArbitrationUserPay = 6, //矿工仲裁成功，用户为过错方
    ChallengeTimeout = 7, //挑战超时，矿工赔付
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PstStat {
    pub owner: String,
    pub amount: Pledge,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TrackerDMCOrder {
    pub order_id: String,
    pub user: String,
    pub miner: String,
    pub bill_id: String,
    pub user_pledge_amount: String,
    pub user_pledge_symbol: String,
    pub miner_pledge_amount: String,
    pub miner_pledge_symbol: String,
    pub price_amount: String,
    pub price_symbol: String,
    pub settlement_pledge_amount: String,
    pub settlement_pledge_symbol: String,
    pub lock_amount: String,
    pub lock_symbol: String,
    pub state: u8,
    pub deliver_start_date: String,
    pub latest_settlement_date: String,
}

impl TrackerDMCOrder {
    pub fn get_space(&self) -> BuckyResult<u64> {
        let pst: u64 = self.miner_pledge_amount.parse().map_err(|e| {
            cyfs_err!(BuckyErrorCode::Failed, "parse {} failed {}", self.miner_pledge_amount.as_str(), e)
        })?;
        Ok(pst * 1024 * 1024 * 1024)
    }

    pub fn get_create_time(&self) -> BuckyResult<u64> {
        let time = js_time_to_bucky_time(date_to_time_point(self.latest_settlement_date.as_str())? as u64 * 1000);
        Ok(time)
    }
}

impl DMCOrder {
    pub fn get_space(&self) -> BuckyResult<u64> {
        let quantity = self.miner_pledge.quantity.trim_end_matches("PST").trim();
        let pst: u64 = quantity.parse().map_err(|e| {
            cyfs_err!(BuckyErrorCode::Failed, "parse {} failed {}", quantity, e)
        })?;
        Ok(pst * 1024 * 1024 * 1024)
    }

    pub fn get_create_time(&self) -> BuckyResult<u64> {
        let time = js_time_to_bucky_time(date_to_time_point(self.latest_settlement_date.as_str())? as u64 * 1000);
        Ok(time)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PstTransInfo {
    pub total: String,
    pub count: u64,
    pub avg: String
}

impl PstTransInfo {
    pub fn total(&self) -> f64 {
        self.total.parse().unwrap_or(0f64)
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn avg(&self) -> f64 {
        self.avg.parse().unwrap_or(0f64)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CyfsAccount {
    pub account: String,
    pub address: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StakeInfo {
    pub miner: String,
    pub current_rate: String,
    pub miner_rate: String,
    pub total_weight: String,
    pub total_staked: Pledge
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BillRecord {
    pub primary: u64,
    pub bill_id: String,
    pub owner: String,
    pub matched: Pledge,
    pub unmatched: Pledge,
    pub price: f64,
    pub created_at: String,
    pub updated_at: String,
}

fn char_to_symbol(c: char) -> u8 {
    if c >= 'a' && c <= 'z' {
        (c as u8 - 'a' as u8 + 6) as u8
    } else if c >= '1' && c <= '5' {
        (c as u8 - '1' as u8 + 1) as u8
    } else {
        0
    }
}


fn string_to_name(str_name: &str) -> String {
    let mut value: u128 = 0;
    for (i, c1) in str_name.chars().enumerate() {
        let mut c = char_to_symbol(c1);
        if i < 12 {
            c &= 0x1f;
            value += c as u128 * (2u128.pow(64 - 5 * (i as u32 + 1)));
        } else {
            c &= 0x0f;
            value += c as u128;
        }
    }
    format!("{}", value)
}

#[async_trait::async_trait]
pub trait DMCTxSender: 'static + Send + Sync {
    async fn update_auth(&self, permission: String, parent: String, auth: Authority) -> BuckyResult<TransResult>;
    async fn delete_auth(&self, permission: String, parent_permission: String) -> BuckyResult<TransResult>;
    async fn link_auth(&self, code: String, ty: String, permission: String) -> BuckyResult<TransResult>;
    async fn unlink_auth(&self, code: String, ty: String, permission: String) -> BuckyResult<TransResult>;
    async fn stake(&self, amount: &str) -> BuckyResult<TransResult>;
    async fn bill(&self, asset: String, price: f64, memo: String) -> BuckyResult<TransResult>;
    async fn mint(&self, amount: &str) -> BuckyResult<TransResult>;
    async fn add_merkle(&self, order_id: &str, merkle_root: HashValue, data_block_count: u64) -> BuckyResult<TransResult>;
    async fn challenge(
        &self,
        order_id: &str,
        data_id: u64,
        hash_data: HashValue,
        nonce: String
    ) -> BuckyResult<TransResult>;
    async fn add_challenge_resp(
        &self,
        order_id: &str,
        reply_hash: HashValue
    ) -> BuckyResult<TransResult>;
    async fn arbitration(
        &self,
        order_id: &str,
        data: Vec<u8>,
        cut_merkle: Vec<HashValue>
    ) -> BuckyResult<TransResult>;
    async fn report_cyfs_info(&self, info: &CyfsInfo) -> BuckyResult<TransResult>;
}

pub struct LocalDMCTxSender<T: 'static + SignatureProvider> {
    api: DMCApi<T>,
    account_name: String,
    sign_keys: Vec<String>,
}

impl<T: 'static + SignatureProvider> LocalDMCTxSender<T> {
    pub fn new(account_name: &str, server: &str, sign_provider: T) -> Self {
        let rpc = DMCRpc::new(server);
        let sign_keys = sign_provider.get_available_keys().clone();
        let api = DMCApi::new(Arc::new(rpc), sign_provider);
        Self {
            api,
            account_name: account_name.to_string(),
            sign_keys
        }
    }

    async fn send_transaction(&self, trans: Transaction) -> BuckyResult<TransResult> {
        let ret = self.api.transact(trans, TransactionConfig {
            broadcast: Some(true),
            sign: Some(true),
            read_only_trx: None,
            return_failure_traces: None,
            required_keys: Some(self.sign_keys.clone()),
            compression: Some(true),
            blocks_behind: Some(3),
            use_last_irreversible: None,
            expire_seconds: Some(30)
        }).await?;
        Ok(ret)
    }
}

#[async_trait::async_trait]
impl<T: 'static + SignatureProvider> DMCTxSender for LocalDMCTxSender<T>  {
    async fn update_auth(&self, permission: String, parent: String, auth: Authority) -> BuckyResult<TransResult> {
        let params = UpdateAuth {
            account: self.account_name.clone(),
            permission,
            parent: parent.clone(),
            auth
        };
        let trans = TransactionBuilder::new().add_action(
            "dmc",
            "updateauth", vec![Authorization {
                actor: self.account_name.clone(),
                permission: parent }], params)?.build();
        self.send_transaction(trans).await
    }

    async fn delete_auth(&self, permission: String, parent_permission: String) -> BuckyResult<TransResult> {
        let params = DeleteAuth {
            account: self.account_name.clone(),
            permission
        };
        let trans = TransactionBuilder::new().add_action(
            "dmc",
            "deleteauth", vec![Authorization {
                actor: self.account_name.clone(),
                permission: parent_permission }], params)?.build();
        self.send_transaction(trans).await
    }

    async fn link_auth(&self, code: String, ty: String, permission: String) -> BuckyResult<TransResult> {
        let params = LinkAuth {
            account: self.account_name.clone(),
            code,
            ty,
            requirement: "light".to_string()
        };
        let trans = TransactionBuilder::new().add_action(
            "dmc",
            "linkauth", vec![Authorization {
                actor: self.account_name.clone(),
                permission }], params)?.build();
        self.send_transaction(trans).await
    }

    async fn unlink_auth(&self, code: String, ty: String, permission: String) -> BuckyResult<TransResult> {
        let params = UnlinkAuth {
            account: self.account_name.clone(),
            code,
            ty
        };

        let trans = TransactionBuilder::new().add_action(
            "dmc",
            "unlinkauth", vec![Authorization {
                actor: self.account_name.clone(),
                permission}], params)?.build();
        self.send_transaction(trans).await
    }

    async fn stake(&self, amount: &str) -> BuckyResult<TransResult> {
        let params = Increase {
            owner: self.account_name.clone(),
            asset: ExtendedAsset {
                quantity: format!("{:.04} DMC", amount.parse::<f64>().map_err(|e| {cyfs_err!(BuckyErrorCode::InvalidData, "parse {} err {}", amount, e)})?),
                contract: "datamall".to_string()
            },
            miner: self.account_name.clone()
        };

        let trans = TransactionBuilder::new().add_action(
            "dmc.token",
            "increase",
            vec![Authorization { actor: self.account_name.clone(), permission: "active".to_string() }],
            params)?.build();

        let ret = self.api.transact(trans, TransactionConfig {
            broadcast: Some(true),
            sign: Some(true),
            read_only_trx: None,
            return_failure_traces: None,
            required_keys: Some(self.sign_keys.clone()),
            compression: Some(true),
            blocks_behind: Some(3),
            use_last_irreversible: None,
            expire_seconds: Some(30)
        }).await?;
        Ok(ret)
    }

    async fn bill(&self, asset: String, price: f64, memo: String) -> BuckyResult<TransResult> {
        let params = Bill {
            owner: self.account_name.clone(),
            asset: ExtendedAsset {
                quantity: format!("{} PST", asset),
                contract: "datamall".to_string()
            },
            price,
            memo
        };

        let trans = TransactionBuilder::new().add_action(
            "dmc.token",
            "bill",
            vec![Authorization { actor: self.account_name.clone(), permission: "active".to_string()}],
            params
        )?.build();

        let ret = self.api.transact(trans, TransactionConfig {
            broadcast: Some(true),
            sign: Some(true),
            read_only_trx: None,
            return_failure_traces: None,
            required_keys: Some(self.sign_keys.clone()),
            compression: Some(true),
            blocks_behind: Some(3),
            use_last_irreversible: None,
            expire_seconds: Some(30)
        }).await?;
        Ok(ret)
    }

    async fn mint(&self, amount: &str) -> BuckyResult<TransResult> {
        let params = Mint {
            owner: self.account_name.clone(),
            asset: ExtendedAsset {
                quantity: format!("{} PST", amount),
                contract: "datamall".to_string()
            }
        };

        let trans = TransactionBuilder::new().add_action(
            "dmc.token",
            "mint",
            vec![Authorization {
                actor: self.account_name.clone(),
                permission: "active".to_string()
            }],
            params
        )?.build();

        let ret = self.api.transact(trans, TransactionConfig {
            broadcast: Some(true),
            sign: Some(true),
            read_only_trx: None,
            return_failure_traces: None,
            required_keys: Some(self.sign_keys.clone()),
            compression: Some(true),
            blocks_behind: Some(3),
            use_last_irreversible: None,
            expire_seconds: Some(30)
        }).await?;
        Ok(ret)
    }

    async fn add_merkle(&self, order_id: &str, merkle_root: HashValue, data_block_count: u64) -> BuckyResult<TransResult> {
        log::info!("add_merkle order_id {} merkle_root {} data_block_count {}", order_id, merkle_root.to_string(), data_block_count);
        let params = AddMerkle {
            sender: self.account_name.clone(),
            order_id: order_id.parse().map_err(|e| {
                cyfs_err!(BuckyErrorCode::InvalidData, "parse order {} err{}", order_id, e)
            })?,
            merkle_root,
            data_block_count
        };

        let trans = TransactionBuilder::new().add_action(
            "dmc.token",
            "addmerkle", vec![Authorization {
                actor: self.account_name.clone(),
                permission: "light".to_string() }], params)?.build();
        self.send_transaction(trans).await
    }

    async fn challenge(
        &self,
        order_id: &str,
        data_id: u64,
        hash_data: HashValue,
        nonce: String
    ) -> BuckyResult<TransResult> {
        log::info!("challenge order_id {} data_id {} hash_data {} nonce {}", order_id, data_id, hash_data.to_string(), nonce);
        let params = ChallengeReq {
            sender: self.account_name.clone(),
            order_id: order_id.parse().map_err(|e| {
                cyfs_err!(BuckyErrorCode::InvalidData, "parse order {} err{}", order_id, e)
            })?,
            data_id,
            hash_data,
            nonce
        };
        let trans = TransactionBuilder::new().add_action(
            "dmc.token",
            "reqchallenge", vec![Authorization {
                actor: self.account_name.clone(),
                permission: "light".to_string() }], params)?.build();
        self.send_transaction(trans).await
    }

    async fn add_challenge_resp(
        &self,
        order_id: &str,
        reply_hash: HashValue
    ) -> BuckyResult<TransResult> {
        log::info!("add_challenge_resp order_id {} reply_hash {}", order_id, reply_hash.to_string());
        let params = ChallengeResp {
            sender: self.account_name.clone(),
            order_id: order_id.parse().map_err(|e| {
                cyfs_err!(BuckyErrorCode::InvalidData, "parse order {} err{}", order_id, e)
            })?,
            reply_hash
        };
        let trans = TransactionBuilder::new().add_action(
            "dmc.token",
            "anschallenge", vec![Authorization {
                actor: self.account_name.clone(),
                permission: "light".to_string() }], params)?.build();
        self.send_transaction(trans).await
    }

    async fn arbitration(
        &self,
        order_id: &str,
        data: Vec<u8>,
        cut_merkle: Vec<HashValue>
    ) -> BuckyResult<TransResult> {
        log::info!("arbitration order_id {}", order_id);
        let params = Arbitration {
            sender: self.account_name.clone(),
            order_id: order_id.parse().map_err(|e| {
                cyfs_err!(BuckyErrorCode::InvalidData, "parse order {} err{}", order_id, e)
            })?,
            data,
            cut_merkle
        };
        let trans = TransactionBuilder::new().add_action(
            "dmc.token",
            "arbitration", vec![Authorization {
                actor: self.account_name.clone(),
                permission: "light".to_string() }], params)?.build();
        self.send_transaction(trans).await
    }

    async fn report_cyfs_info(&self, info: &CyfsInfo) -> BuckyResult<TransResult> {
        let params = CyfsBind {
            owner: self.account_name.clone(),
            address: serde_json::to_string(info).map_err(|e| {
                cyfs_err!(BuckyErrorCode::Failed, "serde err {}", e)
            })?
        };

        let trans = TransactionBuilder::new().add_action(
            "cyfsaddrinfo",
            "bind", vec![Authorization {
                actor: self.account_name.clone(),
                permission: "light".to_string() }], params)?.build();
        self.send_transaction(trans).await
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct OrderResult {
    find_dmc_order: Vec<TrackerDMCOrder>,
}

#[derive(Serialize, Deserialize, Clone)]
struct FindDmcOrder {
    data: OrderResult
}

pub struct DMCClient<T: DMCTxSender> {
    rpc: DMCRpc,
    account_name: String,
    tracker_server: String,
    sender: T
}

impl<T: DMCTxSender> Deref for DMCClient<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl<T: DMCTxSender> DMCClient<T> {
    pub fn new(account_name: &str, server: &str, tracker_server: &str, sender: T) -> Self {
        let rpc = DMCRpc::new(server);
        Self {
            rpc,
            account_name: account_name.to_string(),
            tracker_server: tracker_server.to_string(),
            sender
        }
    }

    pub fn get_account_name(&self) -> &str {
        self.account_name.as_str()
    }

    pub async fn get_user_orders(&self, _limit: Option<i32>) -> BuckyResult<Vec<TrackerDMCOrder>> {
        let query = format!(r#"{{find_dmc_order(
                                    skip: 0,
                                    limit: 10,
                                    order: "-createdAt,order_id",
                                    where: {{
                                        and:[
                                            {{user: "{}"}},
                                            {{state: 0}}
                                        ]
                                    }},
                            ){{
                                order_id
                                user
                                miner
                                bill_id
                                user_pledge_amount
                                user_pledge_symbol
                                miner_pledge_amount
                                miner_pledge_symbol
                                price_amount
                                price_symbol
                                settlement_pledge_amount
                                settlement_pledge_symbol
                                lock_amount
                                lock_symbol
                                state
                                deliver_start_date
                                latest_settlement_date
                            }}
                        }}"#, self.account_name.as_str());
        let url = format!("{}/1.1", self.tracker_server.as_str());
        for _ in 0..3 {
            return match http_post_request3(url.as_str(), query.as_bytes(), Some("application/graphql")).await {
                Ok(FindDmcOrder{ data }) => Ok(data.find_dmc_order),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "get_user_orders failed"))
    }


    pub async fn get_miner_orders(&self, limit: Option<(u32, u32)>) -> BuckyResult<Vec<TrackerDMCOrder>> {
        let query = format!(r#"{{find_dmc_order(
                                    skip: {},
                                    limit: {},
                                    order: "-createdAt,order_id",
                                    where: {{
                                        and:[
                                            {{miner: "{}"}}
                                        ]
                                    }},
                            ){{
                                order_id
                                user
                                miner
                                bill_id
                                user_pledge_amount
                                user_pledge_symbol
                                miner_pledge_amount
                                miner_pledge_symbol
                                price_amount
                                price_symbol
                                settlement_pledge_amount
                                settlement_pledge_symbol
                                lock_amount
                                lock_symbol
                                state
                                deliver_start_date
                                latest_settlement_date
                            }}
                        }}"#,
                            limit.unwrap_or((0, 100)).0,
                            limit.unwrap_or((0, 100)).1,
                            self.account_name.as_str());
        let url = format!("{}/1.1", self.tracker_server.as_str());
        for _ in 0..3 {
            return match http_post_request3(url.as_str(), query.as_bytes(), Some("application/graphql")).await {
                Ok(FindDmcOrder{ data }) => Ok(data.find_dmc_order),
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "get_user_orders failed"))
    }

    pub async fn get_order_by_id(&self, order_id: &str) -> BuckyResult<Option<TrackerDMCOrder>> {
        let query = format!(r#"{{find_dmc_order(
                                    skip: 0,
                                    limit: 10,
                                    order: "-createdAt,order_id",
                                    where: {{
                                        and:[
                                            {{order_id: "{}"}}
                                        ]
                                    }},
                            ){{
                                order_id
                                user
                                miner
                                bill_id
                                user_pledge_amount
                                user_pledge_symbol
                                miner_pledge_amount
                                miner_pledge_symbol
                                price_amount
                                price_symbol
                                settlement_pledge_amount
                                settlement_pledge_symbol
                                lock_amount
                                lock_symbol
                                state
                                deliver_start_date
                                latest_settlement_date
                            }}
                        }}"#, order_id);
        let url = format!("{}/1.1", self.tracker_server.as_str());
        for _ in 0..3 {
            return match http_post_request3(url.as_str(), query.as_bytes(), Some("application/graphql")).await {
                Ok(FindDmcOrder{ data }) => {
                    if data.find_dmc_order.len() == 0 {
                        Ok(None)
                    } else {
                        Ok(Some(data.find_dmc_order[0].clone()))
                    }
                },
                Err(e) => {
                    if e.code() == BuckyErrorCode::ConnectFailed {
                        continue;
                    }
                    Err(e)
                }
            }
        }
        Err(cyfs_err!(BuckyErrorCode::Failed, "get_user_orders failed"))
    }

    pub async fn get_order_of_miner(&self, order_id: &str) -> BuckyResult<Option<TrackerDMCOrder>> {
        self.get_order_by_id(order_id).await
    }

    pub async fn get_challenge_info(
        &self,
        order_id: &str,
        limit: Option<i32>
    ) -> BuckyResult<GetTableRowsResult<DMCChallengeInfo>> {
        let req = GetTableRowsReq {
            json: true,
            code: "dmc.token",
            table: "dmchallenge",
            scope: "dmc.token",
            index_position: None,
            key_type: None,
            encode_type: None,
            lower_bound: Some(order_id),
            upper_bound: Some(order_id),
            limit,
            reverse: Some(true),
            show_payer: None
        };

        self.rpc.get_table_rows(&req).await
    }

    pub async fn get_cyfs_info(&self, dmc_account: String) -> BuckyResult<CyfsInfo> {
        let req = GetTableRowsReq {
            json: true,
            code: "cyfsaddrinfo",
            table: "accountmap",
            scope: "cyfsaddrinfo",
            index_position: None,
            key_type: None,
            encode_type: None,
            lower_bound: Some(dmc_account.as_str()),
            upper_bound: Some(dmc_account.as_str()),
            limit: None,
            reverse: None,
            show_payer: None
        };

        let resp: GetTableRowsResult<CyfsAccount> = self.rpc.get_table_rows(&req).await?;
        if resp.rows.len() == 0 {
            Err(cyfs_err!(BuckyErrorCode::NotFound, "can't find {} cyfs info", dmc_account))
        } else {
            serde_json::from_str(resp.rows[0].address.as_str()).map_err(|e| {
                cyfs_err!(BuckyErrorCode::Failed, "parse {} failed {}", resp.rows[0].address, e)
            })
        }
    }

    pub async fn get_pst_trans_info(&self) -> BuckyResult<PstTransInfo> {
        let req = GetTableRowsReq {
            json: true,
            code: "dmc.token",
            table: "priceavg",
            scope: "dmc.token",
            index_position: None,
            key_type: None,
            encode_type: None,
            lower_bound: None,
            upper_bound: None,
            limit: None,
            reverse: None,
            show_payer: None
        };

        let mut resp: GetTableRowsResult<PstTransInfo> = self.rpc.get_table_rows(&req).await?;
        if resp.rows.len() == 0 {
            Err(cyfs_err!(BuckyErrorCode::NotFound, "can't find pst info"))
        } else {
            Ok(resp.rows.pop().unwrap())
        }
    }

    pub async fn get_pst_amount(&self, dmc_account: &str) -> BuckyResult<u64> {
        let req = GetTableRowsReq {
            json: true,
            code: "dmc.token",
            table: "pststats",
            scope: "dmc.token",
            index_position: None,
            key_type: None,
            encode_type: None,
            lower_bound: Some(dmc_account),
            upper_bound: Some(dmc_account),
            limit: None,
            reverse: None,
            show_payer: None
        };

        let mut resp: GetTableRowsResult<PstStat> = self.rpc.get_table_rows(&req).await?;
        if resp.rows.len() == 0 {
            Ok(0)
        } else {
            let stat = resp.rows.pop().unwrap();
            let amount = stat.amount.quantity.trim_end_matches("PST").trim();
            amount.parse().map_err(|e| {
                cyfs_err!(BuckyErrorCode::Failed, "parse {} err {}", amount, e)
            })
        }
    }

    pub async fn get_stake_info(&self, dmc_account: &str) -> BuckyResult<StakeInfo> {
        let req = GetTableRowsReq {
            json: true,
            code: "dmc.token",
            table: "dmcmaker",
            scope: "dmc.token",
            index_position: None,
            key_type: None,
            encode_type: None,
            lower_bound: Some(dmc_account),
            upper_bound: Some(dmc_account),
            limit: None,
            reverse: None,
            show_payer: None
        };

        let mut resp: GetTableRowsResult<StakeInfo> = self.rpc.get_table_rows(&req).await?;
        if resp.rows.len() == 0 {
            Err(cyfs_err!(BuckyErrorCode::NotFound, "can't find pst info"))
        } else {
            Ok(resp.rows.pop().unwrap())
        }
    }

    pub async fn get_bill_list(&self, dmc_account: &str, limit: Option<i32>) -> BuckyResult<Vec<BillRecord>> {
        let scope = string_to_name(dmc_account);
        let req = GetTableRowsReq {
            json: true,
            code: "dmc.token",
            table: "stakerec",
            scope: scope.as_str(),
            index_position: None,
            key_type: None,
            encode_type: None,
            lower_bound: None,
            upper_bound: None,
            limit,
            reverse: None,
            show_payer: None
        };

        let resp: GetTableRowsResult<BillRecord> = self.rpc.get_table_rows(&req).await?;
        if resp.rows.len() == 0 {
            Ok(Vec::new())
        } else {
            Ok(resp.rows)
        }
    }
}
