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
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Pledge {
    pub quantity: String,
    pub contract: String,
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

#[derive(Serialize, Deserialize, Clone)]
pub struct DMCChallenge {
    pub order_id: String,
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

pub struct DMCClient<T: 'static + SignatureProvider> {
    api: DMCApi<T>,
    account_name: String,
    sign_keys: Vec<String>,
}

impl<T: 'static + SignatureProvider> DMCClient<T> {
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

    pub fn api(&self) -> &DMCApi<T> {
        &self.api
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

    pub async fn update_auth(&self, permission: String, parent: String, auth: Authority) -> BuckyResult<TransResult> {
        let params = UpdateAuth {
            account: self.account_name.clone(),
            permission,
            parent: parent.clone(),
            auth
        };
        let trans = TransactionBuilder::new().add_action(
            "eosio",
            "updateauth", vec![Authorization {
                actor: self.account_name.clone(),
                permission: parent }], params)?.build();
        self.send_transaction(trans).await
    }

    pub async fn delete_auth(&self, permission: String, parent_permission: String) -> BuckyResult<TransResult> {
        let params = DeleteAuth {
            account: self.account_name.clone(),
            permission
        };
        let trans = TransactionBuilder::new().add_action(
            "eosio",
            "deleteauth", vec![Authorization {
                actor: self.account_name.clone(),
                permission: parent_permission }], params)?.build();
        self.send_transaction(trans).await
    }

    pub async fn link_auth(&self, code: String, ty: String, permission: String) -> BuckyResult<TransResult> {
        let params = LinkAuth {
            account: self.account_name.clone(),
            code,
            ty,
            requirement: "light".to_string()
        };
        let trans = TransactionBuilder::new().add_action(
            "eosio",
            "linkauth", vec![Authorization {
                actor: self.account_name.clone(),
                permission }], params)?.build();
        self.send_transaction(trans).await
    }

    pub async fn unlink_auth(&self, code: String, ty: String, permission: String) -> BuckyResult<TransResult> {
        let params = UnlinkAuth {
            account: self.account_name.clone(),
            code,
            ty
        };

        let trans = TransactionBuilder::new().add_action(
            "eosio",
            "unlinkauth", vec![Authorization {
                actor: self.account_name.clone(),
                permission}], params)?.build();
        self.send_transaction(trans).await
    }

    pub async fn stake(&self, amount: &str) -> BuckyResult<TransResult> {
        let params = Increase {
            owner: self.account_name.clone(),
            asset: ExtendedAsset {
                quantity: format!("{:.04} DMC", amount.parse::<f64>().map_err(|e| {cyfs_err!(BuckyErrorCode::InvalidData, "parse {} err {}", amount, e)})?),
                contract: "datamall".to_string()
            },
            miner: self.account_name.clone()
        };

        let trans = TransactionBuilder::new().add_action(
            "eosio.token",
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

    pub async fn bill(&self, asset: String, price: f64, memo: String) -> BuckyResult<TransResult> {
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
            "eosio.token",
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

    pub async fn mint(&self, amount: &str) -> BuckyResult<TransResult> {
        let params = Mint {
            owner: self.account_name.clone(),
            asset: ExtendedAsset {
                quantity: format!("{} PST", amount),
                contract: "datamall".to_string()
            }
        };

            let trans = TransactionBuilder::new().add_action(
                "eosio.token",
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

    pub async fn get_user_orders(&self, limit: Option<i32>) -> BuckyResult<GetTableRowsResult<DMCOrder>> {
        let req = GetTableRowsReq {
            json: true,
            code: "eosio.token",
            table: "dmcorder",
            scope: "eosio.token",
            index_position: Some("secondary"),
            key_type: Some("name"),
            encode_type: None,
            lower_bound: Some(self.account_name.as_str()),
            upper_bound: Some(self.account_name.as_str()),
            limit,
            reverse: Some(true),
            show_payer: None
        };

        self.api.rpc().get_table_rows(&req).await
    }


    pub async fn get_miner_orders(&self, limit: Option<i32>) -> BuckyResult<GetTableRowsResult<DMCOrder>> {
        let req = GetTableRowsReq {
            json: true,
            code: "eosio.token",
            table: "dmcorder",
            scope: "eosio.token",
            index_position: Some("tertiary"),
            key_type: Some("name"),
            encode_type: None,
            lower_bound: Some(self.account_name.as_str()),
            upper_bound: Some(self.account_name.as_str()),
            limit,
            reverse: Some(true),
            show_payer: None
        };

        self.api.rpc().get_table_rows(&req).await
    }

    pub async fn get_order_of_miner(&self, order_id: &str) -> BuckyResult<Option<DMCOrder>> {
        let mut limit = 100;
        loop {
            let rows = self.get_miner_orders(Some(limit)).await?;
            for row in rows.rows.iter() {
                if row.order_id.as_str() == order_id {
                    return Ok(Some(row.clone()));
                }
            }

            if rows.rows.len() < limit as usize {
                break;
            }

            limit += 100;
        }

        Ok(None)
    }

    pub async fn add_merkle(&self, order_id: &str, merkle_root: HashValue, data_block_count: u64) -> BuckyResult<TransResult> {
        let params = AddMerkle {
            sender: self.account_name.clone(),
            order_id: order_id.parse().map_err(|e| {
                cyfs_err!(BuckyErrorCode::InvalidData, "parse order {} err{}", order_id, e)
            })?,
            merkle_root,
            data_block_count
        };

        let trans = TransactionBuilder::new().add_action(
            "eosio.token",
            "addmerkle", vec![Authorization {
                actor: self.account_name.clone(),
                permission: "light".to_string() }], params)?.build();
        self.send_transaction(trans).await
    }

    pub async fn challenge(
        &self,
        order_id: &str,
        data_id: u64,
        hash_data: HashValue,
        nonce: String
    ) -> BuckyResult<TransResult> {
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
            "eosio.token",
            "reqchallenge", vec![Authorization {
                actor: self.account_name.clone(),
                permission: "light".to_string() }], params)?.build();
        self.send_transaction(trans).await
    }

    pub async fn add_challenge_resp(
        &self,
        order_id: &str,
        reply_hash: HashValue
    ) -> BuckyResult<TransResult> {
        let params = ChallengeResp {
            sender: self.account_name.clone(),
            order_id: order_id.parse().map_err(|e| {
                cyfs_err!(BuckyErrorCode::InvalidData, "parse order {} err{}", order_id, e)
            })?,
            reply_hash
        };
        let trans = TransactionBuilder::new().add_action(
            "eosio.token",
            "anschallenge", vec![Authorization {
                actor: self.account_name.clone(),
                permission: "light".to_string() }], params)?.build();
        self.send_transaction(trans).await
    }

    pub async fn arbitration(
        &self,
        order_id: &str,
        data: Vec<u8>,
        cut_merkle: Vec<HashValue>
    ) -> BuckyResult<TransResult> {
        let params = Arbitration {
            sender: self.account_name.clone(),
            order_id: order_id.parse().map_err(|e| {
                cyfs_err!(BuckyErrorCode::InvalidData, "parse order {} err{}", order_id, e)
            })?,
            data,
            cut_merkle
        };
        let trans = TransactionBuilder::new().add_action(
            "eosio.token",
            "arbitration", vec![Authorization {
                actor: self.account_name.clone(),
                permission: "light".to_string() }], params)?.build();
        self.send_transaction(trans).await
    }

    pub async fn get_challenge(
        &self,
        order_id: &str,
        limit: Option<i32>
    ) -> BuckyResult<GetTableRowsResult<DMCChallenge>> {
        let req = GetTableRowsReq {
            json: true,
            code: "eosio.token",
            table: "dmchallenge",
            scope: "eosio.token",
            index_position: None,
            key_type: None,
            encode_type: None,
            lower_bound: Some(order_id),
            upper_bound: Some(order_id),
            limit,
            reverse: Some(true),
            show_payer: None
        };

        self.api.rpc().get_table_rows(&req).await
    }

    pub async fn report_cyfs_info(&self, info: &CyfsInfo) -> BuckyResult<TransResult> {
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

        let resp: GetTableRowsResult<CyfsAccount> = self.api.rpc().get_table_rows(&req).await?;
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
            code: "eosio.token",
            table: "priceavg",
            scope: "eosio.token",
            index_position: None,
            key_type: None,
            encode_type: None,
            lower_bound: None,
            upper_bound: None,
            limit: None,
            reverse: None,
            show_payer: None
        };

        let mut resp: GetTableRowsResult<PstTransInfo> = self.api.rpc().get_table_rows(&req).await?;
        if resp.rows.len() == 0 {
            Err(cyfs_err!(BuckyErrorCode::NotFound, "can't find pst info"))
        } else {
            Ok(resp.rows.pop().unwrap())
        }
    }

    pub async fn get_pst_amount(&self, dmc_account: &str) -> BuckyResult<u64> {
        let req = GetTableRowsReq {
            json: true,
            code: "eosio.token",
            table: "pststats",
            scope: "eosio.token",
            index_position: None,
            key_type: None,
            encode_type: None,
            lower_bound: Some(dmc_account),
            upper_bound: Some(dmc_account),
            limit: None,
            reverse: None,
            show_payer: None
        };

        let mut resp: GetTableRowsResult<PstStat> = self.api.rpc().get_table_rows(&req).await?;
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
            code: "eosio.token",
            table: "dmcmaker",
            scope: "eosio.token",
            index_position: None,
            key_type: None,
            encode_type: None,
            lower_bound: Some(dmc_account),
            upper_bound: Some(dmc_account),
            limit: None,
            reverse: None,
            show_payer: None
        };

        let mut resp: GetTableRowsResult<StakeInfo> = self.api.rpc().get_table_rows(&req).await?;
        if resp.rows.len() == 0 {
            Err(cyfs_err!(BuckyErrorCode::NotFound, "can't find pst info"))
        } else {
            Ok(resp.rows.pop().unwrap())
        }
    }
}
