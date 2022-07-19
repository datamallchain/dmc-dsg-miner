use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use cyfs_base::{BuckyErrorCode, BuckyResult};
use flate2::Compression;
use serde::{Serialize, Deserialize};
use crate::*;

pub struct CachedAbi {
    pub raw_abi: Vec<u8>,
    pub abi: AbiDef,
}

pub struct TransactionConfig {
    pub broadcast: Option<bool>,
    pub sign: Option<bool>,
    pub read_only_trx: Option<bool>,
    pub return_failure_traces: Option<bool>,
    pub required_keys: Option<Vec<String>>,
    pub compression: Option<bool>,
    pub blocks_behind: Option<i64>,
    pub use_last_irreversible: Option<bool>,
    pub expire_seconds: Option<i64>,
}

#[derive(Serialize, Deserialize)]
pub enum TransResult {
    TransactResult(TransactResult),
    ReadOnlyTransactResult(ReadOnlyTransactResult),
    PushTransactionArgs(PushTransactionArgs),
}

pub struct BlockTaposInfo {
    pub block_num: i64,
    pub id: String,
    pub timestamp: Option<String>,
    pub header: Option<BlockHeader>,
}

pub fn transaction_header(ref_block: BlockTaposInfo, expire_seconds: i64) -> BuckyResult<TransactionHeader> {
    let timestamp = if ref_block.header.is_some() {
        ref_block.header.as_ref().unwrap().timestamp.clone()
    } else {
        ref_block.timestamp.clone().unwrap()
    };
    let prefix = u32::from_str_radix(reverse_hex(&ref_block.id[16..24]).as_str(), 16).unwrap();
    Ok(TransactionHeader {
        expiration: time_point_sec_to_date(date_to_time_point(timestamp.as_str())? + expire_seconds),
        ref_block_num: ref_block.block_num as u16 & 0xffff,
        ref_block_prefix: prefix
    })
}

pub struct DMCApi<T: 'static + SignatureProvider> {
    rpc: Arc<DMCRpc>,
    cached_abis: Mutex<HashMap<String, Arc<CachedAbi>>>,
    sign_provider: T,
    chain_id: Mutex<String>,
}

impl<T: SignatureProvider> DMCApi<T> {
    pub fn new(rpc: Arc<DMCRpc>, sign_provider: T) -> Self {
        Self {
            rpc,
            cached_abis: Default::default(),
            sign_provider,
            chain_id: Mutex::new("".to_string())
        }
    }

    pub fn rpc(&self) -> &Arc<DMCRpc> {
        &self.rpc
    }

    pub async fn get_cached_abi(&self, account_name: &str, reload: bool) -> BuckyResult<Arc<CachedAbi>> {
        if !reload {
            let cached_abis = self.cached_abis.lock().unwrap();
            if let Some(abi) = cached_abis.get(account_name) {
                return Ok(abi.clone());
            }
        }

        let mut raw_abi = self.rpc.get_bin_abi(account_name).await?;
        let abi = AbiDef::parse(&mut raw_abi.abi)?;
        let mut cached_abis = self.cached_abis.lock().unwrap();
        cached_abis.insert(account_name.to_string(), Arc::new(CachedAbi {
            raw_abi: raw_abi.abi,
            abi
        }));

        Ok(cached_abis.get(account_name).unwrap().clone())
    }

    pub async fn get_transaction_abis(&self, transaction: &Transaction, reload: bool) -> BuckyResult<Vec<BinaryAbi>> {
        let mut account_list: Vec<Name> = transaction.context_free_actions.iter().map(|a| a.account.clone()).collect();
        let mut account_list2 = transaction.actions.iter().map(|a| a.account.clone()).collect();
        account_list.append(&mut account_list2);

        let mut tasks = Vec::new();
        for account in account_list.into_iter() {
            let this: &'static DMCApi<T>  = unsafe {std::mem::transmute(&*self)};
            let task = async_std::task::spawn(async move {
                (account.clone(), this.get_cached_abi(account.as_str(), reload).await)
            });
            tasks.push(task);
        }

        let ret_list = futures::future::join_all(tasks).await;
        let mut list = Vec::new();
        for (account_name, ret) in ret_list.into_iter() {
            list.push(BinaryAbi {
                account_name,
                abi: ret?.raw_abi.clone()
            });
        }

        Ok(list)
    }

    fn deflate_serialized_array(&self, data: &[u8]) -> BuckyResult<Vec<u8>> {
        let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(data).map_err(|e| {
            cyfs_err!(BuckyErrorCode::Failed, "write data failed {}", e)
        })?;
        encoder.finish().map_err(|e| {
            cyfs_err!(BuckyErrorCode::Failed, "finish err {}", e)
        })
    }

    async fn try_get_block_info(&self, block_number: i64) -> BuckyResult<GetBlockInfoResult> {
        match self.rpc.get_block_info(block_number).await {
            Ok(info) => {
                Ok(info)
            },
            Err(_) => {
                let ret = self.rpc.get_block(block_number.to_string()).await?;
                Ok(GetBlockInfoResult {
                    timestamp: ret.timestamp,
                    producer: ret.producer,
                    confirmed: ret.confirmed,
                    previous: ret.previous,
                    transaction_mroot: ret.transaction_mroot,
                    action_mroot: ret.action_mroot,
                    schedule_version: ret.schedule_version,
                    producer_signature: ret.producer_signature,
                    id: ret.id,
                    block_num: ret.block_num,
                    ref_block_num: 0,
                    ref_block_prefix: ret.ref_block_prefix
                })
            }
        }
    }

    async fn try_ref_block_from_get_info(&self, info: GetInfoResult) -> BuckyResult<BlockTaposInfo> {
        if info.last_irreversible_block_time.is_some() {
            Ok(BlockTaposInfo {
                block_num: info.last_irreversible_block_num,
                id: info.last_irreversible_block_id.clone(),
                timestamp: info.last_irreversible_block_time.clone(),
                header: None
            })
        } else {
            let ret = self.try_get_block_info(info.last_irreversible_block_num).await?;
            Ok(BlockTaposInfo {
                block_num: ret.block_num,
                id: ret.id,
                timestamp: Some(ret.timestamp),
                header: None
            })
        }
    }

    async fn try_get_block_header_state(&self, tapos_block_number: i64) -> BuckyResult<GetBlockHeaderStateResult> {
        self.rpc.get_block_header_state(tapos_block_number.to_string()).await
    }

    async fn generate_tapos(
        &self,
        info: Option<GetInfoResult>,
        mut transaction: Transaction,
        block_behind: Option<i64>,
        use_last_irreversible: Option<bool>,
        expire_seconds: i64
    ) -> BuckyResult<Transaction> {
        let info = if info.is_some() {
            info.unwrap()
        } else {
            self.rpc.get_info().await?
        };

        if use_last_irreversible.is_some() && use_last_irreversible.unwrap() {
            let block = self.try_ref_block_from_get_info(info).await?;
            let header = transaction_header(block, expire_seconds)?;
            transaction.ref_block_prefix = header.ref_block_prefix;
            transaction.expiration = header.expiration;
            transaction.ref_block_num = header.ref_block_num;
            return Ok(transaction);
        }

        let tapos_block_number = info.head_block_num - block_behind.unwrap();
        let block = if tapos_block_number < info.last_irreversible_block_num {
            let ret = self.try_get_block_info(tapos_block_number).await?;
            BlockTaposInfo {
                block_num: ret.block_num,
                id: ret.id,
                timestamp: Some(ret.timestamp) ,
                header: None
            }
        } else {
            let ret = self.try_get_block_header_state(tapos_block_number).await?;
            BlockTaposInfo {
                block_num: ret.block_num,
                id: ret.id,
                timestamp: Some(ret.header.timestamp),
                header: None
            }
        };
        let header = transaction_header(block, expire_seconds)?;
        transaction.ref_block_prefix = header.ref_block_prefix;
        transaction.expiration = header.expiration;
        transaction.ref_block_num = header.ref_block_num;
        return Ok(transaction);
    }

    pub async fn serialize_transaction_extensions(&self, transcation: &Transaction) -> BuckyResult<Vec<(i64, String)>> {
        let mut transaction_extensions = Vec::new();
        if transcation.resource_payer.is_some() {
            let mut data = Vec::new();
            let mut buf = SerialBuffer::new(&mut data);
            transcation.resource_payer.as_ref().unwrap().dmc_serialize(&mut buf)?;
            transaction_extensions.push((1, hex::encode(buf.as_slice()).to_uppercase()));
        }
        Ok(transaction_extensions)
    }

    pub fn serialize_context_free_data(&self, context_free_data: &Vec<Vec<u8>>) -> Vec<u8> {
        let mut data = Vec::new();
        let mut buf = SerialBuffer::new(&mut data);
        buf.push_var_u32(context_free_data.len() as u32);
        for item in context_free_data.iter() {
            buf.push_bytes(item.as_slice());
        }
        return data;
    }

    async fn push_compressed_signed_transaction(&self, trans_args: PushTransactionArgs, read_only_trx: bool, return_failure_traces: bool) -> BuckyResult<TransResult> {
        let compressed_serialized_transaction = self.deflate_serialized_array(trans_args.serialized_transaction.as_slice())?;
        let compressed_serialize_context_free_data = if trans_args.serialized_context_free_data.is_some() {
            self.deflate_serialized_array(trans_args.serialized_context_free_data.as_ref().unwrap().as_slice())?
        } else {
            Vec::new()
        };
        if read_only_trx {
            Ok(TransResult::ReadOnlyTransactResult(self.rpc.push_ro_transaction(
                trans_args.signatures,
                true,
                compressed_serialized_transaction.as_slice(),
                return_failure_traces).await?))
        } else {
            Ok(TransResult::TransactResult(self.rpc.push_transaction(PushTransactionArgs {
                signatures: trans_args.signatures,
                compression: trans_args.compression,
                serialized_transaction: compressed_serialized_transaction,
                serialized_context_free_data: Some(compressed_serialize_context_free_data)
            }).await?))
        }
    }

    async fn push_signed_transaction(&self, trans_args: PushTransactionArgs, read_only_trx: bool, return_failure_traces: bool) -> BuckyResult<TransResult> {
        if read_only_trx {
            Ok(TransResult::ReadOnlyTransactResult(self.rpc.push_ro_transaction(
                trans_args.signatures,
                false,
                trans_args.serialized_transaction.as_slice(),
                return_failure_traces).await?))
        } else {
            Ok(TransResult::TransactResult(self.rpc.push_transaction(PushTransactionArgs {
                signatures: trans_args.signatures,
                compression: trans_args.compression,
                serialized_transaction: trans_args.serialized_transaction,
                serialized_context_free_data: trans_args.serialized_context_free_data
            }).await?))
        }
    }

    fn has_required_tapos_fields(trans: &Transaction) -> bool {
        !trans.expiration.is_empty()
    }

    pub async fn transact(
        &self,
        mut transaction: Transaction,
        config: TransactionConfig
    ) -> BuckyResult<TransResult> {
        let chain_info = if self.chain_id.lock().unwrap().is_empty() {
            let info = self.rpc.get_info().await?;
            *self.chain_id.lock().unwrap() = info.chain_id.clone();
            Some(info)
        } else {
            None
        };
        if (config.blocks_behind.is_some() || (config.use_last_irreversible.is_some() && config.use_last_irreversible.unwrap())) && config.expire_seconds.unwrap() > 0 {
            transaction = self.generate_tapos(
                chain_info,
                transaction,
                config.blocks_behind.clone(),
                config.use_last_irreversible.clone(),
                config.expire_seconds.unwrap()).await?;
        }

        if !Self::has_required_tapos_fields(&transaction) {
            return Err(cyfs_err!(BuckyErrorCode::Failed, "Required configuration or TAPOS fields are not present"));
        }

        let abis = self.get_transaction_abis(&transaction, false).await?;

        let mut serialized_transaction = Vec::new();
        let mut buf = SerialBuffer::new(&mut serialized_transaction);
        transaction.dmc_serialize(&mut buf)?;

        let serialized_context_free_data = if transaction.context_free_data.is_some() {
            Some(self.serialize_context_free_data(transaction.context_free_data.as_ref().unwrap()))
        } else {
            None
        };

        let mut push_trans_args = if config.sign.is_some() && config.sign.unwrap() {
            if config.required_keys.is_none() {
                return Err(cyfs_err!(BuckyErrorCode::Failed, "required keys not set"));
            }

            let required_keys = config.required_keys.unwrap();
            self.sign_provider.sign(SignatureProviderArgs {
                chain_id: self.chain_id.lock().unwrap().clone(),
                required_keys,
                serialized_transaction,
                serialized_context_free_data,
                abis
            })?
        } else {
            PushTransactionArgs {
                    signatures: vec![],
                    compression: config.compression.clone(),
                    serialized_transaction,
                    serialized_context_free_data
                }
        };

        if config.broadcast.is_some() && config.broadcast.unwrap() {
            if config.compression.is_some() && config.compression.unwrap() {
                push_trans_args.compression = config.compression;
                self.push_compressed_signed_transaction(
                    push_trans_args,
                    config.read_only_trx.unwrap_or(false),
                    config.return_failure_traces.unwrap_or(false)
                ).await
            } else {
                self.push_signed_transaction(
                    push_trans_args,
                    config.read_only_trx.unwrap_or(false),
                    config.return_failure_traces.unwrap_or(false)
                ).await
            }
        } else {
            Ok(TransResult::PushTransactionArgs(push_trans_args))
        }
    }
}

pub struct TransactionBuilder {
    account_name: String,
    actions: Vec<Action>,
    context_free_actions: Vec<Action>
}

impl TransactionBuilder {
    pub fn new() -> Self {
        Self {
            account_name: "".to_string(),
            actions: vec![],
            context_free_actions: vec![]
        }
    }

    pub fn with(mut self, account_name: &str) -> Self {
        self.account_name = account_name.to_string();
        self
    }

    pub fn add_action<T: DMCSerialize>(mut self, contract_name: &str, action_name: &str, authorization: Vec<Authorization>, data: T) -> BuckyResult<Self> {
        let mut raw_buf = Vec::new();
        let mut serial_buf = SerialBuffer::new(&mut raw_buf);
        data.dmc_serialize(&mut serial_buf)?;
        if cfg!(debug_assertions) {
            let action_hex = hex::encode(raw_buf.as_slice()).to_uppercase();
            log::info!("action hex {}", action_hex);
        }
        self.actions.push(Action {
            account: contract_name.to_string(),
            name: action_name.to_string(),
            authorization,
            data: raw_buf,
            hex_data: None
        });
        Ok(self)
    }

    pub fn add_context_free_action<T: DMCSerialize>(mut self, contract_name: &str, action_name: &str, authorization: Vec<Authorization>, data: T) -> BuckyResult<Self> {
        let mut raw_buf = Vec::new();
        let mut serial_buf = SerialBuffer::new(&mut raw_buf);
        data.dmc_serialize(&mut serial_buf)?;
        if cfg!(debug_assertions) {
            let action_hex = hex::encode(raw_buf.as_slice()).to_uppercase();
            log::info!("action hex {}", action_hex);
        }
        self.context_free_actions.push(Action {
            account: contract_name.to_string(),
            name: action_name.to_string(),
            authorization,
            data: raw_buf,
            hex_data: None
        });
        Ok(self)
    }

    pub fn build(self) -> Transaction {
        Transaction {
            expiration: "".to_string(),
            ref_block_num: 0,
            ref_block_prefix: 0,
            max_net_usage_words: 0,
            max_cpu_usage_ms: 0,
            delay_sec: 0,
            context_free_actions: self.context_free_actions,
            context_free_data: None,
            actions: self.actions,
            transaction_extensions: vec![],
            resource_payer: None
        }
    }
}
