use std::collections::HashMap;
use std::str::FromStr;
use cyfs_base::{BuckyErrorCode, BuckyResult};
use sha2::Digest;
use crate::*;

pub struct SignatureProviderArgs {
    pub chain_id: String,
    pub required_keys: Vec<String>,
    pub serialized_transaction: Vec<u8>,
    pub serialized_context_free_data: Option<Vec<u8>>,
    pub abis: Vec<BinaryAbi>,
}

pub trait SignatureProvider: Send + Sync {
    fn get_available_keys(&self) -> &Vec<String>;
    fn sign(&self, args: SignatureProviderArgs) -> BuckyResult<PushTransactionArgs>;
}

pub struct SimpleSignatureProvider {
    available_keys: Vec<String>,
    keys: HashMap<String, DMCPrivateKey>,
}

impl SimpleSignatureProvider {
    pub fn new(priv_keys: Vec<String>) -> BuckyResult<Self> {
        let mut available_keys = Vec::new();
        let mut keys = HashMap::new();
        for key in priv_keys.into_iter() {
            let private_key = DMCPrivateKey::from_str(key.as_str())?;
            let public_key = private_key.get_public_key().to_legacy_string()?;
            log::info!("public_key {}", public_key.as_str());
            available_keys.push(public_key.clone());
            keys.insert(public_key, private_key);
        }
        Ok(Self { available_keys, keys })
    }
}

impl SignatureProvider for SimpleSignatureProvider {
    fn get_available_keys(&self) -> &Vec<String> {
        &self.available_keys
    }

    fn sign(&self, args: SignatureProviderArgs) -> BuckyResult<PushTransactionArgs> {
        let chain_id = hex::decode(args.chain_id.as_str()).map_err(|e| {
            cyfs_err!(BuckyErrorCode::InvalidParam, "decode chain_id {} failed {:?}", args.chain_id.as_str(), e)
        })?;
        let hash = if args.serialized_context_free_data.is_some() {
            let mut sha256 = sha2::Sha256::new();
            sha256.update(args.serialized_context_free_data.as_ref().unwrap().as_slice());
            sha256.finalize().to_vec()
        } else {
            let mut buf = Vec::<u8>::new();
            buf.resize(32, 0);
            buf
        };
        let sign_buf = vec![chain_id, args.serialized_transaction.clone(), hash].concat();

        let mut sha256 = sha2::Sha256::new();
        sha256.update(sign_buf.as_slice());
        let sign_hash = sha256.finalize().to_vec();

        let mut signatures = Vec::new();
        for key in args.required_keys.iter() {
            let public_key = DMCPublicKey::from_str(key)?;
            let private_key = self.keys.get(public_key.to_legacy_string()?.as_str());
            if private_key.is_none() {
                continue;
            }
            let signature = private_key.unwrap().sign(sign_hash.as_slice(), false)?;
            signatures.push(signature.to_string());
        }
        Ok(PushTransactionArgs {
            signatures,
            compression: None,
            serialized_transaction: args.serialized_transaction,
            serialized_context_free_data: args.serialized_context_free_data
        })
    }
}
