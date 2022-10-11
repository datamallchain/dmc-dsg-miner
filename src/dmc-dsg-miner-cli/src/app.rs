use std::sync::Arc;
use async_std::task::JoinHandle;
use cyfs_base::{BuckyErrorCode, BuckyResult, NamedObject, ObjectDesc, ObjectId, OwnerObjectDesc, RawConvertTo};
use cyfs_lib::{SharedCyfsStack, UtilGetSystemInfoOutputRequest};
use dmc_dsg_base::{Authority, DMCClient, DSGJSON, JSONObject, JsonProtocol, KeyWeight, SharedCyfsStackEx, SimpleSignatureProvider, cyfs_err, SetDMCAccount, DMCPrivateKey, CyfsPath};

pub struct DmcInfo {
    pub dmc_account: String,
    pub pst: String,
    pub stake_dmc: String,
    pub max_mint_pst: String,
    pub price: String,
}

pub struct App {
    stack: Arc<SharedCyfsStack>,
    dec_id: ObjectId,
    ood_id: ObjectId,
    owner_id: ObjectId,
    dmc_sever: String,
    req_path: String,
}

impl App {
    pub async fn new(stack: Arc<SharedCyfsStack>, dec_id: ObjectId, dmc_server: String) -> BuckyResult<Self> {
        let owner_id = stack.local_device().desc().owner().as_ref().unwrap().clone();
        let ood_id = stack.resolve_ood(owner_id.clone()).await?;
        let cyfs_path = CyfsPath::new(ood_id, dec_id.clone(), "commands");
        Ok(Self {
            stack,
            dec_id,
            ood_id,
            owner_id,
            dmc_sever: dmc_server.to_string(),
            req_path: cyfs_path.to_path()
        })
    }

    pub async fn get_dmc_key(&self, dmc_account: &str) -> BuckyResult<String> {
        let req = JSONObject::new(self.dec_id.clone(), self.owner_id.clone(), JsonProtocol::GetDMCKey as u16, &dmc_account.to_string())?;
        let req_id = req.desc().calculate_id();
        let resp: JSONObject = self.stack.put_object_with_resp2(self.req_path.as_str(), req_id, req.to_vec()?).await?;
        Ok(resp.get()?)
    }

    pub async fn set_dmc_account(&self, dmc_account: &str, private_key: &str) -> BuckyResult<()> {
        let req = JSONObject::new(self.dec_id.clone(), self.owner_id.clone(), JsonProtocol::SetDMCAccount as u16, &SetDMCAccount {
            dmc_account: dmc_account.to_string(),
            dmc_key: private_key.to_string()
        })?;
        let req_id = req.desc().calculate_id();
        let _: JSONObject = self.stack.put_object_with_resp2(self.req_path.as_str(), req_id, req.to_vec()?).await?;
        Ok(())
    }

    pub async fn create_light_auth(&self, dmc_account: &str, private_key: &str) -> BuckyResult<()> {
        let light_private_key = DMCPrivateKey::gen_key();
        let dmc_key = light_private_key.get_public_key().to_legacy_string()?;
        let dmc_client = DMCClient::new(
            dmc_account,
            self.dmc_sever.as_str(),
            SimpleSignatureProvider::new(vec![private_key.to_string()])?);

        let task: JoinHandle<BuckyResult<()>> = async_std::task::spawn(async move {
            let _ = dmc_client.unlink_auth("cyfsaddrinfo".to_string(), "bind".to_string(), "active".to_string()).await;
            let _ = dmc_client.unlink_auth("eosio.token".to_string(), "addmerkle".to_string(), "active".to_string()).await;
            let _ = dmc_client.unlink_auth("eosio.token".to_string(), "reqchallenge".to_string(), "active".to_string()).await;
            let _ = dmc_client.unlink_auth("eosio.token".to_string(), "anschallenge".to_string(), "active".to_string()).await;
            let _ = dmc_client.unlink_auth("eosio.token".to_string(), "arbitration".to_string(), "active".to_string()).await;
            let _ = dmc_client.delete_auth("light".to_string(), "active".to_string()).await;

            dmc_client.update_auth("light".to_string(), "active".to_string(), Authority {
                threshold: 1,
                keys: vec![KeyWeight {
                    key: dmc_key,
                    weight: 1
                }],
                accounts: vec![],
                waits: vec![]}).await?;

            dmc_client.link_auth("cyfsaddrinfo".to_string(), "bind".to_string(), "active".to_string()).await?;
            dmc_client.link_auth("eosio.token".to_string(), "addmerkle".to_string(), "active".to_string()).await?;
            dmc_client.link_auth("eosio.token".to_string(), "reqchallenge".to_string(), "active".to_string()).await?;
            dmc_client.link_auth("eosio.token".to_string(), "anschallenge".to_string(), "active".to_string()).await?;
            dmc_client.link_auth("eosio.token".to_string(), "arbitration".to_string(), "active".to_string()).await?;

            println!("light key {}", light_private_key.to_legacy_string()?);
            Ok(())
        });

        task.await?;


        Ok(())
    }

    pub async fn stake(&self, dmc_account: &str, private_key: &str, dmc_count: &str) -> BuckyResult<()> {
        let dmc_client = DMCClient::new(
            dmc_account,
            self.dmc_sever.as_str(),
            SimpleSignatureProvider::new(vec![private_key.to_string()])?);
        dmc_client.stake(dmc_count).await?;
        Ok(())
    }

    pub async fn mint(&self, dmc_account: &str, private_key: &str, pst_count: &str) -> BuckyResult<()> {
        let resp = self.stack.util().get_system_info(UtilGetSystemInfoOutputRequest {
            common: Default::default()
        }).await?;
        let space = resp.info.hdd_disk_avail + resp.info.ssd_disk_avail;
        let mut max_pst = space / (1024 * 1024 * 1024) * 85 / 100;

        let dmc_client = DMCClient::new(
            dmc_account,
            self.dmc_sever.as_str(),
            SimpleSignatureProvider::new(vec![private_key.to_string()])?);
        let minted_pst = dmc_client.get_pst_amount(dmc_account).await?;
        let stake_info = dmc_client.get_stake_info(dmc_account).await?;
        let pst_info = dmc_client.get_pst_trans_info().await?;
        let quantity: f64 = stake_info.total_staked.quantity.trim_end_matches("DMC").trim().parse().map_err(|e| {
            cyfs_err!(BuckyErrorCode::Failed, "parse {} err {}", stake_info.total_staked.quantity.as_str(), e)
        })?;

        let stake_max_pst = (quantity / (2f64 * pst_info.avg())) as u64;
        if max_pst > stake_max_pst {
            max_pst = stake_max_pst;
        }

        let input_pst: u64 = pst_count.parse().map_err(|e| {
            cyfs_err!(BuckyErrorCode::InvalidInput, "parse {} err {}", pst_count, e)
        })?;
        if minted_pst + input_pst > max_pst {
            return Err(cyfs_err!(BuckyErrorCode::InvalidInput, "maximun mining quantity is {} pst, has minted {}", max_pst, minted_pst));
        }
        dmc_client.mint(pst_count).await?;
        Ok(())
    }

    pub async fn bill(&self, dmc_account: &str, private_key: &str, pst_count: &str, price: f64) -> BuckyResult<()> {
        let dmc_client = DMCClient::new(
            dmc_account,
            self.dmc_sever.as_str(),
            SimpleSignatureProvider::new(vec![private_key.to_string()])?);
        dmc_client.bill(pst_count.to_string(), price, "".to_string()).await?;
        Ok(())
    }

    pub async fn get_info(&self, dmc_account: &str) -> BuckyResult<DmcInfo> {
        let dmc_client = DMCClient::new(
            dmc_account,
            self.dmc_sever.as_str(),
            SimpleSignatureProvider::new(vec![])?);
        let minted_pst = dmc_client.get_pst_amount(dmc_account).await?;
        let stake_info = dmc_client.get_stake_info(dmc_account).await?;
        let pst_info = dmc_client.get_pst_trans_info().await?;

        let resp = self.stack.util().get_system_info(UtilGetSystemInfoOutputRequest {
            common: Default::default()
        }).await?;
        let space = resp.info.hdd_disk_avail + resp.info.ssd_disk_avail;
        let mut max_pst = space / (1024 * 1024 * 1024) * 85 / 100;
        let quantity: f64 = stake_info.total_staked.quantity.trim_end_matches("DMC").trim().parse().map_err(|e| {
            cyfs_err!(BuckyErrorCode::Failed, "parse {} err {}", stake_info.total_staked.quantity.as_str(), e)
        })?;
        let stake_max_pst = (quantity / (2f64 * pst_info.avg())) as u64;
        if max_pst > stake_max_pst {
            max_pst = stake_max_pst;
        }

        Ok(DmcInfo {
            dmc_account: dmc_account.to_string(),
            pst: format!("{}", minted_pst),
            stake_dmc: format!("{:.4}", quantity),
            max_mint_pst: format!("{}", max_pst),
            price: format!("{:.4}", pst_info.avg())
        })
    }
}
