use std::str::FromStr;
use std::sync::Arc;
use cyfs_base::*;
use cyfs_core::{DecApp, DecAppObj};
use cyfs_lib::*;
use dmc_dsg_base::*;
use dmc_dsg_base::Verifier;
use crate::{AppRef, get_dec_id};
use cyfs_dsg_client::*;


struct DMCDsgServiceEndPoint {
    service: DMCDsgServiceRef,
    local_id: ObjectId,
}

#[async_trait::async_trait]
impl SharedCyfsStackExEndpoint for DMCDsgServiceEndPoint {
    async fn call(&self, param: &RouterHandlerPostObjectRequest) -> BuckyResult<SharedCyfsStackExEndpointResult> {
        if param.request.common.target.is_some() && param.request.common.target.as_ref().unwrap() != &self.local_id {
            return Ok(SharedCyfsStackExEndpointResult::Pass);
        }

        let obj_type = if param.request.object.object.is_some() {
            param.request.object.object.as_ref().unwrap().obj_type()
        } else {
            let any_obj = AnyNamedObject::clone_from_slice(param.request.object.object_raw.as_slice())?;
            any_obj.obj_type()
        };
        log::info!("------># recv obj type : {} source:{}", obj_type, param.request.common.source.to_string());

        if obj_type == JSONDescContent::obj_type() {
            let req_obj = JSONObject::clone_from_slice(param.request.object.object_raw.as_slice())?;
            if req_obj.verify_body() {
                let result = self.service.on_recv_json_obj(&param.request.common, req_obj).await?;
                if result.is_some() {
                    let obj = result.unwrap();
                    Ok(SharedCyfsStackExEndpointResult::Accepted((obj.desc().calculate_id(), obj.to_vec()?)))
                } else {
                    Ok(SharedCyfsStackExEndpointResult::Pass)
                }
            } else {
                Ok(SharedCyfsStackExEndpointResult::Pass)
            }
        } else {
            Ok(SharedCyfsStackExEndpointResult::Pass)
        }
    }
}

pub struct DMCDsgService {
    stack: SharedCyfsStackServerRef,
    dec_id: ObjectId,
    app: AppRef,
    owner_id: ObjectId,
}
pub type DMCDsgServiceRef = Arc<DMCDsgService>;

impl DMCDsgService {
    pub fn new(app: AppRef, dec_id: ObjectId) -> DMCDsgServiceRef {
        let ood_id = app.get_stack().local_device_id().object_id().clone();
        let owner_id = app.get_stack().local_device().desc().owner().as_ref().unwrap().clone();
        let service_api_id = DecApp::generate_id(ObjectId::from_str(DMCDsgConfig::PUB_PEOPLE_ID).unwrap(), DMCDsgConfig::PRODUCT_NAME);
        log::info!("device {}, dec {} service api id {}", &ood_id, &dec_id, &service_api_id);

        let req_path = RequestGlobalStatePath::new(None, Some("commands")).format_string();

        let stack = SharedCyfsStackServer::new("dmc-dsg-miner-service".to_string(),
                                                 app.get_stack().clone(),
                                                 req_path);
        DMCDsgServiceRef::new(Self {
            stack,
            dec_id,
            app,
            owner_id,
        })
    }

    pub async fn listen(self: &DMCDsgServiceRef) -> BuckyResult<()> {
        let listener = DMCDsgServiceEndPoint {
            local_id: self.app.get_stack().local_device_id().object_id().clone(),
            service: self.clone(),
        };
        self.stack.set_end_point(listener);
        self.stack.listen().await?;
        Ok(())
    }

    pub async fn on_recv_json_obj(&self, _req_info: &NONInputRequestCommon, req: JSONObject) -> BuckyResult<Option<JSONObject>> {
        let req_type = req.get_json_obj_type();
        log::info!("recv json req {}", req_type);

        if req_type == JsonProtocol::GetDMCKey as u16 {
            self.on_get_dmc_key(req.get()?).await
        } else if req_type == JsonProtocol::GetDMCAccount as u16 {
            self.on_get_dmc_account().await
        } else if req_type == JsonProtocol::SetDMCAccount as u16 {
            self.on_set_dmc_account(req.get()?).await
        } else if req_type == JsonProtocol::SetHttpDomain as u16 {
            self.on_set_http_domain(req.get()?).await
        } else {
            Err(cyfs_err!(BuckyErrorCode::NotSupport, "req_type {}", req_type))
        }
    }

    async fn on_get_dmc_key(&self, dmc_account: String) -> BuckyResult<Option<JSONObject>> {
        let ret = self.app.get_dmc_public_key(dmc_account).await?;
        Ok(Some(JSONObject::new(
            self.dec_id.clone(),
            self.owner_id.clone(),
            JsonProtocol::GetDMCKeyResp as u16,
            &ret
        )?))
    }

    async fn on_set_dmc_account(&self, req: SetDMCAccount) -> BuckyResult<Option<JSONObject>> {
        self.app.set_dmc_account(req.dmc_account, req.dmc_key).await?;
        Ok(Some(JSONObject::new(
            self.dec_id.clone(),
            self.owner_id.clone(),
            JsonProtocol::SetDMCAccountResp as u16,
            &"".to_string()
        )?))
    }

    async fn on_get_dmc_account(&self) -> BuckyResult<Option<JSONObject>> {
        let ret = self.app.get_dmc_account().await?;
        Ok(Some(JSONObject::new(
            self.dec_id.clone(),
            self.owner_id.clone(),
            JsonProtocol::GetDMCAccountResp as u16,
            &ret
        )?))
    }

    async fn on_set_http_domain(&self, domain: String) -> BuckyResult<Option<JSONObject>> {
        self.app.set_http_domain(domain).await?;
        Ok(Some(JSONObject::new(
            self.dec_id.clone(),
            self.owner_id.clone(),
            JsonProtocol::SetHttpDomainResp as u16,
            &"".to_string()
        )?))
    }
}
