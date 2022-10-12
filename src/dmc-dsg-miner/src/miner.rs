use std::{
    sync::Arc,
};
use cyfs_base::*;
use cyfs_lib::*;
use cyfs_util::*;
use cyfs_dsg_client::*;
use crate::*;

#[derive(Clone)]
pub struct OodMiner {
    stack: Arc<SharedCyfsStack>,
    owner_id: ObjectId,
    miner: Arc<DmcDsgMiner<SharedCyfsStack, CyfsStackMetaConnection, CyfsStackMetaStore, NocChunkStore, CyfsStackFileDownloader>>
}


impl OodMiner {
    pub async fn new(stack: Arc<SharedCyfsStack>, miner: Arc<DmcDsgMiner<SharedCyfsStack, CyfsStackMetaConnection, CyfsStackMetaStore, NocChunkStore, CyfsStackFileDownloader>>) -> BuckyResult<Self> {
        let owner_id = stack.local_device().desc().owner().as_ref().unwrap().clone();
        let miner = Self {
            stack,
            owner_id,
            miner
        };
        let _ = miner.listen().await?;
        Ok(miner)
    }

    async fn listen(&self) -> BuckyResult<()> {
        struct OnChallenge {
            miner: OodMiner
        }

        #[async_trait::async_trait]
        impl EventListenerAsyncRoutine<RouterHandlerPostObjectRequest, RouterHandlerPostObjectResult> for OnChallenge {
            async fn call(&self, param: &RouterHandlerPostObjectRequest) -> BuckyResult<RouterHandlerPostObjectResult> {
                log::info!("OnChallenge, id={}, from={}", param.request.object.object_id, param.request.common.source);
                let challenge = DsgChallengeObject::clone_from_slice(param.request.object.object_raw.as_slice())
                    .map_err(|err| {
                        log::info!("OnChallenge failed, id={}, from={}, err=decode challenge {}", param.request.object.object_id, param.request.common.source, err);
                        err
                    })?;
                let _ = self.miner.miner.on_challenge(challenge, param.request.common.source.zone.device.as_ref().unwrap().object_id().clone()).await
                    .map_err(|err| {
                        log::info!("OnChallenge failed, id={}, from={}, err=delegate {}", param.request.object.object_id, param.request.common.source, err);
                        err
                    })?;
                Ok(RouterHandlerPostObjectResult {
                    action: RouterHandlerAction::Response,
                    request: None,
                    response: Some(Ok(NONPostObjectInputResponse {
                        object: None
                    }))
                })
            }
        }

        let req_path = RequestGlobalStatePath::new(Some(dsg_dec_id()), Some("/dmc/dsg/miner/")).format_string();
        info!("miner req path: {}", &req_path);

        self.stack.root_state_meta_stub(None, None).add_access(GlobalStatePathAccessItem {
            path: req_path.clone(),
            access: GlobalStatePathGroupAccess::Default(AccessString::full().value()),
        }).await?;

        let _ = self.stack.router_handlers().add_handler(
            RouterHandlerChain::Handler,
            "OnChallenge",
            0,
            None,
            Some(req_path.clone()),
            RouterHandlerAction::Default,
            Some(Box::new(OnChallenge {miner: self.clone()}))
        )?;

        struct OnCommand {
            miner: OodMiner
        }

        #[async_trait::async_trait]
        impl EventListenerAsyncRoutine<RouterHandlerPostObjectRequest, RouterHandlerPostObjectResult> for OnCommand{
            async fn call(&self, param: &RouterHandlerPostObjectRequest) -> BuckyResult<RouterHandlerPostObjectResult> {
                log::info!("OnCommand, id={}, from={}", param.request.object.object_id, param.request.common.source);
                let req = JSONObject::clone_from_slice(param.request.object.object_raw.as_slice())?;
                let ret = if req.get_json_obj_type() == 10000 {
                    self.miner.on_get_order_info(req.get()?).await?
                } else {
                    None
                };
                if ret.is_none() {
                    Ok(RouterHandlerPostObjectResult {
                        action: RouterHandlerAction::Response,
                        request: None,
                        response: Some(Ok(NONPostObjectInputResponse {
                            object: None
                        }))
                    })
                } else {
                    Ok(RouterHandlerPostObjectResult {
                        action: RouterHandlerAction::Response,
                        request: None,
                        response: Some(Ok(NONPostObjectInputResponse {
                            object: Some(NONObjectInfo {
                                object_id: ret.as_ref().unwrap().desc().calculate_id(),
                                object_raw: ret.as_ref().unwrap().to_vec()?,
                                object: None
                            })
                        }))
                    })
                }
            }
        }

        self.stack.root_state_meta_stub(None, None).add_access(GlobalStatePathAccessItem {
            path: "dmc_dsg_commands".to_string(),
            access: GlobalStatePathGroupAccess::Default(AccessString::full().value()),
        }).await?;

        let _ = self.stack.router_handlers().add_handler(
            RouterHandlerChain::Handler,
            "OnCommand",
            0,
            None,
            Some("dmc_dsg_commands".to_string()),
            RouterHandlerAction::Default,
            Some(Box::new(OnCommand {
                miner: self.clone()
            }))
        )?;

        Ok(())
    }

    async fn on_get_order_info(&self, order: String) -> BuckyResult<Option<JSONObject>> {
        let (contract_id, state_id) = self.miner.get_order_info(order.as_str()).await?;
        Ok(Some(JSONObject::new(
            dsg_dec_id(),
            self.owner_id.clone(),
            10001,
            &(contract_id.to_string(), state_id.to_string())
        )?))
    }
}
