use std::{
    sync::Arc,
};
use std::str::FromStr;
use cyfs_base::*;
use cyfs_lib::*;
use cyfs_util::*;
use cyfs_dsg_client::*;
use crate::*;
use dmc_dsg_base::*;
use dmc_dsg_base::DSGJSON;
use dmc_dsg_base::DSGJSONType;

type Miner = DmcDsgMiner<SharedCyfsStack, CyfsStackMetaConnection, NocChunkStore, CyfsStackFileDownloader, RemoteDMCTxSender<SharedCyfsStack>>;

#[derive(Clone)]
pub struct OodMiner {
    stack: Arc<SharedCyfsStack>,
    owner_id: ObjectId,
    miner: Arc<Miner>
}


impl OodMiner {
    pub async fn new(stack: Arc<SharedCyfsStack>, miner: Arc<Miner>) -> BuckyResult<Self> {
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
                let ret: BuckyResult<()> = async move {
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
                    Ok(())
                }.await;
                match ret {
                    Ok(_) => {
                        Ok(RouterHandlerPostObjectResult {
                            action: RouterHandlerAction::Response,
                            request: None,
                            response: Some(Ok(NONPostObjectInputResponse {
                                object: None
                            }))
                        })
                    }
                    Err(e) => {
                        log::error!("handle err {}", &e);
                        Ok(RouterHandlerPostObjectResult {
                            action: RouterHandlerAction::Response,
                            request: None,
                            response: Some(Err(e))
                        })
                    }
                }
            }
        }

        self.stack.root_state_meta_stub(None, None).add_access(GlobalStatePathAccessItem {
            path: "/dmc/dsg/miner/".to_string(),
            access: GlobalStatePathGroupAccess::Default(AccessString::full().value()),
        }).await?;

        let _ = self.stack.router_handlers().add_handler(
            RouterHandlerChain::Handler,
            "OnChallenge",
            0,
            None,
            Some("/dmc/dsg/miner/".to_string()),
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
                let ret: BuckyResult<Option<JSONObject>> = async move {
                    let req = JSONObject::clone_from_slice(param.request.object.object_raw.as_slice())?;
                    let ret = if req.get_json_obj_type() == 10000 {
                        self.miner.on_get_order_info(req.get()?).await?
                    } else if req.get_json_obj_type() == 10002 {
                        self.miner.on_get_chunk_merkle_hash(req.get()?).await?
                    } else {
                        None
                    };
                    Ok(ret)
                }.await;
                match ret {
                    Ok(ret) => {
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
                    Err(e) => {
                        log::error!("handle err {}", &e);
                        Ok(RouterHandlerPostObjectResult {
                            action: RouterHandlerAction::Response,
                            request: None,
                            response: Some(Err(e))
                        })
                    }
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

    async fn on_get_chunk_merkle_hash(&self, req: GetChunkMerkleHashReq) -> BuckyResult<Option<JSONObject>> {
        let mut chunk_list = Vec::new();
        for chunk_id in req.chunk_list.iter() {
            chunk_list.push(ChunkId::from_str(chunk_id)?);
        }
        let hash_list = self.miner.get_chunk_merkle_hash(chunk_list, req.chunk_size).await?;
        let list: Vec<String> = hash_list.iter().map(|v| v.to_string()).collect();
        Ok(Some(JSONObject::new(
            dsg_dec_id(),
            self.owner_id.clone(),
            10003,
            &list
        )?))
    }

    pub fn get_dmc_miner(&self) -> &Arc<Miner> {
        &self.miner
    }
}
