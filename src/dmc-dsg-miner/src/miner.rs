use std::{
    sync::Arc,
};
use async_trait::async_trait;
use cyfs_base::*;
use cyfs_bdt::*;
use cyfs_lib::*;
use cyfs_util::*;
use cyfs_dsg_client::*;
use crate::*;


struct InterfaceImpl {
    stack: Arc<SharedCyfsStack>
}

#[derive(Clone)]
pub struct DsgMinerInterface {
    inner: Arc<InterfaceImpl>
}

impl DsgMinerInterface {
    pub fn new(stack: Arc<SharedCyfsStack>) -> Self {
        Self {
            inner: Arc::new(InterfaceImpl {
                stack,
            })
        }
    }

    pub fn stack(&self) -> &SharedCyfsStack {
        self.inner.stack.as_ref()
    }

    pub fn chunk_reader(&self) -> Box<dyn ChunkReader> {
        DsgStackChunkReader::new(self.inner.stack.clone()).clone_as_reader()
    }

    pub async fn verify_proof<'a>(&self, proof: DsgProofObjectRef<'a>, to: DeviceId) -> BuckyResult<DsgProofObject> {
        log::info!("DsgMiner will request sign for proof, proof={}, to={}", proof, to);
        let mut req = NONPostObjectOutputRequest::new(
            NONAPILevel::default(),
            proof.id(),
            proof.as_ref().to_vec().unwrap());
        req.common.target = Some(to.object_id().clone());
        let path = RequestGlobalStatePath {
            global_state_category: None,
            global_state_root: None,
            dec_id: Some(dsg_dec_id()),
            req_path: Some("/dsg/service/proof/".to_string())
        };
        req.common.req_path = Some(path.to_string());
        let resp = self.stack().non_service().post_object(req).await
            .map_err(|err| {
                log::error!("DsgMiner will request sign for proof failed, proof={}, to={}, err=post object {}", proof.id(), to, err);
                err
            })?;

        if let Some(object_raw) = resp.object.as_ref().map(|o| o.object_raw.as_slice()) {
            let signed_proof = DsgProofObject::clone_from_slice(object_raw)
                .map_err(|err| {
                    log::error!("DsgMiner request sign for proof failed, proof={}, to={}, err=decode resp {}", proof.id(), to, err);
                    err
                })?;

            //FIXME: verify sign
            log::info!("DsgMiner request sign for proof success, proof={}, to={}", proof.id(), to);
            Ok(signed_proof)
        } else {
            let err = BuckyError::new(BuckyErrorCode::InvalidData, "consumer return no object");
            log::error!("DsgMiner request sign for proof failed, proof={}, to={}, err=decode resp {}", proof.id(), to, err);
            Err(err)
        }

    }

    pub async fn get_object_from_consumer<O: for <'de> RawDecode<'de>>(&self, _id: ObjectId) -> BuckyResult<O> {
        unimplemented!()
    }

    pub async fn get_object_from_noc<O: for <'de> RawDecode<'de>>(&self, id: ObjectId) -> BuckyResult<O> {
        let resp = self.stack().non_service().get_object(NONGetObjectOutputRequest::new(NONAPILevel::NOC, id, None)).await?;
        O::clone_from_slice(resp.object.object_raw.as_slice())
    }

    pub async fn put_object_to_noc<O: RawEncode>(&self, id: ObjectId, object: &O) -> BuckyResult<()> {
        let _ = self.stack().non_service().put_object(NONPutObjectOutputRequest::new(NONAPILevel::NOC, id, object.to_vec()?)).await?;
        Ok(())
    }
}


#[async_trait]
pub trait DsgMinerDelegate: Send + Sync {
    async fn on_challenge(&self, interface: &DsgMinerInterface, challenge: DsgChallengeObject, from: DeviceId) -> BuckyResult<()>;
}


struct MinerImpl<D>
    where D: 'static + DsgMinerDelegate {
    interface: DsgMinerInterface,
    delegate: D,
}


pub struct OodMiner<D>
    where D: 'static + DsgMinerDelegate {
    inner: Arc<MinerImpl<D>>
}


impl<D> Clone for OodMiner<D>
    where D: 'static + DsgMinerDelegate {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

impl<D> std::fmt::Display for OodMiner<D>
    where D: 'static + DsgMinerDelegate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DsgMiner")
    }
}



impl<D> OodMiner<D>
    where D: 'static + DsgMinerDelegate {
    pub async fn new(stack: Arc<SharedCyfsStack>, delegate: D) -> BuckyResult<Self> {
        let miner = Self {
            inner: Arc::new(MinerImpl {
                interface: DsgMinerInterface::new(stack),
                delegate
            })
        };
        let _ = miner.listen().await?;
        Ok(miner)
    }

    fn interface(&self) -> &DsgMinerInterface {
        &self.inner.interface
    }

    fn delegate(&self) -> &D {
        &self.inner.delegate
    }

    async fn listen(&self) -> BuckyResult<()> {
        struct OnChallenge<D>
            where D: 'static + DsgMinerDelegate {
            miner: OodMiner<D>
        }

        #[async_trait::async_trait]
        impl<D> EventListenerAsyncRoutine<RouterHandlerPostObjectRequest, RouterHandlerPostObjectResult> for OnChallenge<D>
            where D: 'static + DsgMinerDelegate {
            async fn call(&self, param: &RouterHandlerPostObjectRequest) -> BuckyResult<RouterHandlerPostObjectResult> {
                log::info!("{} OnChallenge, id={}, from={}", self.miner, param.request.object.object_id, param.request.common.source);
                let challenge = DsgChallengeObject::clone_from_slice(param.request.object.object_raw.as_slice())
                    .map_err(|err| {
                        log::info!("{} OnChallenge failed, id={}, from={}, err=decode challenge {}", self.miner, param.request.object.object_id, param.request.common.source, err);
                        err
                    })?;

                let _ = self.miner.delegate().on_challenge(self.miner.interface(), challenge, param.request.common.source.zone.device.clone().unwrap()).await
                    .map_err(|err| {
                        log::info!("{} OnChallenge failed, id={}, from={}, err=delegate {}", self.miner, param.request.object.object_id, param.request.common.source, err);
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

        self.interface().stack().root_state_meta_stub(None, None).add_access(GlobalStatePathAccessItem {
            path: req_path.clone(),
            access: GlobalStatePathGroupAccess::Default(AccessString::full().value()),
        }).await?;

        let _ = self.interface().stack().router_handlers().add_handler(
            RouterHandlerChain::Handler,
            "OnChallenge",
            0,
            None,
            Some(req_path.clone()),
            RouterHandlerAction::Default,
            Some(Box::new(OnChallenge {miner: self.clone()}))
        )?;

        Ok(())
    }
}
