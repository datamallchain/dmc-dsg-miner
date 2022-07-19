use cyfs_lib::*;
use std::ops::{Deref};
use cyfs_base::*;
use async_trait::async_trait;
use std::sync::{Arc, Weak, Mutex};
use crate::*;
use async_std::future::Future;
use cyfs_util::EventListenerAsyncRoutine;
use crate::ArcWeakHelper;

pub enum SharedCyfsStackExEndpointResult {
    Pass,
    Accepted((ObjectId, Vec<u8>))
}

#[async_trait]
pub trait SharedCyfsStackExEndpoint: Send + Sync + 'static {
    async fn call(&self, param: &RouterHandlerPostObjectRequest) -> BuckyResult<SharedCyfsStackExEndpointResult>;
}

#[async_trait]
impl<F, Fut> SharedCyfsStackExEndpoint for F
    where
        F: Send + Sync + 'static + Fn(&RouterHandlerPostObjectRequest) -> Fut,
        Fut: Send + 'static + Future<Output = BuckyResult<SharedCyfsStackExEndpointResult>>,
{
    async fn call(&self, param: &RouterHandlerPostObjectRequest) -> BuckyResult<SharedCyfsStackExEndpointResult> {
        let fut = (self)(param);
        fut.await
    }
}

pub struct SharedCyfsStackServer {
    stack: Arc<SharedCyfsStack>,
    name: String,
    ep: Mutex<Option<Arc<dyn SharedCyfsStackExEndpoint>>>,
    filter_dec_id: Vec<ObjectId>,
    dec_id: ObjectId,
}
pub type SharedCyfsStackServerRef = Arc<SharedCyfsStackServer>;
pub type SharedCyfsStackServerWeakRef = Weak<SharedCyfsStackServer>;

impl Deref for SharedCyfsStackServer {
    type Target = Arc<SharedCyfsStack>;

    fn deref(&self) -> &Self::Target {
        &self.stack
    }
}

struct OnPutHandle {
    stackex: SharedCyfsStackServerWeakRef
}

#[async_trait]
impl EventListenerAsyncRoutine<RouterHandlerPostObjectRequest, RouterHandlerPostObjectResult> for OnPutHandle {
    async fn call(&self, param: &RouterHandlerPostObjectRequest) -> BuckyResult<RouterHandlerPostObjectResult> {
        self.stackex.to_rc()?.on_recv_obj(param).await
    }
}

impl SharedCyfsStackServer {
    pub fn new(name: String, stack: Arc<SharedCyfsStack>, dec_id: ObjectId, filter_dec_id: Vec<ObjectId>) -> SharedCyfsStackServerRef {
        SharedCyfsStackServerRef::new(Self {
            stack,
            name,
            ep: Mutex::new(None),
            filter_dec_id,
            dec_id
        })
    }

    pub fn get_stack(&self) -> &Arc<SharedCyfsStack> {
        &self.stack
    }

    pub fn set_end_point(&self, ep: impl SharedCyfsStackExEndpoint) {
        let mut self_ep = self.ep.lock().unwrap();
        *self_ep = Some(Arc::new(ep))
    }

    pub async fn listen(self: &SharedCyfsStackServerRef) -> BuckyResult<()> {
        for filter_dec_id in &self.filter_dec_id {
            let listener = OnPutHandle {
                stackex: SharedCyfsStackServerRef::downgrade(self)
            };

            let filter = format!("dec_id == {}", filter_dec_id);
            self.stack.router_handlers().add_handler(RouterHandlerChain::Handler,
                                                     (self.name.clone() + filter_dec_id.to_string().as_str()).as_str(),
                                                     0,
                                                     filter.as_str(),
                                                     RouterHandlerAction::Default,
                                                     Some(Box::new(listener)))?;
        }
        Ok(())
    }

    pub async fn stop(&self) -> BuckyResult<bool> {
        for filter_dec_id in &self.filter_dec_id {
            self.stack.router_handlers().remove_handler(RouterHandlerChain::PreRouter,
                                                        RouterHandlerCategory::PostObject,
                                                        (self.name.clone() + filter_dec_id.to_string().as_str()).as_str()).await?;
        }

        Ok(true)
    }

    pub(crate) async fn on_recv_obj(self: &SharedCyfsStackServerRef, param: &RouterHandlerPostObjectRequest) -> BuckyResult<RouterHandlerPostObjectResult> {
        let ep = {
            let ep = self.ep.lock().unwrap();
            if ep.is_some() {
                Some(ep.as_ref().unwrap().clone())
            } else {
                None
            }
        };

        if ep.is_some() {
            match ep.unwrap().call(param).await {
                Ok(ret) => {
                    match ret {
                        SharedCyfsStackExEndpointResult::Accepted((object_id, object_raw)) => {
                            match self.sign_object(object_id.clone(), object_raw).await {
                                Ok(object_raw) => {
                                    Ok(RouterHandlerPostObjectResult {
                                        action: RouterHandlerAction::Response,
                                        request: None,
                                        response: Some(Ok(NONPostObjectInputResponse{
                                            object: Some(NONObjectInfo {
                                                object_id,
                                                object_raw,
                                                object: None
                                            })
                                        })),
                                    })
                                },
                                Err(e) => {
                                    Ok(RouterHandlerPostObjectResult {
                                        action: RouterHandlerAction::Response,
                                        request: None,
                                        response: Some(Err(e))
                                    })
                                }
                            }
                        },
                        SharedCyfsStackExEndpointResult::Pass => {
                            Ok(
                                RouterHandlerPostObjectResult {
                                    action: RouterHandlerAction::Pass,
                                    request: None,
                                    response: None,
                                })
                        }
                    }
                },
                Err(e) => {
                    log::error!("handle err {}", &e);
                    Ok(RouterHandlerPostObjectResult {
                        action: RouterHandlerAction::Response,
                        request: None,
                        response: Some(Err(e))
                    })
                }
            }
        } else {
            Ok(
                RouterHandlerPostObjectResult {
                    action: RouterHandlerAction::Pass,
                    request: None,
                    response: None,
                })
        }
    }

    async fn sign_object(&self, object_id: ObjectId, object_raw: Vec<u8>) -> BuckyResult<Vec<u8>> {
        let flags = CRYPTO_REQUEST_FLAG_SIGN_BY_DEVICE | CRYPTO_REQUEST_FLAG_SIGN_PUSH_DESC;
        let resp = self.stack.crypto().sign_object(CryptoSignObjectRequest {
            common: CryptoOutputRequestCommon {
                req_path: None,
                dec_id: Some(self.dec_id.clone()),
                target: None,
                flags
            },
            flags,
            object: NONObjectInfo {
                object_id,
                object_raw,
                object: None
            }
        }).await?;

        Ok(resp.object.unwrap().object_raw)
    }
}

pub struct SharedCyfsStackClient {
    stack: Arc<SharedCyfsStack>,
    dec_id: Option<ObjectId>,
}
pub type SharedCyfsStackClientRef = Arc<SharedCyfsStackClient>;
pub type SharedCyfsStackClientWeakRef = Weak<SharedCyfsStackClient>;

impl Deref for SharedCyfsStackClient {
    type Target = Arc<SharedCyfsStack>;

    fn deref(&self) -> &Self::Target {
        &self.stack
    }
}

impl SharedCyfsStackClient {
    pub fn new(stack: Arc<SharedCyfsStack>, dec_id: Option<ObjectId>) -> SharedCyfsStackClient {
        Self {
            stack,
            dec_id
        }
    }

    pub fn get_stack(&self) -> &Arc<SharedCyfsStack> {
        &self.stack
    }

    pub async fn sign_object(&self, object_id: ObjectId, object_raw: Vec<u8>) -> BuckyResult<Vec<u8>> {
        let flags = CRYPTO_REQUEST_FLAG_SIGN_BY_DEVICE | CRYPTO_REQUEST_FLAG_SIGN_PUSH_DESC;
        let resp = self.stack.crypto().sign_object(CryptoSignObjectRequest {
            common: CryptoOutputRequestCommon {
                req_path: None,
                dec_id: self.dec_id.clone(),
                target: None,
                flags
            },
            flags,
            object: NONObjectInfo {
                object_id,
                object_raw,
                object: None
            }
        }).await?;

        Ok(resp.object.unwrap().object_raw)
    }

    pub async fn get_object<T: for <'a> RawDecode<'a>>(&self, target: Option<ObjectId>, object_id: ObjectId) -> BuckyResult<T> {
        let resp = self.stack.non_service().get_object(NONGetObjectOutputRequest {
            common: NONOutputRequestCommon {
                req_path: None,
                dec_id: self.dec_id.clone(),
                level: if target.is_none() {NONAPILevel::NOC} else {NONAPILevel::Router},
                target,
                flags: 0
            },
            object_id,
            inner_path: None
        }).await?;

        T::clone_from_slice(resp.object.object_raw.as_slice())
    }

    pub async fn put_object(&self, target: ObjectId, object_id: ObjectId, object_raw: Vec<u8>) -> BuckyResult<()> {
        log::info!("put_object_with_resp target={} object_id={}", target.to_string(), object_id.to_string());
        let object_raw = self.sign_object(object_id.clone(), object_raw).await?;
        let _ = self.stack.non_service().post_object(NONPostObjectOutputRequest {
            common: NONOutputRequestCommon {
                req_path: None,
                dec_id: self.dec_id.clone(),
                level: NONAPILevel::Router,
                target: Some(target),
                flags: 0
            },
            object: NONObjectInfo {
                object_id,
                object_raw,
                object: None
            }
        }).await?;

        Ok(())
    }

    pub async fn put_object_with_resp(&self, target: ObjectId, object_id: ObjectId, object_raw: Vec<u8>, _timeout: u64) -> BuckyResult<Vec<u8>> {
        app_call_log!("put_object_with_resp target={} object_id={}", target.to_string(), object_id.to_string());
        let object_raw = self.sign_object(object_id.clone(), object_raw).await?;
        let resp = self.stack.non_service().post_object(NONPostObjectOutputRequest {
            common: NONOutputRequestCommon {
                req_path: None,
                dec_id: self.dec_id.clone(),
                level: NONAPILevel::Router,
                target: Some(target),
                flags: 0
            },
            object: NONObjectInfo {
                object_id,
                object_raw,
                object: None
            }
        }).await?;

        if resp.object.is_none() {
            Err(cyfs_err!(BuckyErrorCode::InvalidData, "resp data is none"))
        } else {
            let object_raw = resp.object.unwrap().object_raw;
            Ok(object_raw)
        }
    }

    pub async fn put_object_with_resp2<T: RawEncode + for <'a> RawDecode<'a>>(&self, target: ObjectId, object_id: ObjectId, object_raw: Vec<u8>, _timeout: u64) -> BuckyResult<T> {
        app_call_log!("put_object_with_resp2 target={} object_id={}", target.to_string(), object_id.to_string());
        let object_raw = self.sign_object(object_id.clone(), object_raw).await?;
        let resp = self.stack.non_service().post_object(NONPostObjectOutputRequest {
            common: NONOutputRequestCommon {
                req_path: None,
                dec_id: self.dec_id.clone(),
                level: NONAPILevel::Router,
                target: Some(target),
                flags: 0
            },
            object: NONObjectInfo {
                object_id,
                object_raw,
                object: None
            }
        }).await?;

        if resp.object.is_none() {
            Err(cyfs_err!(BuckyErrorCode::InvalidData, "resp data is none"))
        } else {
            let object_raw = resp.object.unwrap().object_raw;
            Ok(T::clone_from_slice(object_raw.as_slice())?)
        }
    }
}

pub type SharedCyfsStackRef = Arc<SharedCyfsStack>;

#[async_trait::async_trait]
pub trait SharedCyfsStackEx {
    async fn sign_object(&self, object_id: ObjectId, object_raw: Vec<u8>) -> BuckyResult<Vec<u8>>;
    async fn sign_object2<T: ObjectType + Sync + Send, O: for <'a> RawDecode<'a>>(&self, obj: &NamedObjectBase<T>) -> BuckyResult<O>
        where <T as cyfs_base::ObjectType>::ContentType: cyfs_base::BodyContent + cyfs_base::RawEncode,
              <T as cyfs_base::ObjectType>::DescType: RawEncodeWithContext<cyfs_base::NamedObjectContext>;
    async fn resolve_ood(&self, object_id: ObjectId) -> BuckyResult<ObjectId>;
    async fn get_object_from_noc<T: for <'a> RawDecode<'a>>(&self, object_id: ObjectId) -> BuckyResult<T>;
    async fn put_object_to_noc<T: ObjectType + Sync + Send>(&self, obj: &NamedObjectBase<T>) -> BuckyResult<ObjectId>
        where <T as cyfs_base::ObjectType>::ContentType: cyfs_base::BodyContent + cyfs_base::RawEncode,
              <T as cyfs_base::ObjectType>::DescType: RawEncodeWithContext<cyfs_base::NamedObjectContext>;
    async fn get_object<T: for <'a> RawDecode<'a>>(
        &self,
        target: Option<ObjectId>,
        object_id: ObjectId
    ) -> BuckyResult<T>;
    async fn put_object_with_resp(
        &self,
        target: ObjectId,
        object_id: ObjectId,
        object_raw: Vec<u8>
    ) -> BuckyResult<Vec<u8>>;
    async fn put_object_with_resp2<T: RawEncode + for <'a> RawDecode<'a>>(
        &self,
        target: ObjectId,
        object_id: ObjectId,
        object_raw: Vec<u8>
    ) -> BuckyResult<T>;
}

#[async_trait::async_trait]
impl SharedCyfsStackEx for SharedCyfsStack {
    async fn sign_object(&self, object_id: ObjectId, object_raw: Vec<u8>) -> BuckyResult<Vec<u8>> {
        let flags = CRYPTO_REQUEST_FLAG_SIGN_BY_DEVICE | CRYPTO_REQUEST_FLAG_SIGN_PUSH_DESC;
        let resp = self.crypto().sign_object(CryptoSignObjectRequest {
            common: CryptoOutputRequestCommon {
                req_path: None,
                dec_id: None,
                target: None,
                flags
            },
            flags,
            object: NONObjectInfo {
                object_id,
                object_raw,
                object: None
            }
        }).await?;

        Ok(resp.object.unwrap().object_raw)
    }

    async fn sign_object2<T: ObjectType + Sync + Send, O: for<'a> RawDecode<'a>>(&self, obj: &NamedObjectBase<T>) -> BuckyResult<O>
        where <T as ObjectType>::ContentType: BodyContent + RawEncode, <T as ObjectType>::DescType: RawEncodeWithContext<NamedObjectContext> {
        let object_id = obj.desc().calculate_id();
        let signed = self.sign_object(object_id, obj.to_vec()?).await?;
        O::clone_from_slice(signed.as_slice())
    }

    async fn resolve_ood(&self, object_id: ObjectId) -> BuckyResult<ObjectId> {
        let resp = self.util().resolve_ood(UtilResolveOODRequest {
            common: UtilOutputRequestCommon {
                req_path: None,
                dec_id: None,
                target: None,
                flags: 0
            },
            object_id,
            owner_id: None
        }).await?;

        let ood_id = resp.device_list[0].object_id().clone();
        Ok(ood_id)
    }

    async fn get_object_from_noc<T: for<'a> RawDecode<'a>>(&self, object_id: ObjectId) -> BuckyResult<T> {
        self.get_object(None, object_id).await
    }

    async fn put_object_to_noc<T: ObjectType + Sync + Send>(&self, obj: &NamedObjectBase<T>) -> BuckyResult<ObjectId>
        where <T as cyfs_base::ObjectType>::ContentType: cyfs_base::BodyContent + cyfs_base::RawEncode,
              <T as cyfs_base::ObjectType>::DescType: RawEncodeWithContext<cyfs_base::NamedObjectContext> {
        let object_id = obj.desc().calculate_id();
        let object_raw = obj.to_vec()?;
        self.non_service().put_object(NONPutObjectOutputRequest { common: NONOutputRequestCommon {
            req_path: None,
            dec_id: None,
            level: NONAPILevel::NOC,
            target: None,
            flags: 0
        }, object: NONObjectInfo {
            object_id: object_id.clone(),
            object_raw,
            object: None
        } }).await?;

        Ok(object_id)
    }

    async fn get_object<T: for <'a> RawDecode<'a>>(&self, target: Option<ObjectId>, object_id: ObjectId) -> BuckyResult<T> {
        let resp = self.non_service().get_object(NONGetObjectOutputRequest {
            common: NONOutputRequestCommon {
                req_path: None,
                dec_id: None,
                level: if target.is_none() {NONAPILevel::NOC} else {NONAPILevel::Router},
                target,
                flags: 0
            },
            object_id,
            inner_path: None
        }).await?;

        T::clone_from_slice(resp.object.object_raw.as_slice())
    }

    async fn put_object_with_resp(&self, target: ObjectId, object_id: ObjectId, object_raw: Vec<u8>) -> BuckyResult<Vec<u8>> {
        app_call_log!("put_object_with_resp target={} object_id={}", target.to_string(), object_id.to_string());
        let object_raw = self.sign_object(object_id.clone(), object_raw).await?;
        let resp = self.non_service().post_object(NONPostObjectOutputRequest {
            common: NONOutputRequestCommon {
                req_path: None,
                dec_id: None,
                level: NONAPILevel::Router,
                target: Some(target),
                flags: 0
            },
            object: NONObjectInfo {
                object_id,
                object_raw,
                object: None
            }
        }).await?;

        if resp.object.is_none() {
            Err(cyfs_err!(BuckyErrorCode::InvalidData, "resp data is none"))
        } else {
            let object_raw = resp.object.unwrap().object_raw;
            Ok(object_raw)
        }
    }

    async fn put_object_with_resp2<T: RawEncode + for <'a> RawDecode<'a>>(&self, target: ObjectId, object_id: ObjectId, object_raw: Vec<u8>) -> BuckyResult<T> {
        app_call_log!("put_object_with_resp2 target={} object_id={}", target.to_string(), object_id.to_string());
        let object_raw = self.sign_object(object_id.clone(), object_raw).await?;
        let resp = self.non_service().post_object(NONPostObjectOutputRequest {
            common: NONOutputRequestCommon {
                req_path: None,
                dec_id: None,
                level: NONAPILevel::Router,
                target: Some(target),
                flags: 0
            },
            object: NONObjectInfo {
                object_id,
                object_raw,
                object: None
            }
        }).await?;

        if resp.object.is_none() {
            Err(cyfs_err!(BuckyErrorCode::InvalidData, "resp data is none"))
        } else {
            let object_raw = resp.object.unwrap().object_raw;
            Ok(T::clone_from_slice(object_raw.as_slice())?)
        }
    }
}
