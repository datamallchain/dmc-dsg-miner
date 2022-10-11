use cyfs_lib::*;
use std::ops::{Deref};
use cyfs_base::*;
use async_trait::async_trait;
use std::sync::{Arc, Weak, Mutex};
use crate::*;
use async_std::future::Future;
use crate::ArcWeakHelper;
use cyfs_util::{EventListenerAsyncRoutine};

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
    req_path: String,
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
    pub fn new(name: String, stack: Arc<SharedCyfsStack>, req_path: String) -> SharedCyfsStackServerRef {
        SharedCyfsStackServerRef::new(Self {
            stack,
            name,
            ep: Mutex::new(None),
            req_path,
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
        let listener = OnPutHandle {
            stackex: SharedCyfsStackServerRef::downgrade(self)
        };

        self.stack.root_state_meta_stub(None, None).add_access(GlobalStatePathAccessItem {
            path: self.req_path.clone(),
            access: GlobalStatePathGroupAccess::Default(AccessString::full().value())
        }).await?;

        self.stack.router_handlers().add_handler(RouterHandlerChain::Handler,
                                                 self.name.clone().as_str(),
                                                 0,
                                                 None,
                                                 Some(self.req_path.clone()),
                                                 RouterHandlerAction::Default,
                                                 Some(Box::new(listener)))?;
        Ok(())
    }

    pub async fn stop(&self) -> BuckyResult<bool> {
        self.stack.router_handlers().remove_handler(RouterHandlerChain::Handler,
                                                    RouterHandlerCategory::PostObject,
                                                    self.name.as_str()).await?;

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
                            Ok(RouterHandlerPostObjectResult {
                                action: RouterHandlerAction::Response,
                                request: None,
                                response: Some(Ok(NONPostObjectInputResponse {
                                    object: Some(NONObjectInfo {
                                        object_id,
                                        object_raw,
                                        object: None,
                                    })
                                })),
                            })
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

    // pub async fn verify_object(&self, object_id: ObjectId, object_raw: Vec<u8>) -> BuckyResult<bool> {
    //     let req = NONVerifyBySignRequest {
    //         target: None,
    //         object_id,
    //         object_raw,
    //         desc_signs: None,
    //         body_signs: None
    //     };
    // }
}

pub struct CyfsPath {
    pub target: ObjectId,
    pub target_dec_id: ObjectId,
    pub req_path: String,
}

impl CyfsPath {
    pub fn new(target: ObjectId, target_dec_id: ObjectId, req_path: &str) -> Self {
        Self {
            target,
            target_dec_id,
            req_path: req_path.to_string()
        }
    }

    pub fn to_path(&self) -> String {
        format!("/{}/{}/{}", self.target.to_string(), self.target_dec_id.to_string(), self.req_path)
    }

    pub fn parse(path: &str) -> BuckyResult<Self> {
        if !path.starts_with("/") {
            return Err(cyfs_err!(BuckyErrorCode::InvalidFormat, "parse {} err", path));
        }
        let path_ref = &path[1..];
        let pos = path_ref.find("/");
        if pos.is_none() {
            return Err(cyfs_err!(BuckyErrorCode::InvalidFormat, "parse {} err", path));
        }
        let target = &path_ref[..pos.unwrap()];

        let path_ref = &path_ref[pos.unwrap() + 1..];
        let pos = path_ref.find("/");
        if pos.is_none() {
            return Err(cyfs_err!(BuckyErrorCode::InvalidFormat, "parse {} err", path));
        }
        let target_dec_id = &path_ref[..pos.unwrap()];

        let req_path = path_ref[pos.unwrap() + 1..].to_string();
        Ok(Self {
            target: ObjectId::from_base58(target)?,
            target_dec_id: ObjectId::from_base58(target_dec_id)?,
            req_path
        })
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
                source: None,
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

    pub async fn put_object_with_resp(&self, req_path: &str, object_id: ObjectId, object_raw: Vec<u8>) -> BuckyResult<Vec<u8>> {
        app_call_log!("put_object_with_resp req_path={}", req_path);
        let cyfs_path = CyfsPath::parse(req_path)?;
        let path = RequestGlobalStatePath {
            global_state_category: None,
            global_state_root: None,
            dec_id: Some(cyfs_path.target_dec_id),
            req_path: Some(cyfs_path.req_path)
        };

        // let object_raw = self.sign_object(object_id.clone(), object_raw).await?;
        let resp = self.stack.non_service().post_object(NONPostObjectOutputRequest {
            common: NONOutputRequestCommon {
                req_path: Some(path.to_string()),
                source: None,
                dec_id: None,
                level: NONAPILevel::Router,
                target: Some(cyfs_path.target),
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

    pub async fn put_object_with_resp2<T: RawEncode + for <'a> RawDecode<'a>>(&self, req_path: &str, object_id: ObjectId, object_raw: Vec<u8>) -> BuckyResult<T> {
        app_call_log!("put_object_with_resp2 req_path={}", req_path);
        let cyfs_path = CyfsPath::parse(req_path)?;
        let path = RequestGlobalStatePath {
            global_state_category: None,
            global_state_root: None,
            dec_id: Some(cyfs_path.target_dec_id),
            req_path: Some(cyfs_path.req_path)
        };

        let resp = self.stack.non_service().post_object(NONPostObjectOutputRequest {
            common: NONOutputRequestCommon {
                req_path: Some(path.to_string()),
                source: None,
                dec_id: None,
                level: NONAPILevel::Router,
                target: Some(cyfs_path.target),
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
pub trait SharedCyfsStackEx: 'static + Sync + Send {
    async fn sign_object(&self, object_id: ObjectId, object_raw: Vec<u8>) -> BuckyResult<Vec<u8>>;
    async fn sign_object2<T: ObjectType + Sync + Send, O: for <'a> RawDecode<'a>>(&self, obj: &NamedObjectBase<T>) -> BuckyResult<O>
        where <T as cyfs_base::ObjectType>::ContentType: cyfs_base::BodyContent + cyfs_base::RawEncode,
              <T as cyfs_base::ObjectType>::DescType: RawEncodeWithContext<cyfs_base::NamedObjectContext>;
    async fn resolve_ood(&self, object_id: ObjectId) -> BuckyResult<ObjectId>;
    async fn resolve_ood_list(&self, object_id: ObjectId) -> BuckyResult<Vec<DeviceId>>;
    async fn get_object_from_local<T: for <'a> RawDecode<'a>>(&self, object_id: ObjectId) -> BuckyResult<T>;
    async fn put_object_to_local<T: ObjectType + Sync + Send>(&self, obj: &NamedObjectBase<T>) -> BuckyResult<ObjectId>
        where <T as cyfs_base::ObjectType>::ContentType: cyfs_base::BodyContent + cyfs_base::RawEncode,
              <T as cyfs_base::ObjectType>::DescType: RawEncodeWithContext<cyfs_base::NamedObjectContext>;
    async fn pub_object<T: ObjectType + Sync + Send>(&self, obj: &NamedObjectBase<T>) -> BuckyResult<ObjectId>
        where <T as cyfs_base::ObjectType>::ContentType: cyfs_base::BodyContent + cyfs_base::RawEncode,
              <T as cyfs_base::ObjectType>::DescType: RawEncodeWithContext<cyfs_base::NamedObjectContext>;
    async fn get_object<T: for <'a> RawDecode<'a>>(
        &self,
        target: Option<ObjectId>,
        object_id: ObjectId
    ) -> BuckyResult<T>;
    async fn put_object_with_resp(
        &self,
        req_path: &str,
        object_id: ObjectId,
        object_raw: Vec<u8>
    ) -> BuckyResult<Vec<u8>>;
    async fn put_object_with_resp2<T: RawEncode + for <'a> RawDecode<'a>>(
        &self,
        req_path: &str,
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

    async fn resolve_ood_list(&self, object_id: ObjectId) -> BuckyResult<Vec<DeviceId>> {
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

        Ok(resp.device_list)
    }

    async fn get_object_from_local<T: for<'a> RawDecode<'a>>(&self, object_id: ObjectId) -> BuckyResult<T> {
        self.get_object(None, object_id).await
    }

    async fn put_object_to_local<T: ObjectType + Sync + Send>(&self, obj: &NamedObjectBase<T>) -> BuckyResult<ObjectId>
        where <T as cyfs_base::ObjectType>::ContentType: cyfs_base::BodyContent + cyfs_base::RawEncode,
              <T as cyfs_base::ObjectType>::DescType: RawEncodeWithContext<cyfs_base::NamedObjectContext> {
        let object_id = obj.desc().calculate_id();
        let object_raw = obj.to_vec()?;
        self.non_service().put_object(NONPutObjectOutputRequest { common: NONOutputRequestCommon {
            req_path: None,
            source: None,
            dec_id: None,
            level: NONAPILevel::NOC,
            target: None,
            flags: 0
        }, object: NONObjectInfo {
            object_id: object_id.clone(),
            object_raw,
            object: None
        },
            access: None
        }).await?;

        Ok(object_id)
    }

    async fn pub_object<T: ObjectType + Sync + Send>(&self, obj: &NamedObjectBase<T>) -> BuckyResult<ObjectId>
        where <T as ObjectType>::ContentType: BodyContent + RawEncode, <T as ObjectType>::DescType: RawEncodeWithContext<NamedObjectContext> {
        let object_id = obj.desc().calculate_id();
        let object_raw = obj.to_vec()?;
        self.non_service().put_object(NONPutObjectOutputRequest { common: NONOutputRequestCommon {
            req_path: None,
            source: None,
            dec_id: None,
            level: NONAPILevel::NOC,
            target: None,
            flags: 0
        }, object: NONObjectInfo {
            object_id: object_id.clone(),
            object_raw,
            object: None
        },
            access: Some(AccessString::full())
        }).await?;

        Ok(object_id)
    }

    async fn get_object<T: for <'a> RawDecode<'a>>(&self, target: Option<ObjectId>, object_id: ObjectId) -> BuckyResult<T> {
        let resp = self.non_service().get_object(NONGetObjectOutputRequest {
            common: NONOutputRequestCommon {
                req_path: None,
                source: None,
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

    async fn put_object_with_resp(&self, req_path: &str, object_id: ObjectId, object_raw: Vec<u8>) -> BuckyResult<Vec<u8>> {
        app_call_log!("put_object_with_resp req_path={}", req_path);
        let cyfs_path = CyfsPath::parse(req_path)?;
        let path = RequestGlobalStatePath {
            global_state_category: None,
            global_state_root: None,
            dec_id: Some(cyfs_path.target_dec_id),
            req_path: Some(cyfs_path.req_path)
        };
        // let object_raw = self.sign_object(object_id.clone(), object_raw).await?;
        let resp = self.non_service().post_object(NONPostObjectOutputRequest {
            common: NONOutputRequestCommon {
                req_path: Some(path.to_string()),
                source: None,
                dec_id: None,
                level: NONAPILevel::Router,
                target: Some(cyfs_path.target),
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

    async fn put_object_with_resp2<T: RawEncode + for <'a> RawDecode<'a>>(&self, req_path: &str, object_id: ObjectId, object_raw: Vec<u8>) -> BuckyResult<T> {
        app_call_log!("put_object_with_resp2 req_path={}", req_path);
        let cyfs_path = CyfsPath::parse(req_path)?;
        let path = RequestGlobalStatePath {
            global_state_category: None,
            global_state_root: None,
            dec_id: Some(cyfs_path.target_dec_id),
            req_path: Some(cyfs_path.req_path)
        };

        let resp = self.non_service().post_object(NONPostObjectOutputRequest {
            common: NONOutputRequestCommon {
                req_path: Some(path.to_string()),
                source: None,
                dec_id: None,
                level: NONAPILevel::Router,
                target: Some(cyfs_path.target),
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
