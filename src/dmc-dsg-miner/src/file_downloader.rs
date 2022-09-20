use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use cyfs_base::*;
use cyfs_lib::*;
use dmc_dsg_base::CyfsNOC;
use crate::app_call_log;

#[derive(RawEncode, RawDecode, Clone)]
pub struct DownloadParams {
    pub padding_len: u32,
}

#[async_trait::async_trait]
pub trait FileDownloader: 'static + Clone + Sync + Send {
    async fn download(&self, chunk_list: Vec<ChunkId>, source_list: Vec<DeviceId>, params: DownloadParams) -> BuckyResult<()>;
}

#[derive(Clone)]
pub struct CyfsStackFileDownloader {
    stack: Arc<SharedCyfsStack>,
    dec_id: ObjectId,
}

impl CyfsStackFileDownloader {
    pub fn new(stack: Arc<SharedCyfsStack>, dec_id: ObjectId) -> Self {
        Self {
            stack,
            dec_id
        }
    }
}

#[async_trait::async_trait]
impl FileDownloader for CyfsStackFileDownloader {
    async fn download(&self, chunk_list: Vec<ChunkId>, source_list: Vec<DeviceId>, _params: DownloadParams) -> BuckyResult<()> {
        let chunk_ref = if chunk_list.len() < 50 {
            &chunk_list[..]
        } else {
            &chunk_list[0..50]
        };
        app_call_log!("download chunks {:?}", chunk_ref);
        let chunk_bundle = ChunkBundle::new(chunk_list, ChunkBundleHashMethod::Serial);
        let owner_id = self.stack.local_device().desc().owner().clone().unwrap();
        let file = File::new(owner_id, chunk_bundle.len(), chunk_bundle.calc_hash_value(), ChunkList::ChunkInBundle(chunk_bundle)).no_create_time().build();
        let file_id = self.stack.put_object_to_noc(&file).await?;

        let task_id = self.stack.trans().create_task(&TransCreateTaskOutputRequest {
            common: NDNOutputRequestCommon {
                req_path: None,
                dec_id: Some(self.dec_id.clone()),
                level: NDNAPILevel::NDC,
                target: None,
                referer_object: vec![],
                flags: 0
            },
            object_id: file_id,
            local_path: PathBuf::new(),
            device_list: source_list,
            context_id: None,
            auto_start: true
        }).await?.task_id;

        loop {
            let state = self.stack.trans().get_task_state(&TransGetTaskStateOutputRequest {
                common: NDNOutputRequestCommon {
                    req_path: None,
                    dec_id: Some(self.dec_id.clone()),
                    level: NDNAPILevel::NDC,
                    target: None,
                    referer_object: vec![],
                    flags: 0
                },
                task_id: task_id.clone()
            }).await?;

            match state {
                TransTaskState::Pending => {

                }
                TransTaskState::Downloading(_) => {

                }
                TransTaskState::Paused | TransTaskState::Canceled => {
                    let msg = format!("download {} task abnormal exit.", file_id.to_string());
                    log::error!("{}", msg.as_str());
                    return Err(BuckyError::new(BuckyErrorCode::Failed, msg))
                }
                TransTaskState::Finished(_) => {
                    break;
                }
                TransTaskState::Err(err) => {
                    let msg = format!("download {} failed.{}", file_id.to_string(), err);
                    log::error!("{}", msg.as_str());
                    return Err(BuckyError::new(err, msg))
                }
            }
            async_std::task::sleep(Duration::from_secs(1)).await;
        }
        self.stack.trans().delete_task(&TransTaskOutputRequest {
            common: NDNOutputRequestCommon {
                req_path: None,
                dec_id: Some(self.dec_id.clone()),
                level: NDNAPILevel::NDC,
                target: None,
                referer_object: vec![],
                flags: 0
            },
            task_id
        }).await?;
        Ok(())
    }
}
