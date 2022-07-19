use std::sync::Arc;
use super::*;
use anyhow::Result;
use async_std::{io::BufReader, task::block_on};
use tide::{Body, Request};

struct StateMiner{
    chunk_meta: Arc<Box<dyn ContractMetaStore>>,
    raw_data_store: Arc<Box<dyn ContractChunkStore>>,
}

pub struct CacheHttp;

impl CacheHttp {
    pub async fn run(chunk_meta: Arc<Box<dyn ContractMetaStore>>, raw_data_store: Arc<Box<dyn ContractChunkStore>>) -> Result<()> {

        let mut app = tide::with_state(Arc::new(StateMiner{chunk_meta, raw_data_store}));
        app.at("/*").get(Self::get_file);
        app.listen("0.0.0.0:32855").await?;

        Ok(())
    }

    async fn get_file(req: Request<Arc<StateMiner>>) -> tide::Result<Body> {
        let url = req.url();
        let url_path = url.path();

        let state = req.state();
        let chunks_list = state.chunk_meta.get_chunks_by_path(url_path.to_string()).await?;

        let mut len = 0;
        for chunk_id in &chunks_list {
            len += chunk_id.len()
        }

        let list = chunks_list.iter().map(|chunk_id| {
            block_on(state.raw_data_store.get_chunk_reader(chunk_id.clone())).unwrap()
        }).collect::<Vec<_>>();

        let merge_reader = ReaderTool::merge(list).await;
        
        Ok(Body::from_reader(BufReader::new(merge_reader), Some(len)))
    }
}

