use std::sync::Arc;
use super::*;
use anyhow::Result;
use async_std::{io::BufReader, task::block_on};
use tide::{Body, Error, Request, StatusCode};

struct StateMiner<CONN: ContractMetaStore, CHUNKSTORE: ContractChunkStore> {
    meta_store: Arc<dyn MetaStore<CONN>>,
    raw_data_store: Arc<CHUNKSTORE>,
}

pub struct CacheHttp {
}

impl CacheHttp {
    pub async fn run<CONN: ContractMetaStore, CHUNKSTORE: ContractChunkStore>(
        meta_store: Arc<dyn MetaStore<CONN>>,
        raw_data_store: Arc<CHUNKSTORE>) -> Result<()> {

        let mut app = tide::with_state(Arc::new(StateMiner{meta_store, raw_data_store }));
        app.at("/slice/:start/:end/*").get(Self::get_slice);
        app.at("/*").get(Self::get_file);
        app.listen("0.0.0.0:32855").await?;

        Ok(())
    }

    async fn get_file<CONN: ContractMetaStore, CHUNKSTORE: ContractChunkStore>(req: Request<Arc<StateMiner<CONN, CHUNKSTORE>>>) -> tide::Result<Body> {
        let url = req.url();
        let url_path = url.path();

        let state = req.state();
        let mut conn = state.meta_store.create_meta_connection().await?;
        let chunks_list = conn.get_chunks_by_path(url_path.to_string()).await?;

        let mut len = 0;
        for chunk_id in &chunks_list {
            len += chunk_id.len()
        }

        let list = chunks_list.iter().map(|chunk_id| {
            block_on(state.raw_data_store.get_chunk_reader(chunk_id)).unwrap()
        }).collect::<Vec<_>>();

        let merge_reader = ReaderTool::merge(list).await;

        Ok(Body::from_reader(BufReader::new(merge_reader), Some(len)))
    }

    async fn get_slice<CONN: ContractMetaStore, CHUNKSTORE: ContractChunkStore>(req: Request<Arc<StateMiner<CONN, CHUNKSTORE>>>) -> tide::Result<Body> {
        let index_start: usize = req.param("start")?.parse().unwrap_or(0);
        let index_end: usize = req.param("end")?.parse().unwrap_or(0);

        if index_end > index_start {
            let url = req.url();
            let url_path = url.path();

            let url_list = url_path.splitn(5,'/').collect::<Vec<_>>();
            let qpath = format!("/{}", url_list[4]);
            let state = req.state();
            let mut conn = state.meta_store.create_meta_connection().await?;
            let chunks_list = conn.get_chunks_by_path(qpath).await?;
            let file_chunks = &chunks_list[index_start..index_end];

            let mut len = 0;
            for chunk_id in file_chunks {
                len += chunk_id.len()
            }

            let list = chunks_list.iter().map(|chunk_id| {
                block_on(state.raw_data_store.get_chunk_reader(chunk_id)).unwrap()
            }).collect::<Vec<_>>();

            let merge_reader = ReaderTool::merge(list).await;

            return Ok(Body::from_reader(BufReader::new(merge_reader), Some(len)))
        }
        Err(Error::from_str(StatusCode::BadRequest, "params err"))
    }
}

