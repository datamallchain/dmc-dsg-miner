use std::ops::{Deref, DerefMut};
use cyfs_base::BuckyResult;

#[async_trait::async_trait]
pub trait MetaConnection: 'static + Send {
    async fn begin_trans(&mut self) -> BuckyResult<()>;
    async fn commit_trans(&mut self) -> BuckyResult<()>;
    async fn rollback_trans(&mut self) -> BuckyResult<()>;
}

pub struct MetaConnectionProxy<CONN: MetaConnection> {
    conn: CONN,
    has_commit: bool,
}

impl<CONN: MetaConnection> MetaConnectionProxy<CONN> {
    pub fn new(conn: CONN) -> Self {
        Self {
            conn,
            has_commit: false
        }
    }

    pub async fn begin(&mut self) -> BuckyResult<()> {
        self.conn.begin_trans().await
    }

    pub async fn commit(&mut self) -> BuckyResult<()> {
        self.has_commit = true;
        self.conn.commit_trans().await
    }

    pub async fn rollback(&mut self) -> BuckyResult<()> {
        self.conn.rollback_trans().await
    }

    fn has_commit(&self) -> bool {
        self.has_commit
    }
}

impl<CONN: MetaConnection> Deref for MetaConnectionProxy<CONN> {
    type Target = CONN;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl<CONN: MetaConnection> DerefMut for MetaConnectionProxy<CONN> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

impl<CONN: MetaConnection> Drop for MetaConnectionProxy<CONN> {
    fn drop(&mut self) {
        if !self.has_commit() {
            unsafe {
                let this: &'static mut Self = std::mem::transmute(self);
                async_std::task::block_on(async move {
                    if let Err(e) = this.rollback().await {
                        log::error!("rollback err {}", e);
                    }
                });
            }
        }
    }
}
