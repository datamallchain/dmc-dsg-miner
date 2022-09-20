use cyfs_base::*;
use std::sync::Arc;
use crate::{APP_ERROR_FAILED};

pub trait ArcWeakHelper<T: ?Sized> {
    fn to_rc(&self) -> BuckyResult<Arc<T>>;
}

impl <T: ?Sized> ArcWeakHelper<T> for std::sync::Weak<T> {
    fn to_rc(&self) -> BuckyResult<Arc<T>> {
        match self.upgrade() {
            Some(v) => {
                Ok(v)
            },
            None => {
                Err(crate::app_err!(APP_ERROR_FAILED, "weak err"))
            }
        }
    }
}

pub fn random_data(buffer: &mut [u8]) {
    let len = buffer.len();
    let mut gen_count = 0;
    while len - gen_count >= 8 {
        let r = rand::random::<u64>();
        buffer[gen_count..gen_count + 8].copy_from_slice(&r.to_be_bytes());
        gen_count += 8;
    }

    while len - gen_count > 0 {
        let r = rand::random::<u8>();
        buffer[gen_count] = r;
        gen_count += 1;
    }
}

pub fn get_space_size_str(space_size: u64) -> String {
    if space_size < 1024 {
        format!("{} B", space_size)
    } else if space_size < 1024 * 1024 {
        format!("{} KB", space_size / 1024)
    } else if space_size < 10  * 1024 * 1024 {
        format!("{:.2} MB", space_size as f64 / (1024 * 1024) as f64)
    } else if space_size < 1024 * 1024 * 1024 {
        format!("{} MB", space_size / (1024 * 1024))
    } else if space_size < 10 * 1024 * 1024 * 1024 {
        format!("{:.2} GB", space_size as f64 / (1024 * 1024 * 1024) as f64)
    } else if space_size < 1024 * 1024 * 1024 * 1024 {
        format!("{} GB", space_size / (1024 * 1024 * 1024))
    } else if space_size < 10 * 1024 * 1024 * 1024 * 1024 {
        format!("{:.2} TB", space_size as f64 / (1024_u64 * 1024 * 1024 * 1024) as f64)
    } else {
        format!("{} TB", space_size / (1024_u64 * 1024 * 1024 * 1024))
    }
}
