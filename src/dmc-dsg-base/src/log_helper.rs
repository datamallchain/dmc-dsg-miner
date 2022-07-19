use cyfs_base::{BuckyError, BuckyErrorCode};

#[macro_export]
macro_rules! app_err {
    ( $err: expr) => {
    cyfs_base::BuckyError::new(cyfs_base::BuckyErrorCodeEx::DecError($err as u16), format!("{}:{} app_code_err:{}", file!(), line!(), stringify!($err)))
    };
}

#[macro_export]
macro_rules! app_err2 {
    ( $err: expr, $msg: expr) => {
    cyfs_base::BuckyError::new(cyfs_base::BuckyErrorCodeEx::DecError($err as u16), format!("{}:{} app_code_err:{} msg:{}", file!(), line!(), stringify!($err), $msg))
    };
}

#[macro_export]
macro_rules! cyfs_err {
    ( $err: expr, $($arg:tt)*) => {
        {
            log::error!("{}", format!($($arg)*));
            cyfs_base::BuckyError::new($err, format!("{}:{} msg:{}", file!(), line!(), format!($($arg)*)))
        }
    };
}

#[macro_export]
macro_rules! app_err_msg {
    ( $msg: expr) => {
        format!("{}:{} msg:{}", file!(), line!(), $msg)
    };
}

#[macro_export]
macro_rules! app_msg {
    ($($arg:tt)*) => {
        format!("{}:{} msg:{}", file!(), line!(), format!($($arg)*))
    }
}

#[macro_export]
macro_rules! app_call_log {
    ($($arg:tt)*) => {
        let _log = crate::LogObject::new(format!("[{}:{}] {}", file!(), line!(), format!($($arg)*)));
    };
}

pub fn get_app_err_code(ret: &BuckyError) -> u16 {
    if let BuckyErrorCode::DecError(code) = ret.code() {
        code
    } else {
        u16::MAX
    }
}

pub struct LogObject{
    msg: String
}

impl LogObject {
    #[inline(always)]
    pub fn new(msg: String) -> Self {
        log::info!("{} start", msg.as_str());
        Self {
            msg
        }
    }
}

impl Drop for LogObject {
    #[inline(always)]
    fn drop(&mut self) {
        log::info!("{} complete", self.msg);
    }
}
