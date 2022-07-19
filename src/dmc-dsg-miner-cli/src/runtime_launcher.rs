use std::time::Duration;
use std::process::{Command, Stdio};
#[cfg(windows)]
use std::os::windows::process::CommandExt;

pub struct RuntimeLauncher;

impl RuntimeLauncher {
    #[cfg(windows)]
    pub async fn launch() {
        async_std::task::spawn(async {
            loop {
                if !cyfs_util::process::check_process_mutex(cyfs_base::CYFS_RUNTIME_NAME) {
                    let cyfs_path = dirs::config_dir().unwrap().join("cyfs");
                    let runtime_path = cyfs_path.join("services").join("runtime").join("cyfs-runtime.exe");
                    log::info!("launch {}", runtime_path.to_string_lossy().to_string());
                    let _ = Command::new(runtime_path.as_path()).creation_flags(0x08000000).stdout(Stdio::null()).stderr(Stdio::null()).stdin(Stdio::null()).spawn().map_err(|e| {
                        log::info!("launch {} failed.err{}", runtime_path.to_string_lossy().to_string(), e);
                    });
                }
                async_std::task::sleep(Duration::new(1, 0)).await;
            }
        });
    }

    #[cfg(not(windows))]
    pub async fn launch() {
        async_std::task::spawn(async {
            loop {
                if !cyfs_util::process::check_process_mutex(cyfs_base::CYFS_RUNTIME_NAME) {
                    let cyfs_path = dirs::data_dir().unwrap().join("cyfs");
                    let runtime_path = cyfs_path.join("services").join("runtime").join("cyfs-runtime");
                    log::info!("launch {}", runtime_path.to_string_lossy().to_string());
                    let _ = Command::new(runtime_path.as_path()).stdout(Stdio::null()).stderr(Stdio::null()).stdin(Stdio::null()).spawn().map_err(|e| {
                        log::info!("launch {} failed.err{}", runtime_path.to_string_lossy().to_string(), e);
                    });
                }
                async_std::task::sleep(Duration::new(1, 0)).await;
            }
        });
    }
}
