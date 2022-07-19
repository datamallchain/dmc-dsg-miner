use std::path::PathBuf;
use async_std::{fs::{self, File}, io::WriteExt};
use cyfs_base::*;
use std::str::FromStr;
use serde::{Deserialize};


#[derive(Clone, Debug, Deserialize)]
pub struct MinerConfig {
    pub db: DbConfig,
    pub dmc: DmcConfig
}

#[derive(Clone, Debug, Deserialize)]
pub struct DbConfig {
    pub host: String,
    pub username: String,
    pub password: String,
    pub db_name: String
}

#[derive(Clone, Debug, Deserialize)]
pub struct DmcConfig {
    pub dmc_account: String,
    pub dmc_key: String,
    pub dmc_server: String,
    pub http_domain: String,
}


impl MinerConfig {
    pub async fn new() -> BuckyResult<Self> {
        bucky_result!(toml::from_str::<MinerConfig>(&Self::load_config().await?))
    }

    async fn load_config() -> BuckyResult<String> {
        #[cfg(target_os = "windows")]
        let path = bucky_result!(PathBuf::from_str("C:\\dcfs\\etc\\dmc_miner\\conf.cfg"))?;
        #[cfg(not(target_os = "windows"))]
        let path = bucky_result!(PathBuf::from_str("/dcfs/etc/dmc_miner/conf.cfg"))?;

        if !path.is_file() {
            let p = path.parent().unwrap();
            if !p.is_dir(){
                fs::create_dir_all(p).await?;
            }
            let mut f = File::create(&path).await?;
            f.write_all(Self::default_config().await.as_bytes()).await?;
        }

        Ok(std::fs::read_to_string(path)?)
    }

    async fn default_config() -> &'static str {
r#"
[dmc]
dmc_account = ""
dmc_key = ""
dmc_server = ""
http_domain = ""

[db]
host = ""
username = ""
password = ""
db_name = ""

"#
    }

}
