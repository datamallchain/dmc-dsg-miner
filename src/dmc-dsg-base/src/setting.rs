use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use cyfs_base::*;
use cyfs_lib::*;

#[derive(Clone, Debug, RawEncode, RawDecode)]
pub struct SettingDescContent {
    content_hash: HashValue
}

impl DescContent for SettingDescContent {
    fn obj_type() -> u16 {
        50002 as u16
    }

    type OwnerType = Option<ObjectId>;
    type AreaType = SubDescNone;
    type AuthorType = SubDescNone;
    type PublicKeyType = SubDescNone;
}

#[derive(Clone, Debug, RawEncode, RawDecode)]
pub struct SettingBodyContent {
    setting: HashMap<String, String>,
}

impl BodyContent for SettingDescContent {
    fn version(&self) -> u8 {
        0
    }

    fn format(&self) -> u8 {
        OBJECT_CONTENT_CODEC_FORMAT_RAW
    }
}

impl BodyContent for SettingBodyContent {

    fn version(&self) -> u8 {
        0
    }

    fn format(&self) -> u8 {
        OBJECT_CONTENT_CODEC_FORMAT_RAW
    }
}

pub type SettingObjectType = NamedObjType<SettingDescContent, SettingBodyContent>;
pub type SettingObjectBuilder = NamedObjectBuilder<SettingDescContent, SettingBodyContent>;
pub type SettingObject = NamedObjectBase<SettingObjectType>;

trait TSetting {
    fn new(setting: HashMap<String, String>) -> Self;
    fn into_setting_map(self) -> HashMap<String, String>;
}

impl TSetting for SettingObject {
    fn new(setting: HashMap<String, String>) -> Self {
        let body = SettingBodyContent {
            setting
        };
        let hash = hash_data(body.to_vec().unwrap().as_slice());
        let desc = SettingDescContent {
            content_hash: hash
        };

        SettingObjectBuilder::new(desc, body).no_create_time().build()
    }

    fn into_setting_map(self) -> HashMap<String, String> {
        self.into_body().unwrap().into_content().setting
    }
}

pub struct Setting {
    stack: Arc<SharedCyfsStack>,
    setting: Mutex<(bool, HashMap<String, String>)>
}
pub type SettingRef = Arc<Setting>;

impl Setting {
    pub fn new(stack: Arc<SharedCyfsStack>) -> SettingRef {
        Arc::new(Self {
            stack,
            setting: Mutex::new((false, HashMap::new()))
        })
    }

    pub async fn load(&self) -> BuckyResult<()> {
        let op_env = self.stack.root_state_stub(None, None).create_path_op_env().await?;
        let setting_id = op_env.get_by_path("/setting").await?;
        if setting_id.is_some() {
            let resp = self.stack.non_service().get_object(NONGetObjectOutputRequest {
                common: NONOutputRequestCommon {
                    req_path: None,
                    source: None,
                    dec_id: None,
                    level: NONAPILevel::NOC,
                    target: None,
                    flags: 0
                },
                object_id: setting_id.unwrap(),
                inner_path: None
            }).await?;
            let setting = SettingObject::clone_from_slice(resp.object.object_raw.as_slice())?;
            self.setting.lock().unwrap().1 = setting.into_setting_map();
        }
        Ok(())
    }

    pub async fn save(&self) -> BuckyResult<()> {
        let (obj_id, obj) = {
            let setting = self.setting.lock().unwrap();
            if !setting.0 {
                return Ok(());
            }

            let obj = SettingObject::new(setting.1.clone());
            let obj_id = obj.desc().calculate_id();
            (obj_id, obj)
        };

        self.stack.non_service().put_object(NONPutObjectOutputRequest { 
            common: NONOutputRequestCommon {
                req_path: None,
                source: None,
                dec_id: None,
                level: NONAPILevel::NOC,
                target: None,
                flags: 0
            }, object: NONObjectInfo {
                object_id: obj_id.clone(),
                object_raw: obj.to_vec()?,
                object: None
            }, 
            access: Some(AccessString::default())
        }).await?;

        let op_env = self.stack.root_state_stub(None, None).create_path_op_env().await?;
        op_env.set_with_path("/setting", &obj_id, None, true).await?;
        op_env.commit().await?;

        self.setting.lock().unwrap().0 = false;

        Ok(())
    }

    pub fn get_setting(&self, key: &str, default: &str) -> String {
        let setting = self.setting.lock().unwrap();
        match setting.1.get(key) {
            Some(v) => {
                v.clone()
            }
            None => default.to_string()
        }
    }

    pub fn set_setting(&self, key: String, value: String) {
        let mut setting = self.setting.lock().unwrap();
        setting.0 = true;
        setting.1.insert(key, value);
    }
}
