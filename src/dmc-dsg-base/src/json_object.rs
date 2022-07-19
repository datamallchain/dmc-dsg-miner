use cyfs_base::*;
use serde::{Serialize, Deserialize};
use std::ops::{Deref, DerefMut};
use crate::*;

#[derive(Clone, Debug, RawEncode, RawDecode)]
pub struct JSONDescContent {
    obj_type: u16,
    content_hash: HashValue
}

impl DescContent for JSONDescContent {
    fn obj_type() -> u16 {
        50001 as u16
    }

    type OwnerType = Option<ObjectId>;
    type AreaType = SubDescNone;
    type AuthorType = SubDescNone;
    type PublicKeyType = SubDescNone;
}

#[derive(Clone, Debug, RawEncode, RawDecode)]
pub struct JSONBodyContent(pub Vec<u8>);

impl Deref for JSONBodyContent {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for JSONBodyContent {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl BodyContent for JSONDescContent {
    fn version(&self) -> u8 {
        0
    }

    fn format(&self) -> u8 {
        OBJECT_CONTENT_CODEC_FORMAT_RAW
    }
}

impl BodyContent for JSONBodyContent {

    fn version(&self) -> u8 {
        0
    }

    fn format(&self) -> u8 {
        OBJECT_CONTENT_CODEC_FORMAT_RAW
    }
}

pub type JSONObjectType = NamedObjType<JSONDescContent, JSONBodyContent>;
pub type JSONObjectBuilder = NamedObjectBuilder<JSONDescContent, JSONBodyContent>;
pub type JSONObject = NamedObjectBase<JSONObjectType>;

pub trait DSGJSON<T: Serialize + for<'a> Deserialize<'a>> {
    fn new(dec_id: ObjectId, owner_id: ObjectId, obj_type: u16, obj: &T) -> BuckyResult<JSONObject>;
    fn get(&self) -> BuckyResult<T>;
}

pub trait DSGJSONType {
    fn get_json_obj_type(&self) -> u16;
}

impl DSGJSONType for NamedObjectBase<JSONObjectType> {
    fn get_json_obj_type(&self) -> u16 {
        self.desc().content().obj_type
    }
}

pub trait Verifier {
    fn verify_body(&self) -> bool;
}

impl <T: Serialize + for<'a> Deserialize<'a>> DSGJSON<T> for NamedObjectBase<JSONObjectType> {
    fn new(dec_id: ObjectId, owner_id: ObjectId, obj_type: u16, obj: &T) -> BuckyResult<JSONObject> {
        let body = JSONBodyContent(serde_json::to_vec(obj).map_err(|e| {
            log::info!("serde json err:{}", e);
            crate::app_err!(APP_ERROR_FAILED)
        })?);

        let desc = JSONDescContent { obj_type, content_hash: hash_data(body.as_slice()) };

        Ok(JSONObjectBuilder::new(desc, body).owner(owner_id).dec_id(dec_id).build())
    }

    fn get(&self) -> BuckyResult<T> {
        let body = self.body().as_ref().unwrap().content();
        serde_json::from_slice(body.as_ref()).map_err(|e| {
            let str = String::from_utf8_lossy(body.as_slice()).to_string();
            let msg = format!("parse {} body err:{}", str, e);
            log::info!("{}", msg);
            app_err2!(APP_ERROR_FAILED, msg)
        })
    }

}

impl Verifier for NamedObjectBase<JSONObjectType> {
    fn verify_body(&self) -> bool {
        if self.body().is_none() {
            return false;
        }

        let body = self.body().as_ref().unwrap().content();
        if hash_data(body.as_slice()) == self.desc().content().content_hash {
            true
        } else {
            false
        }
    }
}
