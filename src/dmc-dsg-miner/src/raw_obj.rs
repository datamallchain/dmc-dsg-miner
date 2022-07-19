use cyfs_base::*;
use std::ops::{Deref, DerefMut};

#[derive(Clone, Debug, RawEncode, RawDecode)]
pub struct RawDescContent {
    obj_type: u16,
    content_hash: HashValue
}

impl DescContent for RawDescContent {
    fn obj_type() -> u16 {
        50000 as u16
    }

    type OwnerType = Option<ObjectId>;
    type AreaType = SubDescNone;
    type AuthorType = SubDescNone;
    type PublicKeyType = SubDescNone;
}

#[derive(Clone, Debug, RawEncode, RawDecode)]
pub struct RawBodyContent(pub Vec<u8>);

impl Deref for RawBodyContent {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RawBodyContent {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl BodyContent for RawDescContent {
    fn version(&self) -> u8 {
        0
    }

    fn format(&self) -> u8 {
        OBJECT_CONTENT_CODEC_FORMAT_RAW
    }
}

impl BodyContent for RawBodyContent {

    fn version(&self) -> u8 {
        0
    }

    fn format(&self) -> u8 {
        OBJECT_CONTENT_CODEC_FORMAT_RAW
    }
}

pub type RawObjType = NamedObjType<RawDescContent, RawBodyContent>;
pub type RawObjectBuilder = NamedObjectBuilder<RawDescContent, RawBodyContent>;
pub type RawObject = NamedObjectBase<RawObjType>;

pub trait TRawObject<T: RawEncode + for<'a> RawDecode<'a>> {
    fn new(dec_id: ObjectId, owner_id: ObjectId, obj_type: u16, obj: &T) -> BuckyResult<RawObject>;
    fn get(&self) -> BuckyResult<T>;
}

pub trait RawObjectType {
    fn get_raw_obj_type(&self) -> u16;
}

impl RawObjectType for NamedObjectBase<RawObjType> {
    fn get_raw_obj_type(&self) -> u16 {
        self.desc().content().obj_type
    }
}

impl <T: RawEncode + for<'a> RawDecode<'a>> TRawObject<T> for NamedObjectBase<RawObjType> {
    fn new(dec_id: ObjectId, owner_id: ObjectId, obj_type: u16, obj: &T) -> BuckyResult<RawObject> {
        let body = RawBodyContent(obj.to_vec()?);
        let desc = RawDescContent { obj_type, content_hash: hash_data(body.as_slice()) };

        Ok(RawObjectBuilder::new(desc, body).owner(owner_id).dec_id(dec_id).build())
    }

    fn get(&self) -> BuckyResult<T> {
        let body = self.body().as_ref().unwrap().content();
        T::clone_from_slice(body.as_slice())
    }

}

