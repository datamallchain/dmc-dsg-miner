use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use cyfs_base::{BuckyErrorCode, BuckyResult};
use serde::{Serialize, Deserialize};
use crate::*;

pub trait DMCSerialize {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()>;
}

pub trait DMCDeserialize: Sized {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self>;
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AbiDef {
    pub version: String,
    pub types: Vec<TypeDef>,
    pub structs: Vec<StructDef>,
    pub actions: Vec<ActionDef>,
    pub tables: Vec<TableDef>,
    pub ricardian_clauses: Vec<ClausePair>,
    pub error_messages: Vec<ErrorMessage>,
    pub abi_extensions: Vec<ExtensionsEntry>,
    pub variants: Option<Vec<VariantDef>>,
    pub action_results: Option<Vec<ActionResult>>,
    pub kv_tables: Option<KVTable>
}

impl AbiDef {
    pub fn parse(raw_abi: &mut Vec<u8>) -> BuckyResult<Self> {
        let mut buf = SerialBuffer::new(raw_abi);
        let version = buf.get_string()?;
        if !version.starts_with("eosio::abi/1.") {
            return Err(cyfs_err!(BuckyErrorCode::NotSupport, "version {}", version))
        }

        buf.restart_read();

        AbiDef::dmc_deserialize(&mut buf)
    }

    pub fn to_raw_abi(&self) -> BuckyResult<Vec<u8>> {
        let mut buf = Vec::new();
        let mut ser_buf = SerialBuffer::new(&mut buf);
        self.dmc_serialize(&mut ser_buf)?;
        Ok(buf)
    }
}

impl DMCSerialize for AbiDef {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_string(self.version.as_str());
        let len = self.types.len();
        buf.push_var_u32(len as u32);
        for ty in self.types.iter() {
            ty.dmc_serialize(buf)?;
        }
        buf.push_var_u32(self.structs.len() as u32);
        for st in self.structs.iter() {
            st.dmc_serialize(buf)?;
        }
        buf.push_var_u32(self.actions.len() as u32);
        for action in self.actions.iter() {
            action.dmc_serialize(buf)?;
        }
        buf.push_var_u32(self.tables.len() as u32);
        for table in self.tables.iter() {
            table.dmc_serialize(buf)?;
        }
        buf.push_var_u32(self.ricardian_clauses.len() as u32);
        for clause in self.ricardian_clauses.iter() {
            clause.dmc_serialize(buf)?;
        }
        buf.push_var_u32(self.error_messages.len() as u32);
        for message in self.error_messages.iter() {
            message.dmc_serialize(buf)?;
        }
        buf.push_var_u32(self.abi_extensions.len() as u32);
        for ext in self.abi_extensions.iter() {
            ext.dmc_serialize(buf)?;
        }
        if self.variants.is_some() {
            buf.push_var_u32(self.variants.as_ref().unwrap().len() as u32);
            for var in self.variants.as_ref().unwrap().iter() {
                var.dmc_serialize(buf)?;
            }

            if self.action_results.is_some() {
                buf.push_var_u32(self.action_results.as_ref().unwrap().len() as u32);
                for result in self.action_results.as_ref().unwrap().iter() {
                    result.dmc_serialize(buf)?;
                }
                if self.kv_tables.is_some() {
                    self.kv_tables.as_ref().unwrap().dmc_serialize(buf)?;
                }
            }
        }
        Ok(())
    }
}

impl DMCDeserialize for AbiDef {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        let version = buf.get_string()?;
        let len = buf.get_var_u32()?;
        let mut types = Vec::new();
        for _ in 0..len {
            types.push(TypeDef::dmc_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut structs = Vec::new();
        for _ in 0..len {
            structs.push(StructDef::dmc_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut actions = Vec::new();
        for _ in 0..len {
            actions.push(ActionDef::dmc_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut tables = Vec::new();
        for _ in 0..len {
            tables.push(TableDef::dmc_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut ricardian_clauses = Vec::new();
        for _ in 0..len {
            ricardian_clauses.push(ClausePair::dmc_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut error_messages = Vec::new();
        for _ in 0..len {
            error_messages.push(ErrorMessage::dmc_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut abi_extensions = Vec::new();
        for _ in 0..len {
            abi_extensions.push(ExtensionsEntry::dmc_deserialize(buf)?);
        }
        let (variants, action_results, kv_tables) = if buf.have_read_data() {
            let len = buf.get_var_u32()?;
            let mut variants = Vec::new();
            for _ in 0..len {
                variants.push(VariantDef::dmc_deserialize(buf)?);
            }
            let (action_results, kv_tables) = if buf.have_read_data() {
                let len = buf.get_var_u32()?;
                let mut action_results = Vec::new();
                for _ in 0..len {
                    action_results.push(ActionResult::dmc_deserialize(buf)?);
                }
                let kv_tables = if buf.have_read_data() {
                    let kv_tables = KVTable::dmc_deserialize(buf)?;
                    Some(kv_tables)
                } else {
                    None
                };
                (Some(action_results), kv_tables)
            } else {
                (None, None)
            };
            (Some(variants), action_results, kv_tables)
        } else {
            (None, None, None)
        };
        Ok(Self {
            version,
            types,
            structs,
            actions,
            tables,
            ricardian_clauses,
            error_messages,
            abi_extensions,
            variants,
            action_results,
            kv_tables
        })
    }
}

pub type Name = String;

impl DMCSerialize for Name {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_name(self.as_str())?;
        Ok(())
    }
}

impl DMCDeserialize for Name {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        buf.get_name()
    }
}

pub type TimePointSec = String;

pub fn time_point_sec_dmc_serialize(time: &TimePointSec, buf: &mut SerialBuffer) -> BuckyResult<()> {
    buf.push_u32(date_to_time_point(time.as_str())? as u32);
    Ok(())
}

fn time_point_sec_dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<TimePointSec> {
    Ok(time_point_sec_to_date(buf.get_u32()? as i64))
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TypeDef {
    pub new_type_name: String,
    #[serde(rename="type")]
    pub ty: String,
}

impl DMCSerialize for TypeDef {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_string(self.new_type_name.as_str());
        buf.push_string(self.ty.as_str());
        Ok(())
    }
}

impl DMCDeserialize for TypeDef {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            new_type_name: buf.get_string()?,
            ty: buf.get_string()?,
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StructDef {
    pub name: String,
    pub base: String,
    pub fields: Vec<FieldDef>,
}

impl DMCSerialize for StructDef {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_string(self.name.as_str());
        buf.push_string(self.base.as_str());
        buf.push_var_u32(self.fields.len() as u32);
        for field in self.fields.iter() {
            field.dmc_serialize(buf)?;
        }
        Ok(())
    }
}

impl DMCDeserialize for StructDef {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        let name = buf.get_string()?;
        let base = buf.get_string()?;
        let len = buf.get_var_u32()?;
        let mut fields = Vec::new();
        for _ in 0..len {
            fields.push(FieldDef::dmc_deserialize(buf)?);
        }
        Ok(Self {
            name,
            base,
            fields
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ActionDef {
    pub name: Name,
    #[serde(rename="type")]
    pub ty: String,
    pub ricardian_contract: String,
}

impl DMCSerialize for ActionDef {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        self.name.dmc_serialize(buf)?;
        buf.push_string(self.ty.as_str());
        buf.push_string(self.ricardian_contract.as_str());
        Ok(())
    }
}

impl DMCDeserialize for ActionDef {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            name: Name::dmc_deserialize(buf)?,
            ty: buf.get_string()?,
            ricardian_contract: buf.get_string()?
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TableDef {
    pub name: Name,
    pub index_type: String,
    pub key_names: Vec<String>,
    pub key_types: Vec<String>,
    #[serde(rename="type")]
    pub ty: String,
}

impl DMCSerialize for TableDef {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        self.name.dmc_serialize(buf)?;
        buf.push_string(self.index_type.as_str());
        buf.push_var_u32(self.key_names.len() as u32);
        for key_name in self.key_names.iter() {
            buf.push_string(key_name);
        }
        buf.push_var_u32(self.key_types.len() as u32);
        for key_type in self.key_types.iter() {
            buf.push_string(key_type);
        }
        buf.push_string(self.ty.as_str());
        Ok(())
    }
}

impl DMCDeserialize for TableDef {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        let name = Name::dmc_deserialize(buf)?;
        let index_type = buf.get_string()?;
        let len = buf.get_var_u32()?;
        let mut key_names = Vec::new();
        for _ in 0..len {
            key_names.push(buf.get_string()?);
        }
        let len = buf.get_var_u32()?;
        let mut key_types = Vec::new();
        for _ in 0..len {
            key_types.push(buf.get_string()?);
        }
        let ty = buf.get_string()?;
        Ok(Self {
            name,
            index_type,
            key_names,
            key_types,
            ty
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ClausePair {
    pub id: String,
    pub body: String,
}

impl DMCSerialize for ClausePair {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_string(self.id.as_str());
        buf.push_string(self.body.as_str());
        Ok(())
    }
}

impl DMCDeserialize for ClausePair {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            id: buf.get_string()?,
            body: buf.get_string()?
        })
    }
}
#[derive(Serialize, Deserialize, Clone)]
pub struct ErrorMessage {
    pub error_code: u64,
    pub error_msg: String,
}

impl DMCSerialize for ErrorMessage {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_u64(self.error_code);
        buf.push_string(self.error_msg.as_str());
        Ok(())
    }
}

impl DMCDeserialize for ErrorMessage {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            error_code: buf.get_u64()?,
            error_msg: buf.get_string()?,
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ExtensionsEntry {
    pub tag: u16,
    pub value: Vec<u8>,
}

impl DMCSerialize for ExtensionsEntry {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_u16(self.tag);
        buf.push_bytes(self.value.as_slice());
        Ok(())
    }
}

impl DMCDeserialize for ExtensionsEntry {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            tag: buf.get_u16()?,
            value: buf.get_bytes()?.to_vec()
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct VariantDef {
    pub name: String,
    pub types: Vec<String>,
}

impl DMCSerialize for VariantDef {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_string(self.name.as_str());
        buf.push_var_u32(self.types.len() as u32);
        for ty in self.types.iter() {
            buf.push_string(ty);
        }
        Ok(())
    }
}

impl DMCDeserialize for VariantDef {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        let name = buf.get_string()?;
        let len = buf.get_var_u32()?;
        let mut types = Vec::new();
        for _ in 0..len {
            types.push(buf.get_string()?);
        }
        Ok(Self {
            name,
            types
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ActionResult {
    pub name: Name,
    #[serde(rename="type")]
    pub ty: String,
}

impl DMCSerialize for ActionResult {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        self.name.dmc_serialize(buf)?;
        buf.push_string(self.ty.as_str());
        Ok(())
    }
}

impl DMCDeserialize for ActionResult {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            name: Name::dmc_deserialize(buf)?,
            ty: buf.get_string()?
        })
    }
}
#[derive(Serialize, Deserialize, Clone)]
pub struct KVTable {
    pub name: Option<Name>,
    pub kv_table_entry_def: Option<KVTableEntryDef>,
}

impl DMCSerialize for KVTable {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        if self.kv_table_entry_def.is_some() {
            len += 1;
        }
        buf.push_var_u32(len);
        if self.name.is_some() {
            buf.push_string("name");
            Name::dmc_serialize(self.name.as_ref().unwrap(), buf)?;
        }
        if self.kv_table_entry_def.is_some() {
            buf.push_string("kv_table_entry_def");
            self.kv_table_entry_def.as_ref().unwrap().dmc_serialize(buf)?;
        }
        Ok(())
    }
}

impl DMCDeserialize for KVTable {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        let len = buf.get_var_u32()?;
        if len == 0 {
            Ok(Self {
                name: None,
                kv_table_entry_def: None,
            })
        } else {
            let mut name = None;
            let mut kv_table_entry_def = None;
            for _ in 0..len {
                let key = buf.get_string()?;
                if key.as_str() == "name" {
                    name = Some(Name::dmc_deserialize(buf)?);
                } else if key.as_str() == "kv_table_entry_def" {
                    kv_table_entry_def = Some(KVTableEntryDef::dmc_deserialize(buf)?)
                }
            }
            Ok(Self {
                name,
                kv_table_entry_def
            })
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FieldDef {
    pub name: String,
    #[serde(rename="type")]
    pub ty: String,
}

impl DMCSerialize for FieldDef {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_string(self.name.as_str());
        buf.push_string(self.ty.as_str());
        Ok(())
    }
}

impl DMCDeserialize for FieldDef {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        let name = buf.get_string()?;
        let ty = buf.get_string()?;
        Ok(Self { name, ty })
    }
}
#[derive(Serialize, Deserialize, Clone)]
pub struct SecondaryIndexDef {
    #[serde(rename="type")]
    ty: String
}

impl DMCSerialize for SecondaryIndexDef {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_string(self.ty.as_str());
        Ok(())
    }
}

impl DMCDeserialize for SecondaryIndexDef {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        buf.get_string().map(|ty| Self {ty})
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SecondaryIndices {
    pub name: Option<Name>,
    pub secondary_index_def: Option<SecondaryIndexDef>
}

impl DMCSerialize for SecondaryIndices {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        if self.secondary_index_def.is_some() {
            len += 1;
        }
        buf.push_var_u32(len);
        if self.name.is_some() {
            buf.push_string("name");
            Name::dmc_serialize(self.name.as_ref().unwrap(), buf)?;
        }
        if self.secondary_index_def.is_some() {
            buf.push_string("secondary_index_def");
            self.secondary_index_def.as_ref().unwrap().dmc_serialize(buf)?;
        }
        Ok(())
    }
}

impl DMCDeserialize for SecondaryIndices {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        let len = buf.get_var_u32()?;
        if len == 0 {
            Ok(Self {
                name: None,
                secondary_index_def: None,
            })
        } else {
            let mut name = None;
            let mut secondary_index_def = None;
            for _ in 0..len {
                let key = buf.get_string()?;
                if key.as_str() == "name" {
                    name = Some(Name::dmc_deserialize(buf)?);
                } else if key.as_str() == "secondary_index_def" {
                    secondary_index_def = Some(SecondaryIndexDef::dmc_deserialize(buf)?)
                }
            }
            Ok(Self {
                name,
                secondary_index_def
            })
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct KVTableEntryDef {
    pub ty: String,
    pub primary_index: PrimaryKeyIndexDef,
    pub secondary_indices: SecondaryIndices,
}

impl DMCSerialize for KVTableEntryDef {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_string(self.ty.as_str());
        self.primary_index.dmc_serialize(buf)?;
        self.secondary_indices.dmc_serialize(buf)?;
        Ok(())
    }
}

impl DMCDeserialize for KVTableEntryDef {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            ty: buf.get_string()?,
            primary_index: PrimaryKeyIndexDef::dmc_deserialize(buf)?,
            secondary_indices: SecondaryIndices::dmc_deserialize(buf)?,
        })
    }
}
#[derive(Serialize, Deserialize, Clone)]
pub struct PrimaryKeyIndexDef {

}

impl DMCSerialize for PrimaryKeyIndexDef {
    fn dmc_serialize(&self, _buf: &mut SerialBuffer) -> BuckyResult<()> {
        Ok(())
    }
}

impl DMCDeserialize for PrimaryKeyIndexDef {
    fn dmc_deserialize(_buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self{})
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ResourcePayer {
    pub payer: Name,
    pub max_net_bytes: u64,
    pub max_cpu_us: u64,
    pub max_memory_bytes: u64,
}

impl DMCSerialize for ResourcePayer {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        Name::dmc_serialize(&self.payer, buf)?;
        buf.push_u64(self.max_net_bytes);
        buf.push_u64(self.max_cpu_us);
        buf.push_u64(self.max_memory_bytes);
        Ok(())
    }
}

impl DMCDeserialize for ResourcePayer {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            payer: Name::dmc_deserialize(buf)?,
            max_net_bytes: buf.get_u64()?,
            max_cpu_us: buf.get_u64()?,
            max_memory_bytes: buf.get_u64()?
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PermissionLevel {
    pub actor: Name,
    pub permission: Name,
}
pub type Authorization = PermissionLevel;

impl DMCSerialize for PermissionLevel {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        Name::dmc_serialize(&self.actor, buf)?;
        Name::dmc_serialize(&self.permission, buf)?;
        Ok(())
    }
}

impl DMCDeserialize for PermissionLevel {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            actor: Name::dmc_deserialize(buf)?,
            permission: Name::dmc_deserialize(buf)?
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Action {
    pub account: Name,
    pub name: Name,
    pub authorization: Vec<PermissionLevel>,
    pub data: Vec<u8>,
    #[serde(skip)]
    pub hex_data: Option<String>
}

impl DMCSerialize for Action {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        Name::dmc_serialize(&self.account, buf)?;
        Name::dmc_serialize(&self.name, buf)?;
        buf.push_var_u32(self.authorization.len() as u32);
        for per in self.authorization.iter() {
            per.dmc_serialize(buf)?;
        }
        buf.push_bytes(self.data.as_slice());
        Ok(())
    }
}

impl DMCDeserialize for Action {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        let account = Name::dmc_deserialize(buf)?;
        let name = Name::dmc_deserialize(buf)?;
        let len = buf.get_var_u32()?;
        let mut authorization = Vec::new();
        for _ in 0..len {
            authorization.push(PermissionLevel::dmc_deserialize(buf)?);
        }
        let data = buf.get_bytes()?.to_vec();
        Ok(Self {
            account,
            name,
            authorization,
            data,
            hex_data: None,
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TransExtension {
    #[serde(rename="type")]
    pub ty: u16,
    pub data: Vec<u8>,
}

impl DMCSerialize for TransExtension  {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        buf.push_u16(self.ty);
        buf.push_bytes(self.data.as_slice());
        Ok(())
    }
}

impl DMCDeserialize for TransExtension {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            ty: buf.get_u16()?,
            data: buf.get_bytes()?.to_vec()
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TransHeader {
    pub expiration: TimePointSec,
    pub ref_block_num: u16,
    pub ref_block_prefix: u32,
    pub max_net_usage_words: u32,
    pub max_cpu_usage_ms: u8,
    pub delay_sec: u32,
}

impl DMCSerialize for TransHeader {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        time_point_sec_dmc_serialize(&self.expiration, buf)?;
        buf.push_u16(self.ref_block_num);
        buf.push_u32(self.ref_block_prefix);
        buf.push_var_u32(self.max_net_usage_words);
        buf.push_u8(self.max_cpu_usage_ms);
        buf.push_var_u32(self.delay_sec);
        Ok(())
    }
}

impl DMCDeserialize for TransHeader {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        Ok(Self {
            expiration: time_point_sec_dmc_deserialize(buf)?,
            ref_block_num: buf.get_u16()?,
            ref_block_prefix: buf.get_u32()?,
            max_net_usage_words: buf.get_var_u32()?,
            max_cpu_usage_ms: buf.get_u8()?,
            delay_sec: buf.get_var_u32()?
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Transaction {
    pub expiration: TimePointSec,
    pub ref_block_num: u16,
    pub ref_block_prefix: u32,
    pub max_net_usage_words: u32,
    pub max_cpu_usage_ms: u8,
    pub delay_sec: u32,
    pub context_free_actions: Vec<Action>,
    #[serde(skip)]
    pub context_free_data: Option<Vec<Vec<u8>>>,
    pub actions: Vec<Action>,
    pub transaction_extensions: Vec<TransExtension>,
    #[serde(skip)]
    pub resource_payer: Option<ResourcePayer>,
}

impl DMCSerialize for Transaction {
    fn dmc_serialize(&self, buf: &mut SerialBuffer) -> BuckyResult<()> {
        time_point_sec_dmc_serialize(&self.expiration, buf)?;
        buf.push_u16(self.ref_block_num);
        buf.push_u32(self.ref_block_prefix);
        buf.push_var_u32(self.max_net_usage_words);
        buf.push_u8(self.max_cpu_usage_ms);
        buf.push_var_u32(self.delay_sec);
        buf.push_var_u32(self.context_free_actions.len() as u32);
        for action in self.context_free_actions.iter() {
            action.dmc_serialize(buf)?;
        }
        buf.push_var_u32(self.actions.len() as u32);
        for action in self.actions.iter() {
            action.dmc_serialize(buf)?;
        }
        buf.push_var_u32(self.transaction_extensions.len() as u32);
        for extenstion in self.transaction_extensions.iter() {
            extenstion.dmc_serialize(buf)?;
        }

        Ok(())
    }
}

impl DMCDeserialize for Transaction {
    fn dmc_deserialize(buf: &mut SerialBuffer) -> BuckyResult<Self> {
        let expiration = time_point_sec_dmc_deserialize(buf)?;
        let ref_block_num = buf.get_u16()?;
        let ref_block_prefix = buf.get_u32()?;
        let max_net_usage_words = buf.get_var_u32()?;
        let max_cpu_usage_ms = buf.get_u8()?;
        let delay_sec = buf.get_var_u32()?;
        let len = buf.get_var_u32()?;
        let mut context_free_actions = Vec::new();
        for _ in 0..len {
            context_free_actions.push(Action::dmc_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut actions = Vec::new();
        for _ in 0..len {
            actions.push(Action::dmc_deserialize(buf)?);
        }
        let len = buf.get_var_u32()?;
        let mut transaction_extensions = Vec::new();
        for _ in 0..len {
            transaction_extensions.push(TransExtension::dmc_deserialize(buf)?);
        }

        Ok(Self {
            expiration,
            ref_block_num,
            ref_block_prefix,
            max_net_usage_words,
            max_cpu_usage_ms,
            delay_sec,
            context_free_actions,
            context_free_data: None,
            actions,
            transaction_extensions,
            resource_payer: None
        })
    }
}

pub fn reverse_hex(h: &str) -> String {
    format!("{}{}{}{}", &h[6..8], &h[4..6], &h[2..4], &h[0..2])
}

pub fn date_to_time_point(date: &str) -> BuckyResult<i64> {
    let time = DateTime::parse_from_rfc3339(format!("{}Z", date).as_str()).map_err(|e| {
        cyfs_err!(BuckyErrorCode::InvalidParam, "Invalid time format {} err {}", date, e)
    })?;

    Ok(time.timestamp())
}

pub fn time_point_to_date(us: i64) -> String {
    Utc.timestamp_millis_opt(us).unwrap().to_rfc3339_opts(SecondsFormat::Secs, true).trim_end_matches("Z").to_string()
}

pub fn time_point_sec_to_date(sec: i64) -> String {
    Utc.timestamp_opt(sec, 0).unwrap().to_rfc3339_opts(SecondsFormat::Secs, true).trim_end_matches("Z").to_string()
}

pub fn date_to_block_timestamp(date: &str) -> BuckyResult<i64> {
    let time = DateTime::parse_from_rfc3339(date).map_err(|e| {
        cyfs_err!(BuckyErrorCode::InvalidParam, "Invalid time format {} err {}", date, e)
    })?;
    Ok((time.timestamp_millis() - 946684800000) / 500)
}

pub fn block_timestamp_to_date(slot: i64) -> String {
    let time: String = Utc.timestamp_millis_opt(slot * 500 + 946684800000).unwrap().to_rfc3339_opts(SecondsFormat::Secs, true);
    time.trim_end_matches("Z").to_string()
}
