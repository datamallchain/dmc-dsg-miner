use std::str::FromStr;
use cyfs_base::{BuckyErrorCode, BuckyResult};
use regex::Regex;
use crate::*;

fn is_negative(bignum: &[u8]) -> bool {
    bignum[bignum.len() - 1] != 0
}

fn negate(bignum: &mut [u8]) {
    let mut carry: u32 = 1;
    for i in 0..bignum.len() {
        let num = bignum[i] as u32;
        let x = !num + carry;
        bignum[i] = x as u8;
        carry = x >> 8;
    }
}

fn decimal_to_binary(size: u8, s: &str) -> BuckyResult<Vec<u8>> {
    let mut result = Vec::<u8>::new();
    result.resize(size as usize, 0);
    for (_i, src_digit) in s.chars().enumerate() {
        if src_digit < '0' || src_digit > '9' {
            return Err(cyfs_err!(BuckyErrorCode::Failed, "invalid number {}", s));
        }

        let mut carry = (src_digit as u8 - '0' as u8) as u32;
        for j in 0..size {
            let x = result[j as usize] as u32 * 10 + carry as u32;
            result[j as usize] = x as u8;
            carry = x >> 8;
        }
        if carry != 0 {
            return Err(cyfs_err!(BuckyErrorCode::Failed, "number is out of range"));
        }
    }

    Ok(result)
}

fn signed_decimal_to_binary(size: u8, s: &str) -> BuckyResult<Vec<u8>> {
    let negative = s.chars().nth(0).unwrap() == '-';
    let ss = if negative {
        &s[1..]
    } else {
        s
    };

    let mut result = decimal_to_binary(size, ss)?;
    if negative {
        negate(result.as_mut_slice());
        if !is_negative(result.as_slice()) {
            return Err(cyfs_err!(BuckyErrorCode::Failed, "number is out of range"));
        }
    } else if is_negative(result.as_slice()) {
        return Err(cyfs_err!(BuckyErrorCode::Failed, "number is out of range"));
    }
    Ok(result)
}

pub struct SerialBuffer<'a> {
    array: &'a mut Vec<u8>,
    read_pos: usize,
    length: usize,
}

impl<'a> SerialBuffer<'a> {
    pub fn new(array: &'a mut Vec<u8>) -> Self {
        let length = array.len();
        Self {
            array,
            read_pos: 0,
            length
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.array[..self.length]
    }

    pub fn reserve(&mut self, size: usize) {
        self.array.reserve(size as usize);
    }

    pub fn have_read_data(&self) -> bool {
        self.read_pos < self.length
    }

    pub fn restart_read(&mut self) {
        self.read_pos = 0;
    }

    pub fn push_array(&mut self, new_array: &[u8]) {
        self.reserve(new_array.len());
        self.length += new_array.len() as usize;
        self.array.extend_from_slice(new_array);
    }

    pub fn get(&mut self) -> BuckyResult<&u8> {
        if self.read_pos < self.length {
            let ret = self.array.get(self.read_pos);
            self.read_pos += 1;
            Ok(ret.unwrap())
        } else {
            Err(cyfs_err!(BuckyErrorCode::Failed, "read_pos {} length {}", self.read_pos, self.length))
        }
    }

    pub fn push_u8(&mut self, v: u8) {
        self.reserve(1);
        self.length += 1;
        self.array.push(v);
    }

    pub fn get_u8(&mut self) -> BuckyResult<u8> {
        self.get().map(|i| *i)
    }

    pub fn get_array(&mut self, len: usize) -> BuckyResult<&[u8]> {
        if self.read_pos + len > self.length {
            Err(cyfs_err!(BuckyErrorCode::Failed, "read_pos {} length {}", self.read_pos + len, self.length))
        } else {
            let ret = &self.array[self.read_pos..self.read_pos + len];
            self.read_pos += len;
            Ok(ret)
        }
    }

    pub fn skip(&mut self, len: usize) {
        if self.read_pos + len > self.length {
            self.read_pos = self.length;
        } else {
            self.read_pos += len;
        }
    }

    pub fn push_u16(&mut self, v: u16) {
        self.push_array(v.to_le_bytes().as_slice())
    }

    pub fn get_u16(&mut self) -> BuckyResult<u16> {
        if self.read_pos + 2 > self.length {
            Err(cyfs_err!(BuckyErrorCode::Failed, "read_pos {} length {}", self.read_pos, self.length))
        } else {
            let mut b = [0u8; 2];
            b.copy_from_slice(&self.array[self.read_pos..self.read_pos+2]);
            self.read_pos += 2;
            Ok(u16::from_le_bytes(b))
        }
    }

    pub fn push_u32(&mut self, v: u32) {
        self.push_array(v.to_le_bytes().as_slice())
    }

    pub fn get_u32(&mut self) -> BuckyResult<u32> {
        if self.read_pos + 4 > self.length {
            Err(cyfs_err!(BuckyErrorCode::Failed, "read_pos {} length {}", self.read_pos, self.length))
        } else {
            let mut b = [0u8; 4];
            b.copy_from_slice(&self.array[self.read_pos..self.read_pos+4]);
            self.read_pos += 4;
            Ok(u32::from_le_bytes(b))
        }
    }

    pub fn push_u64(&mut self, v: u64) {
        self.push_array(v.to_le_bytes().as_slice());
    }

    pub fn get_u64(&mut self) -> BuckyResult<u64> {
        if self.read_pos + 8 > self.length {
            Err(cyfs_err!(BuckyErrorCode::Failed, "read_pos {} length {}", self.read_pos, self.length))
        } else {
            let mut b = [0u8; 8];
            b.copy_from_slice(&self.array[self.read_pos..self.read_pos+8]);
            self.read_pos += 8;
            Ok(u64::from_le_bytes(b))
        }
    }

    pub fn push_var_u32(&mut self, mut v: u32) {
        loop {
            if v >> 7 != 0 {
                self.push_u8((0x80 | (v & 0x7f)) as u8);
                v = v >> 7;
            } else {
                self.push_u8(v as u8);
                break;
            }
        }
    }

    pub fn get_var_u32(&mut self) -> BuckyResult<u32> {
        let mut v = 0;
        let mut bit = 0;
        loop {
            let b = self.get_u8()?;

            v |= ((b & 0x7f) as u32) << bit as u32;
            bit += 7;
            if b & 0x80 == 0 {
                break;
            }
        }
        Ok(v)
    }

    pub fn push_var_int32(&mut self, v: i32) {
        self.push_var_u32(((v << 1) ^ (v >> 31)) as u32)
    }

    pub fn get_var_int32(&mut self) -> BuckyResult<i32> {
        let v = self.get_var_u32()?;
        if v & 1 != 0 {
            Ok((!v >> 1 | 0x8000_0000) as i32)
        } else {
            Ok( (v >> 1) as i32)
        }
    }

    pub fn push_f32(&mut self, v: f32) {
        self.push_array(v.to_le_bytes().as_slice());
    }

    pub fn get_f32(&mut self) -> BuckyResult<f32> {
        if self.read_pos + 4 > self.length {
            Err(cyfs_err!(BuckyErrorCode::Failed, "read_pos {} length {}", self.read_pos, self.length))
        } else {
            let mut b = [0u8; 4];
            b.copy_from_slice(&self.array[self.read_pos..self.read_pos+4]);
            self.read_pos += 4;
            Ok(f32::from_le_bytes(b))
        }
    }

    pub fn push_f64(&mut self, v: f64) {
        self.push_array(v.to_le_bytes().as_slice());
    }

    pub fn get_f64(&mut self) -> BuckyResult<f64> {
        if self.read_pos + 8 > self.length {
            Err(cyfs_err!(BuckyErrorCode::Failed, "read_pos {} length {}", self.read_pos, self.length))
        } else {
            let mut b = [0u8; 8];
            b.copy_from_slice(&self.array[self.read_pos..self.read_pos+8]);
            self.read_pos += 8;
            Ok(f64::from_le_bytes(b))
        }
    }

    fn char_to_symbol(c: char) -> u8 {
        if c >= 'a' && c <= 'z' {
            (c as u8 - 'a' as u8 + 6) as u8
        } else if c >= '1' && c <= '5' {
            (c as u8 - '1' as u8 + 1) as u8
        } else {
            0
        }
    }

    pub fn push_name(&mut self, s: &str) -> BuckyResult<()> {
        let regex = Regex::new(r#"^[.1-5a-z]{0,12}[.1-5a-j]?$"#).unwrap();
        if !regex.is_match(s) {
            return Err(cyfs_err!(BuckyErrorCode::Failed, "Name should be less than 13 characters, or less than 14 if last character is between 1-5 or a-j, and only contain the following symbols .12345abcdefghijklmnopqrstuvwxyz"))
        }

        let mut a = Vec::with_capacity(8);
        a.resize(8, 0);
        let mut bit = 63;
        for i in s.chars() {
            let mut c = Self::char_to_symbol(i);
            if bit < 5 {
                c = c << 1;
            }
            for j in (0..5).rev() {
                if bit >= 0 {
                    a[(bit as f32/8f32).floor() as usize] |= ((c >> j) & 1) << (bit % 8);
                    bit -= 1;
                }
            }
        }
        self.push_array(a.as_slice());
        Ok(())
    }

    pub fn get_name(&mut self) -> BuckyResult<String> {
        let a = self.get_array(8)?;
        let mut bit = 63;
        let mut result = Vec::<u8>::new();
        while bit > 0 {
            let mut c = 0;
            for _ in 0..5 {
                if bit >= 0 {
                    c = (c << 1) | ((a[(bit as f32/8f32).floor() as usize] >> (bit % 8)) & 1);
                    bit -= 1;
                }
            }
            if c >= 6 {
                result.push(c + 'a' as u8 - 6);
            } else if c >= 1 {
                result.push( c + '1' as u8 - 1);
            } else {
                result.push('.' as u8);
            }
        }

        let mut result = String::from_utf8_lossy(result.as_slice()).to_string();
        result = result.trim_end_matches(".").to_string();
        Ok(result)
    }

    pub fn push_bytes(&mut self, v: &[u8]) {
        self.push_var_u32(v.len() as u32);
        self.push_array(v);
    }

    pub fn get_bytes(&mut self) -> BuckyResult<&[u8]> {
        let len = self.get_var_u32()?;
        self.get_array(len as usize)
    }

    pub fn push_string(&mut self, v: &str) {
        self.push_var_u32(v.as_bytes().len() as u32);
        self.push_array(v.as_bytes());
    }

    pub fn get_string(&mut self) -> BuckyResult<String> {
        let len = self.get_var_u32()?;
        let array = self.get_array(len as usize)?;
        Ok(String::from_utf8_lossy(array).to_string())
    }

    pub fn push_symbol_code(&mut self, name: &str) {
        let mut buf = name.as_bytes().to_vec();
        if buf.len() < 8 {
            buf.push(0);
        }
        self.push_array(&buf[0..8]);
    }

    pub fn get_symbol_code(&mut self) -> BuckyResult<String> {
        let a = self.get_array(8)?;
        let mut i = 0;
        while i < a.len() && a[i] != 0 {
            i += 1;
        }
        Ok(String::from_utf8_lossy(&a[..i]).to_string())
    }

    pub fn push_symbol(&mut self, name: &str, precision: u8) -> BuckyResult<()> {
        let regex = Regex::new(r#"^[A-Z]{1,7}$"#).unwrap();
        if !regex.is_match(name) {
            return Err(cyfs_err!(BuckyErrorCode::Failed, "Expected symbol to be A-Z and between one and seven characters"));
        }

        let mut a = vec![precision];
        a.extend_from_slice(name.as_bytes());
        while a.len() < 8 {
            a.push(0);
        }

        self.push_array(&a[..8]);
        Ok(())
    }

    pub fn get_symbol(&mut self) -> BuckyResult<(String, u8)> {
        let precision = self.get()?;
        let precision = *precision;

        let a = self.get_array(7)?;
        let mut i = 0;
        while i < a.len() && a[i] != 0 {
            i += 1;
        }

        let name = String::from_utf8_lossy(&a[..i]).to_string();
        Ok((name, precision))
    }

    pub fn push_assert(&mut self, s: &str) -> BuckyResult<()> {
        let s = s.trim();
        let mut pos = 0;
        let mut amount = "".to_string();
        let mut precision = 0;

        let s: Vec<char> = s.chars().collect();
        if s[pos] == '-' {
            amount += "-";
            pos += 1;
        }

        let mut found_digit = false;
        while pos < s.len() && s[pos] >= '0' && s[pos] <= '9' {
            found_digit = true;
            amount += s[pos].to_string().as_str();
            pos += 1;
        }

        if !found_digit {
            return Err(cyfs_err!(BuckyErrorCode::Failed, "Asset must begin with a number"));
        }

        if s[pos] == '.' {
            pos += 1;
            while pos < s.len() && s[pos] >= '0' && s[pos] <= '9' {
                amount += s[pos].to_string().as_str();
                precision += 1;
                pos += 1;
            }
        }

        let name: String = s[pos..].iter().collect();
        let name1 = name.trim();
        self.push_array(signed_decimal_to_binary(8, amount.as_str())?.as_slice());
        self.push_symbol(name1, precision)?;

        Ok(())
    }

    pub fn get_asset(&mut self) -> BuckyResult<String> {
        let amount = self.get_array(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(amount);

        let symbol = self.get_symbol()?;
        let (name, precision) = symbol;
        let mut s = format!("{}", i64::from_be_bytes(a));
        if precision > 0 {
            let (first, second) = s.split_at(precision as usize);
            s = format!("{}.{}", first, second);
        }
        Ok(format!("{} {}", s, name))
    }

    pub fn push_public_key(&mut self, s: &str) -> BuckyResult<()> {
        let public_key = DMCPublicKey::from_str(s)?;
        self.push_u8(public_key.key_type as u8);
        self.push_array(public_key.key.serialize_compressed().as_slice());
        Ok(())
    }

    pub fn get_public_key(&mut self) -> BuckyResult<String> {
        let key_type = self.get()?;
        let key_type = *key_type;
        if key_type == 0 || key_type == 1{
            let key_data = self.get_array(libsecp256k1::util::COMPRESSED_PUBLIC_KEY_SIZE)?;
            let key_type = if key_type == 0 {
                KeyType::K1
            } else {
                KeyType::R1
            };

            let key = libsecp256k1::PublicKey::parse_slice(key_data, None);
            if key.is_err() {
                return Err(cyfs_err!(BuckyErrorCode::Failed, "parse public key failed"));
            }

            let public_key = DMCPublicKey {
                key_type,
                key: key.unwrap()
            };

            Ok(public_key.to_string())
        } else if key_type == 2 {
            return Err(cyfs_err!(BuckyErrorCode::NotSupport, "not support"));
        } else {
            return Err(cyfs_err!(BuckyErrorCode::NotSupport, "not support"));
        }
    }

    pub fn push_private_key(&mut self, s: &str) -> BuckyResult<()> {
        let private_key = DMCPrivateKey::from_str(s)?;
        self.push_u8(private_key.key_type as u8);
        self.push_array(private_key.key.serialize().as_slice());
        Ok(())
    }

    pub fn get_private_key(&mut self) -> BuckyResult<String> {
        let key_type = self.get()?;
        let key_type = match *key_type {
            0 => KeyType::K1,
            1 => KeyType::R1,
            2 => KeyType::Wa,
            v @ _ => {
                return Err(cyfs_err!(BuckyErrorCode::Failed, "unsupport {} key type", v));
            }
        };

        let key = self.get_array(libsecp256k1::util::SECRET_KEY_SIZE as usize)?;
        let key = libsecp256k1::SecretKey::parse_slice(key).map_err(|e| {
            cyfs_err!(BuckyErrorCode::Failed, "parse key err {}", e)
        })?;

        let private_key = DMCPrivateKey {
            key_type,
            key
        };
        private_key.to_string()
    }
}
