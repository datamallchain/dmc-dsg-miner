use std::convert::{TryInto};
use std::io::SeekFrom;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use sha2::{Digest};
use cyfs_base::*;
use async_std::io::Cursor;
use async_std::io::prelude::SeekExt;
use async_std::io::ReadExt;
use memmap2::MmapMut;

#[async_trait::async_trait]
pub trait HashStore: Send + Sync {
    async fn get_node_list_len(&self, layer_number: u16) -> BuckyResult<u64>;
    async fn get_node(&self, layer_number: u16, index: u64) -> BuckyResult<&[u8;32]>;
    async fn set_node(&mut self, layer_number: u16, index: u64, hash: &[u8;32]) -> BuckyResult<()>;
    async fn get_min_layer_number(&self) -> BuckyResult<u16>;
}

pub trait VecCache<T: Send + Sync + Deref<Target=[u8]> + DerefMut<Target=[u8]>>: Send + Sync {
    fn create(len: u64) -> BuckyResult<T>;
}

pub struct MemVecCache;

impl VecCache<Vec<u8>> for MemVecCache {
    fn create(len: u64) -> BuckyResult<Vec<u8>> {
        let mut cache = Vec::with_capacity(len as usize);
        cache.resize(len as usize, 0);
        Ok(cache)
    }
}

pub struct MmapVecCache;

pub fn get_tmp_path() -> BuckyResult<PathBuf> {
    let file = tempfile::NamedTempFile::new()?;
    let save_path = file.path();
    Ok(save_path.to_owned())
}

impl VecCache<MmapMut> for MmapVecCache {
    fn create(len: u64) -> BuckyResult<MmapMut> {
        unsafe {
            let file = tempfile::NamedTempFile::new().map_err(|e| {
                let msg = format!("[{}:{}] open file failed.err {}", file!(), line!().to_string(), e);
                log::error!("{}", msg.as_str());
                BuckyError::new(BuckyErrorCode::Failed, msg)
            })?;
            file.as_file().set_len(len).map_err(|e| {
                let msg = format!("[{}:{}] set file  len {} failed.err {}", file!(), line!(), len, e);
                log::error!("{}", msg.as_str());
                BuckyError::new(BuckyErrorCode::Failed, msg)
            })?;
            let mmap = MmapMut::map_mut(&file).map_err(|e| {
                let msg = format!("[{}:{}] create file map failed.err {}", file!(), line!(), e);
                log::error!("{}", msg.as_str());
                BuckyError::new(BuckyErrorCode::Failed, msg)
            })?;
            Ok(mmap)
        }
    }
}

pub struct HashVecStore<T: Send + Sync + Deref<Target=[u8]> + DerefMut<Target=[u8]>> {
    layer_info: Vec<(u64, u64)>,
    cache: T,
    min_layer: u16,
    min_offset: u64,
}

impl <T: Send + Sync + Deref<Target=[u8]> + DerefMut<Target=[u8]>> HashVecStore<T> {
    pub fn new<C: VecCache<T>>(leafs: u64) -> BuckyResult<Self> {
        let mut count  = leafs;
        let mut cur_nodes = leafs;
        let mut layer_info = vec![(0, leafs)];
        loop {
            if cur_nodes == 1 {
                break;
            }
            if cur_nodes % 2 == 0 {
                layer_info.push((count, cur_nodes / 2));
                count += cur_nodes / 2;
                cur_nodes = cur_nodes / 2;
            } else {
                layer_info.push((count, (cur_nodes + 1) / 2));
                count += (cur_nodes + 1) / 2;
                cur_nodes = (cur_nodes + 1) / 2;
            }
        }
        let cache = C::create(count * 32)?;
        Ok(Self {
            layer_info,
            cache,
            min_layer: 0,
            min_offset: 0,
        })
    }

    pub fn get_data(&self, min_layer: u16) -> BuckyResult<&[u8]> {
        match self.layer_info.get(min_layer as usize) {
            Some((offset, _)) => {
                Ok(&self.cache[(*offset - self.min_offset) as usize * 32..])
            },
            None => {
                let msg = format!("can't find layer {}", min_layer);
                log::error!("{}", msg.as_str());

                Err(BuckyError::new(BuckyErrorCode::NotFound, msg))
            }
        }
    }

    pub fn load(leafs: u64, data: T) -> BuckyResult<Self> {
        let mut count  = leafs;
        let mut cur_nodes = leafs;
        let mut layer_info = vec![(0, leafs)];
        loop {
            if cur_nodes == 1 {
                break;
            }
            if cur_nodes % 2 == 0 {
                layer_info.push((count, cur_nodes / 2));
                count += cur_nodes / 2;
                cur_nodes = cur_nodes / 2;
            } else {
                layer_info.push((count, (cur_nodes + 1) / 2));
                count += (cur_nodes + 1) / 2;
                cur_nodes = (cur_nodes + 1) / 2;
            }
        }
        let mut count = 0;
        let mut min_layer = 0;
        let mut min_offset = 0;
        for (layer, (offset, len)) in layer_info.iter().enumerate().rev() {
            count += *len;
            if count as usize * 32 == data.len() {
                min_layer = layer as u16;
                min_offset = *offset;
                break;
            } else if count as usize * 32 > data.len() {
                let msg = format!("data len is invalid");
                log::info!("{}", msg);
                return Err(BuckyError::new(BuckyErrorCode::InvalidData, msg))
            }
        }
        Ok(Self {
            layer_info,
            cache: data,
            min_layer,
            min_offset,
        })
    }
}

#[async_trait::async_trait]
impl <T: Send + Sync + Deref<Target=[u8]> + DerefMut<Target=[u8]>> HashStore for HashVecStore<T> {
    async fn get_node_list_len(&self, layer_number: u16) -> BuckyResult<u64> {
        match self.layer_info.get(layer_number as usize) {
            Some((_, len)) => {
                Ok(*len)
            },
            None => {
                let msg = format!("can't find layer {}", layer_number);
                log::error!("{}", msg.as_str());

                Err(BuckyError::new(BuckyErrorCode::NotFound, msg))
            }
        }
    }

    async fn get_node(&self, layer_number: u16, index: u64) -> BuckyResult<&[u8;32]> {
        match self.layer_info.get( layer_number as usize) {
            Some((offset, len)) => {
                if index >= *len {
                    let msg = format!("can't find index {} at {}", index, layer_number);
                    log::error!("{}", msg.as_str());

                    Err(BuckyError::new(BuckyErrorCode::NotFound, msg))
                } else {
                    let hash = (&self.cache[(*offset + index - self.min_offset) as usize * 32..(*offset + index - self.min_offset + 1) as usize * 32]).try_into().unwrap();
                    Ok(hash)
                }
            },
            None => {
                let msg = format!("can't find layer {}", layer_number);
                log::error!("{}", msg.as_str());

                Err(BuckyError::new(BuckyErrorCode::NotFound, msg))
            }
        }
    }

    async fn set_node(&mut self, layer_number: u16, index: u64, hash: &[u8;32]) -> BuckyResult<()> {
        log::info!("set node layer {} index {} hash {}", layer_number, index, hex::encode(hash));
        match self.layer_info.get( layer_number as usize) {
            Some((offset, len)) => {
                if index >= *len {
                    let msg = format!("can't find index {} at {}", index, layer_number);
                    log::error!("{}", msg.as_str());

                    Err(BuckyError::new(BuckyErrorCode::NotFound, msg))
                } else {
                    self.cache[(*offset + index - self.min_offset) as usize * 32..(*offset + index - self.min_offset + 1) as usize * 32].copy_from_slice(hash);
                    Ok(())
                }
            },
            None => {
                let msg = format!("can't find layer {}", layer_number);
                log::error!("{}", msg.as_str());

                Err(BuckyError::new(BuckyErrorCode::NotFound, msg))
            }
        }
    }

    async fn get_min_layer_number(&self) -> BuckyResult<u16> {
        Ok(self.min_layer)
    }
}

pub struct MerkleTree<READ: async_std::io::Read + async_std::io::Seek + Send, CACHE: HashStore> {
    cache: CACHE,
    reader: READ,
    root: [u8;32],
}

pub const DSG_CHUNK_PIECE_SIZE: u64 = 1024;

impl <READ: async_std::io::Read + async_std::io::Seek + Send + Unpin, CACHE: HashStore> MerkleTree<READ, CACHE> {
    fn hash(left: &[u8], right: &[u8]) -> [u8;32] {
        let mut sha256 = sha2::Sha256::new();
        sha256.update(left);
        sha256.update(right);
        sha256.finalize().into()
    }

    fn hash_data(data: &[u8]) -> [u8;32] {
        let mut sha256 = sha2::Sha256::new();
        sha256.update(data);
        sha256.finalize().into()
    }

    pub async fn create_from_raw(mut reader: READ, mut cache: CACHE) -> BuckyResult<Self> {
        let mut index = 0;
        let mut buf = Vec::<u8>::new();
        buf.resize(1024*1024*4, 0);
        loop {
            let mut pos = 0;
            let read_size = loop {
                let read_size = reader.read(&mut buf[pos..]).await.map_err(|e| {
                    let msg = format!("read error {}", e);
                    log::error!("{}", msg);
                    BuckyError::new(BuckyErrorCode::Failed, msg)
                })?;

                if read_size == 0 || buf[pos + read_size..].len() == 0 {
                    break pos + read_size;
                }
                pos += read_size;
            };
            if read_size == 0 {
                break;
            }

            let mut tmp_buf = &mut buf[0..read_size];
            loop {
                if tmp_buf.len() <= DSG_CHUNK_PIECE_SIZE as usize {
                    let hash = Self::hash_data(tmp_buf);
                    cache.set_node(0, index, &hash).await?;
                    index += 1;
                    break;
                } else {
                    let hash = Self::hash_data(&tmp_buf[..DSG_CHUNK_PIECE_SIZE as usize]);
                    cache.set_node(0, index, &hash).await?;
                    index += 1;
                    tmp_buf = &mut tmp_buf[DSG_CHUNK_PIECE_SIZE as usize..];
                }
            }
        }

        let root;
        let mut layer_number = 1;
        loop {
            let node_len = cache.get_node_list_len(layer_number - 1).await?;
            if node_len == 1 {
                root = cache.get_node(layer_number - 1, 0).await?;
                break;
            }
            let mut i = 0;
            let mut index = 0;
            while i < node_len {
                let left = Some(cache.get_node(layer_number - 1, i).await?);
                let right = if i + 1 < node_len {
                    Some(cache.get_node(layer_number - 1, i + 1).await?)
                } else {
                    None
                };

                let hash:[u8;32] = if right.is_some() {
                    Self::hash(left.as_ref().unwrap().as_slice(), right.as_ref().unwrap().as_slice())
                } else {
                    Self::hash(left.as_ref().unwrap().as_slice(), left.as_ref().unwrap().as_slice())
                };
                cache.set_node(layer_number, index, &hash).await?;
                index += 1;
                i += 2;
            }
            layer_number += 1;
        }
        let root = root.clone();
        Ok(Self {
            reader,
            cache,
            root
        })
    }

    pub async fn create_from_base(reader: READ, mut cache: CACHE, base_layer: u16) -> BuckyResult<Self> {
        let root;
        let mut layer_number = base_layer + 1;
        loop {
            let node_len = cache.get_node_list_len(layer_number - 1).await?;
            if node_len == 1 {
                root = cache.get_node(layer_number - 1, 0).await?;
                break;
            }
            let mut i = 0;
            let mut index = 0;
            while i < node_len {
                let left = Some(cache.get_node(layer_number - 1, i).await?);
                let right = if i + 1 < node_len {
                    Some(cache.get_node(layer_number - 1, i + 1).await?)
                } else {
                    None
                };

                let hash:[u8;32] = if right.is_some() {
                    Self::hash(left.as_ref().unwrap().as_slice(), right.as_ref().unwrap().as_slice())
                } else {
                    Self::hash(left.as_ref().unwrap().as_slice(), left.as_ref().unwrap().as_slice())
                };
                cache.set_node(layer_number, index, &hash).await?;
                index += 1;
                i += 2;
            }
            layer_number += 1;
        }
        let root = root.clone();
        Ok(Self {
            reader,
            cache,
            root
        })
    }

    pub async fn load(reader: READ, cache: CACHE) -> BuckyResult<Self> {
        let min_layer = cache.get_min_layer_number().await?;
        let mut layer = min_layer;
        let root;
        loop {
            if cache.get_node_list_len(layer).await? == 1 {
                root = cache.get_node(layer, 0).await?;
                break;
            }
            layer += 1;
        }
        let root = root.clone();
        Ok(Self {
            cache,
            reader,
            root
        })
    }

    pub fn root(&self) -> &[u8;32] {
        &self.root
    }

    async fn read(&mut self, pos: u64, len: usize) -> BuckyResult<(usize, Vec<u8>)> {
        let mut buf = Vec::<u8>::new();
        buf.resize(len as usize, 0);

        self.reader.seek(SeekFrom::Start(pos as u64)).await.map_err(|e| {
            let msg = format!("merkle tree seek err {}", e);
            log::error!("{}", msg.as_str());
            BuckyError::new(BuckyErrorCode::Failed, msg)
        })?;

        let size = self.reader.read(&mut buf).await.map_err(|e| {
            let msg = format!("merkle tree read err {}", e);
            log::error!("{}", msg.as_str());
            BuckyError::new(BuckyErrorCode::Failed, msg)
        })?;

        Ok((size, buf))
    }

    #[async_recursion::async_recursion]
    async fn gen_proof_tree_path(&mut self, index: u64) -> BuckyResult<(Vec<[u8;32]>, Vec<u8>)> {
        let piece_pos = index as u64 * DSG_CHUNK_PIECE_SIZE as u64;
        let min_layer = self.cache.get_min_layer_number().await?;
        let (mut cur_index, mut cur_layer, mut path_list, piece) = if min_layer == 0 {
            let (size, buf) = self.read(piece_pos as u64, DSG_CHUNK_PIECE_SIZE as usize).await?;
            let piece = if size == DSG_CHUNK_PIECE_SIZE as usize {
                buf
            } else {
                buf[..size].to_vec()
            };
            (index, min_layer, Vec::new(), piece)
        } else {
            let read_len = 2u64.pow(min_layer as u32) * DSG_CHUNK_PIECE_SIZE as u64;
            let read_pos = (piece_pos / read_len) * read_len;

            let mut buf = Vec::<u8>::with_capacity(read_len as usize);

            self.reader.seek(SeekFrom::Start(read_pos as u64)).await.map_err(|e| {
                let msg = format!("merkle tree seek err {}", e);
                log::error!("{}", msg.as_str());
                BuckyError::new(BuckyErrorCode::Failed, msg)
            })?;

            let size = self.reader.by_ref().take(read_len).read_to_end(&mut buf).await.map_err(|e| {
                let msg = format!("merkle tree read err {}", e);
                log::error!("{}", msg.as_str());
                BuckyError::new(BuckyErrorCode::Failed, msg)
            })?;

            let sub_leafs = if size as u64 % DSG_CHUNK_PIECE_SIZE == 0 {
                size as u64 / DSG_CHUNK_PIECE_SIZE
            } else {
                size as u64 / DSG_CHUNK_PIECE_SIZE + 1
            };
            let mut sub_tree = MerkleTree::create_from_raw(Cursor::new(&buf[0..size]), HashVecStore::<Vec<u8>>::new::<MemVecCache>(sub_leafs)?).await?;
            let sub_index = index - read_pos / DSG_CHUNK_PIECE_SIZE;
            let (mut sub_path_list, piece) = sub_tree.gen_proof_tree_path(sub_index).await?;

            if sub_path_list.len() < min_layer as usize {
                let mut cur_node = Self::hash_data(piece.as_slice());
                let mut cur_index = sub_index;
                for node in sub_path_list.iter() {
                    if cur_index % 2 != 0 {
                        cur_node = Self::hash(node, &cur_node);
                    } else {
                        cur_node = Self::hash(&cur_node, node);
                    }

                    cur_index = cur_index / 2;
                }

                loop {
                    sub_path_list.push(cur_node.clone());
                    if sub_path_list.len() >= min_layer as usize {
                        break;
                    }
                    cur_node = Self::hash(&cur_node, &cur_node);
                }

            }
            let mut cur_index = index;
            for _ in 0..min_layer {
                cur_index = cur_index / 2;
            }
            (cur_index, min_layer, sub_path_list, piece)
        };

        loop {
            if self.cache.get_node_list_len(cur_layer).await? == 1 {
                break;
            }
            if cur_index % 2 != 0 {
                path_list.push(self.cache.get_node(cur_layer, cur_index - 1).await?.clone());
            } else if self.cache.get_node_list_len(cur_layer).await? > cur_index + 1 {
                path_list.push(self.cache.get_node(cur_layer, cur_index + 1).await?.clone());
            } else {
                path_list.push(self.cache.get_node(cur_layer, cur_index).await?.clone());
            }
            cur_index = cur_index / 2;
            cur_layer += 1;
        }
        Ok((path_list, piece))
    }

    pub async fn gen_proof(&mut self, index: u64) -> BuckyResult<SinglePieceProof> {
        let (path_list, piece) = self.gen_proof_tree_path(index).await?;
        Ok(SinglePieceProof {
            piece_index: index,
            piece,
            path_list
        })
    }

    pub fn get_cache(&self) -> &CACHE {
        &self.cache
    }
}

#[derive(Clone)]
pub struct SinglePieceProof {
    pub piece_index: u64,
    pub piece: Vec<u8>,
    pub path_list: Vec<[u8;32]>,
}

impl RawEncode for SinglePieceProof {
    fn raw_measure(&self, purpose: &Option<RawEncodePurpose>) -> BuckyResult<usize> {
        let mut size = size_of::<u64>();
        size += USize(self.piece.len()).raw_measure(purpose)? + self.piece.len();
        size += USize(self.path_list.len()).raw_measure(purpose)? + self.path_list.len() * 32;
        Ok(size)
    }

    fn raw_encode<'a>(&self, buf: &'a mut [u8], _purpose: &Option<RawEncodePurpose>) -> BuckyResult<&'a mut [u8]> {
        let buf = self.piece_index.raw_encode(buf, _purpose)?;
        let buf = USize(self.piece.len()).raw_encode(buf, _purpose)?;
        buf[..self.piece.len()].copy_from_slice(self.piece.as_slice());
        let buf = &mut buf[self.piece.len()..];
        let mut buf = USize(self.path_list.len()).raw_encode(buf, _purpose)?;
        for item in self.path_list.iter() {
            if buf.len() < 32 {
                log::error!("out of limit");
                return Err(BuckyError::new(BuckyErrorCode::OutOfLimit, "out of limit"));
            }
            buf[..32].copy_from_slice(item.as_slice());
            buf = &mut buf[32..];
        }
        Ok(buf)
    }
}

impl <'de> RawDecode<'de> for SinglePieceProof {
    fn raw_decode(buf: &'de [u8]) -> BuckyResult<(Self, &'de [u8])> {
        let (piece_index, buf) = u64::raw_decode(buf)?;
        let (len, buf) = USize::raw_decode(buf)?;
        if buf.len() < len.0 {
            log::error!("out of limit");
            return Err(BuckyError::new(BuckyErrorCode::OutOfLimit, "out of limit"));
        }
        let piece = buf[..len.0].to_vec();
        let buf = &buf[len.0..];
        let (len, mut buf) = USize::raw_decode(buf)?;
        if buf.len() < len.0 * 32 {
            log::error!("out of limit");
            return Err(BuckyError::new(BuckyErrorCode::OutOfLimit, "out of limit"));
        }
        let mut path_list = Vec::new();
        for _ in 0..len.0 {
            let hash: [u8;32] = (&buf[..32]).try_into().unwrap();
            buf = &buf[32..];
            path_list.push(hash);
        }
        Ok((Self {
            piece_index,
            piece,
            path_list
        }, buf))
    }
}

impl SinglePieceProof {
    pub fn verify(&self, root: &[u8;32]) -> bool {
        let mut cur_node: [u8;32] = {
            let mut sha256 = sha2::Sha256::new();
            sha256.update(self.piece.as_slice());
            sha256.finalize().into()
        };
        let mut cur_index = self.piece_index;
        for node in self.path_list.iter() {
            let mut sha256 = sha2::Sha256::new();
            if cur_index % 2 != 0 {
                sha256.update(node);
                sha256.update(&cur_node);
                cur_node = sha256.finalize().into();
            } else {
                sha256.update(&cur_node);
                sha256.update(node);
                cur_node = sha256.finalize().into();
            }

            cur_index = cur_index / 2;
        }

        &cur_node == root
    }
}

#[cfg(test)]
mod test_merkle {
    use crate::{DSG_CHUNK_PIECE_SIZE, HashStore, HashVecStore, MemVecCache, MerkleTree};
    use async_std::io::Cursor;
    use cyfs_base::HashValue;

    #[test]
    fn test() {
        async_std::task::block_on(async move {
            let mut buf = Vec::<u8>::new();
            let len = 1024 * 1024 + rand::random::<usize>() % (10 * 1024 * 1024);
            buf.resize(len, 0);

            for i in 0..len {
                buf[i] = rand::random();
            }

            let max_index = if len % DSG_CHUNK_PIECE_SIZE as usize == 0 {
                len / DSG_CHUNK_PIECE_SIZE as usize
            } else {
                len / DSG_CHUNK_PIECE_SIZE as usize + 1
            };

            let mut tree = MerkleTree::create_from_raw(Cursor::new(buf.as_slice()), HashVecStore::<Vec<u8>>::new::<MemVecCache>(max_index as u64).unwrap()).await.unwrap();
            let root = tree.root().clone();

            println!("{}", HashValue::from(&root).to_string());

            let proof = tree.gen_proof(0).await.unwrap();
            assert!(proof.verify(&root));
            let proof = tree.gen_proof(max_index as u64 - 1).await.unwrap();
            assert!(proof.verify(&root));
            for _ in 0..100 {
                let proof = tree.gen_proof(rand::random::<u64>() % max_index as u64).await.unwrap();
                assert!(proof.verify(&root));
            }

            let mut i = 3;
            let mut new_cache = HashVecStore::load(max_index as u64, tree.cache.get_data(i as u16).unwrap().to_vec()).unwrap();
            assert_eq!(new_cache.get_min_layer_number().await.unwrap(), 3);

            let mut tree = MerkleTree::load(Cursor::new(buf.as_slice()), new_cache).await.unwrap();
            assert_eq!(tree.root(), &root);

            let proof = tree.gen_proof(0).await.unwrap();
            assert!(proof.verify(&root));
            let proof = tree.gen_proof(max_index as u64 - 1).await.unwrap();
            assert!(proof.verify(&root));
            for _ in 0..100 {
                let proof = tree.gen_proof(rand::random::<u64>() % max_index as u64).await.unwrap();
                assert!(proof.verify(&root));
            }

        });
    }
}
