use std::{
    fs::OpenOptions,
    io::Read,
    os::unix::prelude::OpenOptionsExt,
    path::{Path, PathBuf},
};

use prost::Message;

use crate::server::log::Record;

use super::{config::Config, index::Index, store::Store};

#[derive(Debug)]
pub struct Segment {
    store: Option<Store>,
    index: Option<Index>,
    pub base_offset: u64,
    pub next_offset: u64,
    config: Config,
    index_name: PathBuf,
    store_name: PathBuf,
}

impl Segment {
    pub fn new<P: AsRef<Path>>(dir: P, base_offset: u64, c: Config) -> std::io::Result<Self> {
        let store_filename = format!("{}{}", base_offset, ".store");
        let dir = dir.as_ref();
        let store_path = dir.join(&store_filename);
        let store_file = OpenOptions::new()
            .mode(0o644)
            .append(true)
            .read(true)
            .create(true)
            .open(&store_path)?;
        let store = Store::new(store_file)?;

        let index_filename = format!("{}{}", base_offset, ".index");
        let index_path = dir.join(&index_filename);
        let index_file = OpenOptions::new()
            .mode(0o644)
            .append(true)
            .read(true)
            .create(true)
            .open(&index_path)?;
        let index = Index::new(index_file, c, &index_path)?;

        let next_offset = if let Ok((offset, _pos)) = index.read(-1) {
            base_offset + offset as u64 + 1
        } else {
            base_offset
        };

        let segment = Segment {
            base_offset,
            next_offset,
            config: c,
            index: Some(index),
            store: Some(store),
            index_name: index_path,
            store_name: store_path,
        };

        Ok(segment)
    }

    #[inline]
    fn get_store_and_index_mut(&mut self) -> Option<(&mut Store, &mut Index)> {
        let store = match self.store {
            Some(ref mut store) => store,
            None => return None,
        };
        let index = match self.index {
            Some(ref mut index) => index,
            None => return None,
        };
        Some((store, index))
    }

    #[inline]
    fn get_store_and_index(&self) -> Option<(&Store, &Index)> {
        let store = match self.store {
            Some(ref store) => store,
            None => return None,
        };
        let index = match self.index {
            Some(ref index) => index,
            None => return None,
        };
        Some((store, index))
    }

    pub fn append(&mut self, mut record: Record) -> std::io::Result<Option<u64>> {
        let cur = self.next_offset;
        record.offset = cur;
        let offset = (self.next_offset - self.base_offset) as u32;
        let (store, index) = match self.get_store_and_index_mut() {
            Some(val) => val,
            None => return Ok(None),
        };
        let mut buf: Vec<u8> = Vec::new();
        record.encode(&mut buf)?;
        let (_, pos) = store.append(&buf)?;
        index.write(offset, pos)?;
        self.next_offset += 1;
        Ok(Some(cur))
    }

    pub fn read_at_offset(&mut self, offset: u64) -> std::io::Result<Option<Record>> {
        let base_offset = self.base_offset;
        let (store, index) = match self.get_store_and_index_mut() {
            Some(val) => val,
            None => return Ok(None),
        };
        let (_, pos) = index.read((offset - base_offset) as i64)?;
        let buf = store.read_at_offset(pos)?;
        let record: Record = Message::decode(&buf[..])?;
        Ok(Some(record))
    }

    #[inline]
    pub fn is_maxed(&self) -> bool {
        let (store, index) = match self.get_store_and_index() {
            Some(val) => val,
            None => return false,
        };
        return store.size() >= self.config.max_store_bytes
            || index.size() >= self.config.max_index_bytes;
    }

    pub fn remove(&mut self) -> std::io::Result<()> {
        self.close()?;
        std::fs::remove_file(&self.index_name)?;
        std::fs::remove_file(&self.store_name)?;
        Ok(())
    }

    pub fn close(&mut self) -> std::io::Result<()> {
        if let Some(index) = self.index.take() {
            index.close()?;
        }

        if let Some(store) = self.store.take() {
            store.close()?;
        }

        Ok(())
    }
}

impl Read for Segment {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.store {
            Some(ref mut store) => store.read(buf),
            None => Ok(0),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::log::index::ENTWIDTH;

    use super::*;

    #[test]
    fn test_segment() {
        let dir = tempfile::Builder::new()
            .prefix("segment-test")
            .tempdir()
            .unwrap();
        let want = Record {
            value: Vec::from(b"hello world"),
            offset: 0,
        };

        let mut c = Config::default();
        c.max_store_bytes = 1024;
        c.max_index_bytes = ENTWIDTH as u64 * 3;

        let mut s = Segment::new(dir.as_ref(), 16, c).unwrap();
        assert_eq!(16u64, s.next_offset);
        assert!(!s.is_maxed());

        for i in 0u64..3 {
            let offset = s.append(want.clone()).unwrap().unwrap();
            assert_eq!(16 + i, offset);
            let got = s.read_at_offset(offset).unwrap().unwrap();
            assert_eq!(want.value, got.value);
        }

        let err = s.append(want.clone()).map_err(|e| e.kind());
        assert_eq!(err, Err(std::io::ErrorKind::UnexpectedEof));

        // maxed index
        assert!(s.is_maxed());

        c.max_store_bytes = want.value.len() as u64 * 3;
        c.max_index_bytes = 1024;

        let mut s = Segment::new(dir.as_ref(), 16, c).unwrap();
        // maxed store
        assert!(s.is_maxed());

        s.remove().unwrap();

        let s = Segment::new(dir.as_ref(), 16, c).unwrap();
        assert!(!s.is_maxed());
    }
}
