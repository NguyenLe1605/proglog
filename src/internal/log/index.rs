use std::{
    fs::File,
    io::{Error, ErrorKind},
    path::{Path, PathBuf},
};

use byteorder::{BigEndian, ByteOrder};
use memmap2::MmapMut;

use super::config::Config;

pub const OFFWIDTH: usize = 4;
pub const POSWIDTH: usize = 8;
pub const ENTWIDTH: usize = OFFWIDTH + POSWIDTH;

#[derive(Debug)]
pub struct Index {
    file: File,
    mmap: MmapMut,
    size: usize,
    path: PathBuf,
}

impl Index {
    pub fn new<P: AsRef<Path>>(f: File, c: Config, path: P) -> std::io::Result<Self> {
        let fi = f.metadata()?;
        let size = fi.len() as usize;
        f.set_len(c.max_index_bytes)?;
        let mmap = unsafe { MmapMut::map_mut(&f)? };
        let path = path.as_ref().to_path_buf();
        let idx = Index {
            file: f,
            mmap,
            size,
            path,
        };
        Ok(idx)
    }

    pub fn close(self) -> std::io::Result<()> {
        self.mmap.flush()?;
        self.file.sync_all()?;
        self.file.set_len(self.size as u64)?;
        drop(self);
        Ok(())
    }

    pub fn read(&self, inp: i64) -> std::io::Result<(u32, u64)> {
        if self.size == 0 {
            return Err(Error::from(ErrorKind::UnexpectedEof));
        }

        let out = if inp == -1 {
            ((self.size / ENTWIDTH) - 1) as u32
        } else {
            inp as u32
        };

        let pos = out as usize * ENTWIDTH;

        if (self.size as usize) < pos + ENTWIDTH {
            return Err(Error::from(ErrorKind::UnexpectedEof));
        }

        let out = BigEndian::read_u32(&self.mmap[pos..(pos + OFFWIDTH)]);
        let pos = BigEndian::read_u64(&self.mmap[(pos + OFFWIDTH)..(pos + ENTWIDTH)]);
        Ok((out, pos))
    }

    pub fn write(&mut self, offset: u32, pos: u64) -> std::io::Result<()> {
        if self.mmap.len() < self.size as usize + ENTWIDTH {
            return Err(Error::from(ErrorKind::UnexpectedEof));
        }

        BigEndian::write_u32(&mut self.mmap[self.size..(self.size + OFFWIDTH)], offset);
        BigEndian::write_u64(
            &mut self.mmap[(self.size + OFFWIDTH)..(self.size + ENTWIDTH)],
            pos,
        );
        self.size += ENTWIDTH;
        Ok(())
    }

    pub fn get_path(&self) -> PathBuf {
        return self.path.clone();
    }

    #[inline]
    pub fn size(&self) -> u64 {
        return self.size as u64;
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn test_index() {
        let file = tempfile::Builder::new()
            .append(true)
            .prefix("index_test")
            .tempfile()
            .unwrap();
        let mut config = Config::default();
        config.max_index_bytes = 1024;

        let mut idx = Index::new(file.reopen().unwrap(), config, file.path()).unwrap();
        let err = idx.read(-1).map_err(|e| e.kind());
        assert_eq!(file.path().to_path_buf(), idx.get_path());
        assert_eq!(err, Err(io::ErrorKind::UnexpectedEof));
        let entries: Vec<(u32, u64)> = vec![(0, 0), (1, 10)];
        for want in entries.iter() {
            idx.write(want.0, want.1).unwrap();
            let (_, pos) = idx.read(want.0 as i64).unwrap();
            assert_eq!(want.1, pos);
        }
        let err = idx.read(entries.len() as i64).map_err(|e| e.kind());
        assert_eq!(err, Err(io::ErrorKind::UnexpectedEof));
        idx.close().unwrap();

        let f = file.reopen().unwrap();
        let idx = Index::new(f, config, file.path()).unwrap();
        let (offset, pos) = idx.read(-1).unwrap();
        assert_eq!(offset, entries[1].0);
        assert_eq!(pos, entries[1].1);
    }
}
