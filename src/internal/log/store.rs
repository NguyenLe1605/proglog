use std::{
    fs::File,
    io::{BufWriter, Read, Write},
    os::unix::prelude::FileExt,
};

use byteorder::{BigEndian, ByteOrder, WriteBytesExt};

pub const LEN_WIDTH: u64 = 8;

#[derive(Debug)]
pub struct Store {
    file: File,
    writer: BufWriter<File>,
    size: u64,
    offset: usize,
}

impl Store {
    pub fn new(file: File) -> std::io::Result<Self> {
        let fi = file.metadata()?;
        let size = fi.len();
        let write_file = file.try_clone()?;
        let writer = BufWriter::new(write_file);
        let store = Store {
            file,
            writer,
            size,
            offset: 0,
        };

        Ok(store)
    }

    pub fn try_clone(&mut self) -> std::io::Result<Self> {
        self.writer.flush()?;
        let clone_file = self.file.try_clone()?;
        Self::new(clone_file)
    }

    pub fn append(&mut self, p: &[u8]) -> std::io::Result<(u64, u64)> {
        let pos = self.size;
        let len = p.len() as u64;
        self.writer.write_u64::<BigEndian>(len)?;
        self.writer.write_all(p)?;
        let w = len + LEN_WIDTH;
        self.size += w;
        Ok((w, pos))
    }

    pub fn read_at_offset(&mut self, pos: u64) -> std::io::Result<Vec<u8>> {
        self.writer.flush()?;
        let mut size = [0u8; 8];
        self.file.read_at(&mut size, pos)?;
        let size = BigEndian::read_u64(&size);
        let mut buf: Vec<u8> = vec![0; size as usize];
        self.file.read_exact_at(&mut buf, pos + LEN_WIDTH)?;
        Ok(buf)
    }

    pub fn read_at(&mut self, p: &mut [u8], offset: u64) -> std::io::Result<usize> {
        self.writer.flush()?;
        self.file.read_at(p, offset)?;
        Ok(p.len())
    }

    pub fn close(mut self) -> std::io::Result<()> {
        self.writer.flush()?;
        drop(self);
        Ok(())
    }

    #[inline]
    pub fn size(&self) -> u64 {
        return self.size;
    }
}

impl Read for Store {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.offset >= self.size as usize {
            return Ok(0);
        }
        let n = self.read_at(buf, self.offset as u64)?;
        self.offset += n;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, path::Path};

    use super::*;
    use tempfile::Builder;
    const DUMMY_MSG: &'static [u8] = b"hello world";
    const WIDTH: u64 = DUMMY_MSG.len() as u64 + LEN_WIDTH;

    fn test_append(s: &mut Store) {
        for i in 1..4u64 {
            let (n, pos) = s.append(DUMMY_MSG).unwrap();
            assert_eq!(pos + n, WIDTH * i);
        }
    }

    fn test_read(s: &mut Store) {
        let mut pos: u64 = 0;
        for _ in 1..4u64 {
            let read = s.read_at_offset(pos).unwrap();
            assert_eq!(DUMMY_MSG, read);
            pos += WIDTH;
        }
    }

    fn test_read_at(s: &mut Store) {
        let mut offset = 0;
        for _ in 1..4u64 {
            let mut buf = [0u8; LEN_WIDTH as usize];
            let nbytes = s.read_at(&mut buf, offset).unwrap() as u64;
            assert_eq!(LEN_WIDTH, nbytes);

            offset += nbytes;
            let size = BigEndian::read_u64(&buf) as usize;
            let mut buf: Vec<u8> = vec![0; size];
            let nbytes = s.read_at(&mut buf, offset).unwrap();
            assert_eq!(DUMMY_MSG, buf);
            assert_eq!(size, nbytes);
            offset += nbytes as u64;
        }
    }

    #[test]
    fn test_store_append_read() {
        let file = Builder::new()
            .append(true)
            .prefix("store-append-read-test")
            .tempfile()
            .unwrap();

        let mut s = Store::new(file.reopen().unwrap()).unwrap();
        test_append(&mut s);
        test_read(&mut s);
        test_read_at(&mut s);

        let mut s = Store::new(file.into_file()).unwrap();
        test_read(&mut s);
    }

    #[test]
    fn test_store_close() {
        let file = Builder::new()
            .append(true)
            .prefix("store-close-test")
            .tempfile()
            .unwrap();
        let path = file.path().to_owned();
        let path = path.as_path();
        let mut s = Store::new(file.reopen().unwrap()).unwrap();
        let (_, before_size) = open_file(path).unwrap();
        s.append(DUMMY_MSG).unwrap();

        s.close().unwrap();

        let (_, after_size) = open_file(path).unwrap();
        print!("after: {:?}, before: {:?}", after_size, before_size);
        assert!(after_size > before_size);
    }

    fn open_file(path: &Path) -> std::io::Result<(File, u64)> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        let fi = file.metadata()?;
        Ok((file, fi.len()))
    }
}
