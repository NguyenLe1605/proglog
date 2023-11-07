use std::{
    io::Read,
    path::{Path, PathBuf},
};

use crate::server::log::Record;

use super::{config::Config, segment::Segment};
use std::io::{Error, ErrorKind, Result};

pub struct Log {
    pub dir: PathBuf,
    pub config: Config,
    active_segment: usize,
    segments: Vec<Option<Segment>>,
    reader_idx: usize,
}

impl Log {
    pub fn new<P: AsRef<Path>>(dir: P, mut c: Config) -> Result<Self> {
        if c.max_store_bytes == 0 {
            c.max_store_bytes = 1024;
        }

        if c.max_index_bytes == 0 {
            c.max_index_bytes = 1024;
        }
        let mut l = Log {
            dir: dir.as_ref().to_path_buf(),
            config: c,
            active_segment: 0,
            segments: Vec::new(),
            reader_idx: 0,
        };

        l.setup()?;
        Ok(l)
    }

    pub fn append(&mut self, record: Record) -> Result<Option<u64>> {
        let idx = self.active_segment;
        let segment = match self.segments[idx] {
            Some(ref mut segment) => segment,
            None => return Ok(None),
        };
        let offset = match segment.append(record)? {
            Some(offset) => offset,
            None => return Ok(None),
        };

        if segment.is_maxed() {
            self.new_segment(offset + 1)?;
        }

        Ok(Some(offset))
    }

    pub fn read_at_offset(&mut self, offset: u64) -> Result<Option<Record>> {
        let s = match self.segments.iter_mut().find(|seg| {
            if let Some(ref seg) = seg {
                seg.base_offset <= offset && offset < seg.next_offset
            } else {
                false
            }
        }) {
            None => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("offset out of range: {}", offset),
                ))
            }
            Some(s) => match s {
                Some(s) => s,
                None => return Ok(None),
            },
        };
        s.read_at_offset(offset)
    }

    pub fn close(&mut self) -> Result<()> {
        for segment in self.segments.iter_mut() {
            if let Some(ref mut segment) = segment {
                segment.close()?;
            }
        }

        self.reader_idx = self.segments.len();
        Ok(())
    }

    pub fn remove(&mut self) -> Result<()> {
        self.close()?;
        std::fs::remove_dir_all(&self.dir)
    }

    pub fn reset(&mut self) -> Result<()> {
        self.remove()?;
        self.setup()
    }

    pub fn lowest_offset(&self) -> Result<u64> {
        if let Some(ref segment) = self.segments[0] {
            return Ok(segment.base_offset);
        }
        return Err(Error::new(ErrorKind::Other, "corrupted log"));
    }

    #[inline]
    pub fn highest_offset(&self) -> Result<u64> {
        if let Some(ref segment) = self.segments.last().unwrap() {
            let offset = segment.next_offset;
            if offset == 0 {
                return Ok(0);
            }
            return Ok(offset - 1);
        }
        return Err(Error::new(ErrorKind::Other, "corrupted log"));
    }

    pub fn truncate(&mut self, lowest: u64) -> Result<()> {
        let mut segments: Vec<Option<Segment>> = Vec::new();
        for s in self.segments.iter_mut() {
            if let Some(mut segment) = s.take() {
                if segment.next_offset <= lowest + 1 {
                    segment.remove()?;
                    continue;
                }

                segments.push(Some(segment));
            }
        }
        self.reader_idx = 0;
        self.segments = segments;
        Ok(())
    }

    fn setup(&mut self) -> Result<()> {
        let files = std::fs::read_dir(&self.dir)?;
        let mut base_offsets: Vec<u64> = Vec::new();
        for file in files.into_iter() {
            let file = file?;
            let path = file.path();
            let off_str = path
                .file_stem()
                .ok_or(Error::new(ErrorKind::Other, "can not get the file stem"))?;
            let offset = off_str
                .to_str()
                .ok_or(Error::new(ErrorKind::Other, "can convert OsString to str"))?
                .parse::<u64>()
                .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
            base_offsets.push(offset);
        }

        base_offsets.sort_unstable();
        for offset in base_offsets.iter().step_by(2) {
            self.new_segment(*offset)?;
        }

        if self.segments.is_empty() {
            self.new_segment(self.config.initial_offset)?;
        }

        Ok(())
    }

    fn new_segment(&mut self, offset: u64) -> Result<()> {
        let s = Segment::new(&self.dir, offset, self.config.clone())?;
        self.segments.push(Some(s));
        self.active_segment = self.segments.len() - 1;
        Ok(())
    }
}

impl Read for Log {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        loop {
            if self.reader_idx >= self.segments.len() {
                return Ok(0);
            }
            match self.segments[self.reader_idx] {
                None => {
                    self.reader_idx += 1;
                    continue;
                }

                Some(ref mut segment) => match segment.read(buf) {
                    Ok(n) => {
                        return Ok(n);
                    }
                    Err(e) => {
                        if e.kind() == ErrorKind::UnexpectedEof {
                            self.reader_idx += 1;
                            continue;
                        }
                        return Err(e);
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use crate::log::store::LEN_WIDTH;

    use super::*;

    #[test]
    fn test_log() {
        let tests: Vec<(&str, fn(Log))> = vec![
            ("append and read a record succeeds", test_append_read),
            ("offset out of range error", test_out_of_range_err),
            ("init with existing segments", test_init_existing),
            ("reader", test_reader),
            ("truncate", test_truncate),
        ];

        for (scen, func) in tests {
            println!("Running: {}", scen);
            let dir = tempfile::Builder::new()
                .append(true)
                .prefix("store-test")
                .tempdir()
                .unwrap();

            let mut c = Config::default();
            c.max_store_bytes = 32;
            let log = Log::new(&dir, c).unwrap();
            func(log);
        }
    }

    fn test_append_read(mut log: Log) {
        let append = Record {
            value: b"hello world".into(),
            offset: 0,
        };
        let offset = log.append(append.clone()).unwrap().unwrap();
        assert_eq!(0u64, offset);
        let read = log.read_at_offset(offset).unwrap().unwrap();
        assert_eq!(append.value, read.value);
    }
    fn test_out_of_range_err(mut log: Log) {
        let read = log.read_at_offset(1);
        assert!(read.is_err());
    }
    fn test_init_existing(mut log: Log) {
        let append = Record {
            value: b"hello world".into(),
            offset: 0,
        };

        for _ in 0..3 {
            log.append(append.clone()).unwrap().unwrap();
        }

        log.close().unwrap();
        let offset = log.lowest_offset().unwrap();
        assert_eq!(0u64, offset);
        let offset = log.highest_offset().unwrap();
        assert_eq!(2u64, offset);

        let log = Log::new(log.dir, log.config).unwrap();
        let offset = log.lowest_offset().unwrap();
        assert_eq!(0u64, offset);
        let offset = log.highest_offset().unwrap();
        assert_eq!(2u64, offset);
    }
    fn test_reader(mut log: Log) {
        let append = Record {
            value: b"hello world".into(),
            offset: 0,
        };

        let offset = log.append(append.clone()).unwrap();
        assert_eq!(0u64, offset.unwrap());

        let mut buf: Vec<u8> = Vec::new();
        log.read_to_end(&mut buf).unwrap();
        let start = LEN_WIDTH as usize;
        let end = start + append.value.len() + 2;
        let read: Record = Message::decode(&buf[start..end]).unwrap();
        assert_eq!(append.value, read.value);
    }
    fn test_truncate(mut log: Log) {
        let append = Record {
            value: b"hello world".into(),
            offset: 0,
        };

        for _ in 0..3u64 {
            log.append(append.clone()).unwrap().unwrap();
        }

        log.truncate(1).unwrap();

        assert!(log.read_at_offset(0).is_err());
    }
}
