use core::fmt;

use serde_with::{base64::Base64, serde_as};

tonic::include_proto!("log");

pub enum LogError {
    ErrOffsetNotFound,
    Other,
}

impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match &self {
            Self::ErrOffsetNotFound => "offset not found",
            Self::Other => "weird error occurs",
        };

        write!(f, "{}", msg)
    }
}

#[derive(Debug, Clone, Default)]
pub struct Log {
    records: Vec<Record>,
}

impl Log {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    pub fn append(&mut self, mut record: Record) -> Result<u64, LogError> {
        let offset = self.records.len() as u64;
        record.offset = offset;
        self.records.push(record);
        return Ok(offset);
    }

    pub fn read(&self, offset: u64) -> Result<Record, LogError> {
        if offset >= self.records.len() as u64 {
            return Err(LogError::ErrOffsetNotFound);
        }

        return Ok(self.records[offset as usize].clone());
    }
}
