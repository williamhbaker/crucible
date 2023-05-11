use std::{
    fs,
    io::{self, BufReader, Seek, SeekFrom},
    path,
};

use crate::protocol::ReadRecord;

use super::{table_sequence, Index, IndexReader};

pub struct Table {
    index: Index,
    pub sequence: usize,
    file: fs::File,
}

impl Table {
    pub fn new(path: &path::Path) -> io::Result<Self> {
        let file = fs::OpenOptions::new().read(true).open(path)?;
        let mut r = BufReader::new(&file);

        Ok(Table {
            index: Index::from_index_reader(IndexReader(&mut r))?,
            sequence: table_sequence(&path),
            file,
        })
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<ReadRecord>> {
        match self.index.get_offset(key) {
            Some(offset) => {
                let mut r = &self.file.try_clone()?;
                r.seek(SeekFrom::Start(*offset as u64))?;
                // There should always be a record here since we found it in the index.
                ReadRecord::read_from(&mut r)
            }
            None => Ok(None),
        }
    }
}
