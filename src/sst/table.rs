use std::{
    fs,
    io::{BufReader, Seek, SeekFrom},
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
    pub fn new(path: &path::Path) -> Self {
        let file = fs::OpenOptions::new().read(true).open(path).unwrap();
        let mut r = BufReader::new(&file);
        let index = IndexReader(&mut r).into_iter().collect();

        Table {
            index,
            sequence: table_sequence(&path),
            file,
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<ReadRecord> {
        match self.index.get_offset(key) {
            Some(offset) => {
                let mut r = &self.file.try_clone().unwrap();
                r.seek(SeekFrom::Start(*offset as u64)).unwrap();
                // There should always be a record here since we found it in the index.
                Some(ReadRecord::read_from(&mut r).unwrap())
            }
            None => None,
        }
    }
}
