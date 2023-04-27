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
    r: BufReader<fs::File>,
}

impl Table {
    pub fn new(path: &path::Path) -> Self {
        let file = fs::OpenOptions::new().read(true).open(path).unwrap();
        let mut r = BufReader::new(file);
        let index = IndexReader(&mut r).into_iter().collect();

        Table {
            index,
            sequence: table_sequence(&path),
            r,
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Option<ReadRecord> {
        match self.index.get_offset(key) {
            Some(offset) => {
                self.r.seek(SeekFrom::Start(*offset as u64)).unwrap();
                // There should always be a record here since we found it in the index.
                Some(ReadRecord::read_from(&mut self.r).unwrap())
            }
            None => None,
        }
    }
}
