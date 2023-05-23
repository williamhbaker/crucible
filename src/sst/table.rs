use std::{
    fs,
    io::{self, BufReader, Seek, SeekFrom},
    path,
};

use crate::protocol::ReadRecord;

use super::{table_sequence, Index, IndexReader};

pub struct Table {
    index: Index,
    pub sequence: Option<u32>,
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

impl IntoIterator for Table {
    type Item = io::Result<ReadRecord>;
    type IntoIter = TableIter;

    fn into_iter(self) -> Self::IntoIter {
        let mut table_iter = TableIter {
            r: BufReader::new(self.file),
            done: false,
            setup_err: None,
        };

        if let Err(e) = table_iter.r.seek(SeekFrom::Start(0)) {
            table_iter.setup_err = Some(Err(e));
        }

        table_iter
    }
}

pub struct TableIter {
    r: BufReader<fs::File>,
    done: bool,
    setup_err: Option<io::Result<ReadRecord>>,
}

impl Iterator for TableIter {
    type Item = io::Result<ReadRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        if self.setup_err.is_some() {
            self.done = true;
            return self.setup_err.take();
        }

        ReadRecord::read_from(&mut self.r)
            .transpose()
            .and_then(|res| {
                if res.is_err() {
                    self.done = true;
                }
                Some(res)
            })
    }
}
