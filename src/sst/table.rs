use std::{
    fs,
    io::{self, BufReader, Seek, SeekFrom},
    path,
};

use crate::protocol::{self, ReadRecord};

use super::{Index, IndexReader};

pub struct Table {
    index: Index,
    file: fs::File,
    pub path: path::PathBuf,
}

impl Table {
    pub fn new(path: &path::Path) -> io::Result<Self> {
        let file = fs::OpenOptions::new().read(true).open(path)?;
        let mut r = BufReader::new(&file);

        Ok(Table {
            index: Index::from_index_reader(IndexReader(&mut r))?,
            file,
            path: path.into(),
        })
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<ReadRecord>> {
        match self.index.get_offset(key) {
            Some(offset) => {
                let mut r = &self.file.try_clone()?;
                r.seek(SeekFrom::Start(*offset as u64))?;
                // There should always be a record here since we found it in the index.
                Some(ReadRecord::read_from(&mut r)).transpose()
            }
            None => Ok(None),
        }
    }

    pub fn key_start(&self) -> Vec<u8> {
        self.index.key_start.clone()
    }

    pub fn key_end(&self) -> Vec<u8> {
        self.index.key_end.clone()
    }
}

impl IntoIterator for Table {
    type Item = io::Result<ReadRecord>;
    type IntoIter = TableIter;

    fn into_iter(self) -> Self::IntoIter {
        let r = BufReader::new(self.file);

        let mut table_iter = TableIter {
            r: r,
            done: false,
            setup_err: None,
            entries_length: 0,
            read: 0,
        };

        let footer = match protocol::Footer::new_from_reader(&mut table_iter.r) {
            Ok(f) => f,
            Err(e) => {
                table_iter.setup_err = Some(Err(e));
                return table_iter;
            }
        };

        table_iter.entries_length = footer.index_start;

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
    entries_length: u32,
    read: u32,
}

// This needs to be like the index iterator where it knows how far to go. In the into_iter, read the
// footer to get this information. Keep track of how much we have read and set done when we have
// read it all. Then that weird fill_buff function can go away.
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

        let record = ReadRecord::read_from(&mut self.r);
        if record.is_err() {
            self.done = true;
            return Some(record);
        }

        let record = record.unwrap();
        self.read += record.size() as u32;
        if self.read == self.entries_length {
            self.done = true;
        }

        Some(Ok(record))
    }
}
