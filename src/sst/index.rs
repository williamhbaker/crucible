use std::{
    collections::HashMap,
    io::{self, Read, Seek, SeekFrom},
};

use crate::protocol::Footer;

pub struct Index {
    map: HashMap<Vec<u8>, u32>, // Keys (as byte slices) to file offsets
    key_start: Vec<u8>,
    key_end: Vec<u8>,
}

impl Index {
    pub fn get_offset(&self, key: &[u8]) -> Option<&u32> {
        self.map.get(key)
    }

    pub fn from_index_reader<T: Read + Seek>(r: IndexReader<T>) -> io::Result<Index> {
        let mut map = HashMap::new();

        // Requirement: IndexReader iterators through keys in ascending sorted order.
        let mut key_start = None;
        let mut key_end = None;

        for i in r {
            let i = i?;
            if key_start.is_none() {
                key_start = Some(i.key.clone());
            }
            key_end = Some(i.key.clone());

            map.insert(i.key, i.offset);
        }

        Ok(Index {
            map,
            key_start: key_start.expect("IndexReader must have a key"),
            key_end: key_end.expect("IndexReader must have a key"),
        })
    }
}

pub struct IndexEntry {
    key: Vec<u8>,
    offset: u32,
}

pub struct IndexReader<T: Read + Seek>(pub T);

impl<T: Read + Seek> IntoIterator for IndexReader<T> {
    type Item = io::Result<IndexEntry>;
    type IntoIter = IndexIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        let mut index_iter = IndexIter {
            r: self.0,
            done: false,
            setup_err: None,
            index_length: 0,
            read: 0,
        };

        let seeked = match index_iter.r.seek(SeekFrom::End(-4)) {
            Ok(s) => s,
            Err(e) => {
                index_iter.setup_err = Some(Err(e));
                return index_iter;
            }
        };

        let footer = match Footer::new_from_reader(&mut index_iter.r) {
            Ok(f) => f,
            Err(e) => {
                index_iter.setup_err = Some(Err(e));
                return index_iter;
            }
        };

        if let Err(e) = index_iter
            .r
            .seek(SeekFrom::Start(footer.index_start as u64))
        {
            index_iter.setup_err = Some(Err(e));
            return index_iter;
        }

        index_iter.index_length = seeked as u32 + 4
            - footer.index_start
            - footer.footer_length.expect("footer must have length");

        index_iter
    }
}

pub struct IndexIter<T: Read> {
    r: T,
    done: bool,
    setup_err: Option<io::Result<IndexEntry>>,
    index_length: u32, // In bytes
    read: u32,
}

impl<T: Read> Iterator for IndexIter<T> {
    type Item = io::Result<IndexEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        if self.setup_err.is_some() {
            self.done = true;
            return self.setup_err.take();
        }

        // Read record offset & key length. 4 bytes each.
        let mut buf = [0; 8];
        if let Err(e) = self.r.read_exact(&mut buf) {
            self.done = true;
            return Some(Err(e));
        }

        let offset = u32::from_le_bytes(
            buf[0..4]
                .try_into()
                .expect("must convert byte slice to array"),
        );
        let key_length = u32::from_le_bytes(
            buf[4..8]
                .try_into()
                .expect("must convert slice to byte array"),
        );

        let mut key = vec![0; key_length as usize];

        if let Err(e) = self.r.read_exact(&mut key) {
            self.done = true;
            return Some(Err(e));
        }

        self.read += 4 + 4 + key_length;
        if self.read == self.index_length {
            self.done = true;
        }

        Some(Ok(IndexEntry { key, offset }))
    }
}
