use std::{
    collections::HashMap,
    io::{self, Read, Seek, SeekFrom},
};

use crate::protocol::fill_buf;

pub struct Index(HashMap<Vec<u8>, u32>); // Keys (as byte slices) to file offsets

impl Index {
    pub fn get_offset(&self, key: &[u8]) -> Option<&u32> {
        self.0.get(key)
    }

    pub fn from_index_reader<T: Read + Seek>(r: IndexReader<T>) -> io::Result<Index> {
        let mut map = HashMap::new();

        for i in r.into_iter() {
            let i = i?;
            map.insert(i.key, i.offset);
        }

        Ok(Index(map))
    }
}

pub struct IndexEntry {
    key: Vec<u8>,
    offset: u32,
}

impl FromIterator<IndexEntry> for Index {
    fn from_iter<I: IntoIterator<Item = IndexEntry>>(iter: I) -> Self {
        let mut map = HashMap::new();

        for i in iter {
            map.insert(i.key, i.offset);
        }

        Index(map)
    }
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
        };

        if let Err(e) = index_iter.r.seek(SeekFrom::End(-4)) {
            index_iter.setup_err = Some(Err(e));
            return index_iter;
        }

        // 4 bytes for the starting offset of the index in the file.
        let mut buf = [0; 4];
        if let Err(e) = index_iter.r.read_exact(&mut buf) {
            index_iter.setup_err = Some(Err(e));
            return index_iter;
        }

        let index_start = u32::from_le_bytes(
            buf[0..4]
                .try_into()
                .expect("must convert slice to byte array"),
        );

        if let Err(e) = index_iter.r.seek(SeekFrom::Start(index_start as u64)) {
            index_iter.setup_err = Some(Err(e));
            return index_iter;
        }

        index_iter
    }
}

pub struct IndexIter<T: Read> {
    r: T,
    done: bool,
    setup_err: Option<io::Result<IndexEntry>>,
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

        // 4 bytes for the trailer, which is the u32 byte offset of the start of the index.
        match fill_buf(&mut self.r, &mut buf, 4) {
            Ok(Some(8)) => (),
            Ok(Some(0)) => return None,
            Ok(_) => unreachable!(),
            Err(e) => {
                self.done = true;
                return Some(Err(e));
            }
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

        Some(Ok(IndexEntry { key, offset }))
    }
}
