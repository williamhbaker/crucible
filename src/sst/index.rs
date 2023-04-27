use std::{
    collections::HashMap,
    io::{Read, Seek, SeekFrom},
};

pub struct Index(HashMap<Vec<u8>, u32>); // Keys (as byte slices) to file offsets

impl Index {
    pub fn get_offset(&self, key: &[u8]) -> Option<&u32> {
        self.0.get(key)
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
    type Item = IndexEntry;
    type IntoIter = IndexIter<T>;

    fn into_iter(mut self) -> Self::IntoIter {
        self.0.seek(SeekFrom::End(-4)).unwrap();

        // 4 bytes for the starting offset of the index in the file.
        let mut buf = [0; 4];
        self.0.read_exact(&mut buf).unwrap();

        let index_start = u32::from_le_bytes(buf[0..4].try_into().unwrap());

        self.0.seek(SeekFrom::Start(index_start as u64)).unwrap();

        IndexIter(self.0)
    }
}

pub struct IndexIter<T: Read>(T);

impl<T: Read> Iterator for IndexIter<T> {
    type Item = IndexEntry;

    fn next(&mut self) -> Option<Self::Item> {
        // Read record offset & key length. 4 bytes each.
        let mut buf = [0; 8];
        match self.0.read(&mut buf) {
            Ok(8) => (),
            Ok(4) => return None, // EOF since the footer is 4 bytes
            Ok(n) => panic!("bad header in index record, had {} bytes", n),
            Err(e) => panic!("could not read index record header: {}", e),
        }

        let offset = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let key_length = u32::from_le_bytes(buf[4..8].try_into().unwrap());

        let mut key = vec![0; key_length as usize];
        self.0.read_exact(&mut key).unwrap();

        Some(IndexEntry { key, offset })
    }
}
