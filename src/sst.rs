use std::{
    collections::HashMap,
    fs,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path,
};

use crate::{
    memtable,
    protocol::{ReadRecord, WriteRecord},
};

pub struct SST {
    index: Index,
    r: BufReader<fs::File>,
}

struct Index(HashMap<Vec<u8>, u32>); // Keys (as byte slices) to file offsets

impl Index {
    fn get_offset(&self, key: &[u8]) -> Option<&u32> {
        self.0.get(key)
    }
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

impl SST {
    pub fn new(path: &path::Path) -> Self {
        let file = fs::OpenOptions::new().read(true).open(path).unwrap();
        let mut r = BufReader::new(file);
        let index = IndexReader(&mut r).into_iter().collect();

        SST { index, r }
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

struct IndexReader<T: Read + Seek>(T);

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

struct IndexIter<T: Read>(T);

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

pub struct IndexEntry {
    key: Vec<u8>,
    offset: u32,
}

pub fn write_memtable(path: &path::Path, memtable: &memtable::MemTable) {
    let mut data: Vec<(&Vec<u8>, &Option<Vec<u8>>)> = memtable.iter().collect();
    data.sort_by(|a, b| a.0.cmp(b.0));

    let mut index_offsets: HashMap<&[u8], u32> = HashMap::new();

    let file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .unwrap();

    let mut w = BufWriter::new(&file);

    let mut written = 0;

    data.iter().for_each(|(key, val)| {
        index_offsets.insert(key, written);

        let rec = match val {
            Some(v) => WriteRecord::Exists { key, val: v },
            None => WriteRecord::Deleted { key },
        };

        written += rec.write_to(&mut w);
    });

    let index_start = written;

    // Index records are stored as:
    //  record offset: u32 | key length: u32 | key bytes
    data.iter().for_each(|(k, _)| {
        let offset = index_offsets.get(k.as_slice()).unwrap();
        w.write(&offset.to_le_bytes()).unwrap();

        let key_length = k.len() as u32;
        w.write(&key_length.to_le_bytes()).unwrap();
        w.write(k).unwrap();
    });

    // The final byte is the file offset where the index begins.
    w.write(&index_start.to_le_bytes()).unwrap();

    w.flush().unwrap();
    file.sync_all().unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    use crate::memtable::MemTable;

    #[test]
    fn test_sst() {
        let mut memtable = MemTable::new();

        let recs: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"key1".to_vec(), b"val1".to_vec()),
            (b"key2".to_vec(), b"val2".to_vec()),
            (b"key3".to_vec(), b"val3".to_vec()),
        ];

        for (k, v) in recs {
            memtable.put(&k, &v);
        }

        memtable.del(&b"key2".to_vec());

        let dir = TempDir::new("testing").unwrap();
        let path = dir.path().join("data.sst");

        write_memtable(&path, &memtable);

        let mut sst = SST::new(&path);

        assert_eq!(
            Some(ReadRecord::Exists {
                key: b"key1".to_vec(),
                val: b"val1".to_vec()
            }),
            sst.get(&b"key1".to_vec())
        );

        assert_eq!(
            Some(ReadRecord::Exists {
                key: b"key3".to_vec(),
                val: b"val3".to_vec()
            }),
            sst.get(&b"key3".to_vec())
        );

        assert_eq!(
            Some(ReadRecord::Deleted {
                key: b"key2".to_vec(),
            }),
            sst.get(&b"key2".to_vec())
        );
    }
}
