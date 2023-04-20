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
    index: HashMap<Vec<u8>, u32>, // Keys (as byte slices) to file offsets
    file: fs::File,
}

impl SST {
    pub fn new(path: &path::Path) -> Self {
        let mut file = fs::OpenOptions::new().read(true).open(path).unwrap();

        file.seek(SeekFrom::End(-4)).unwrap();

        // 4 bytes for the starting offset of the index in the file.
        let mut buf = [0; 4];
        file.read_exact(&mut buf).unwrap();

        let index_start = u32::from_le_bytes(buf[0..4].try_into().unwrap());

        let mut r = BufReader::new(&file);

        r.seek(SeekFrom::Start(index_start as u64)).unwrap();

        let mut index = HashMap::new();
        let mut entry = IndexEntry::default();
        while entry.read_from(&mut r) {
            index.insert(entry.key, entry.offset);
            entry = IndexEntry::default();
        }

        SST { index, file }
    }

    pub fn get(&self, key: &[u8]) -> Option<ReadRecord> {
        match self.index.get(key) {
            Some(offset) => {
                let mut reader = BufReader::new(&self.file); // TODO: Re-use?
                reader.seek(SeekFrom::Start(*offset as u64)).unwrap();
                ReadRecord::read_from(&mut reader)
            }
            None => None,
        }
    }
}

#[derive(Default)]
pub struct IndexEntry {
    key: Vec<u8>,
    offset: u32,
}

impl IndexEntry {
    fn read_from<R: Read>(&mut self, reader: &mut R) -> bool {
        // Read record offset & key length. 4 bytes each.
        let mut buf = [0; 8];
        match reader.read(&mut buf) {
            Ok(8) => (),
            Ok(4) => return false, // EOF since the footer is 4 bytes
            Ok(n) => panic!("bad header in index record, had {} bytes", n),
            Err(e) => panic!("could not read index record header: {}", e),
        }

        self.offset = u32::from_le_bytes(buf[0..4].try_into().unwrap());

        let key_length = u32::from_le_bytes(buf[4..8].try_into().unwrap());

        self.key = vec![0; key_length as usize];
        reader.read_exact(&mut self.key).unwrap();

        true
    }
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

    data.iter().for_each(|(k, v)| {
        index_offsets.insert(k, written);

        let rec = match v {
            Some(v) => WriteRecord::Exists { key: k, val: v },
            None => WriteRecord::Deleted { key: k },
        };

        written += rec.write(&mut w);
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

        let sst = SST::new(&path);

        assert_eq!(
            Some(ReadRecord::Exists {
                key: b"key1".to_vec(),
                val: b"val1".to_vec()
            }),
            sst.get(&b"key1".to_vec())
        );
    }
}
