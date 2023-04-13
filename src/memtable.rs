use std::collections::{self, HashMap};

use crate::wal;

pub struct MemTable {
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            data: HashMap::new(),
        }
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) {
        self.data.insert(key.to_vec(), val.to_vec());
    }

    pub fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        self.data.get(key)
    }

    pub fn del(&mut self, key: &[u8]) {
        if let collections::hash_map::Entry::Occupied(o) = self.data.entry(key.to_vec()) {
            o.remove_entry();
        }
    }
}

impl From<wal::Wal> for MemTable {
    fn from(wal: wal::Wal) -> Self {
        let mut out = MemTable::new();

        wal.into_iter().for_each(|rec| match rec.op {
            wal::Operation::Put => out.put(&rec.key, &rec.val),
            wal::Operation::Delete => out.del(&rec.key),
        });

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable() {
        let mut mt = MemTable::new();

        // Not found.
        assert_eq!(None, mt.get(b"testKey".to_vec().as_ref()));

        // Put and then get is found.
        mt.put(b"testKey".to_vec().as_ref(), b"testVal".to_vec().as_ref());
        assert_eq!(
            Some(b"testVal".to_vec().as_ref()),
            mt.get(b"testKey".to_vec().as_ref())
        );

        // Update.
        mt.put(
            b"testKey".to_vec().as_ref(),
            b"testValUpdated".to_vec().as_ref(),
        );
        assert_eq!(
            Some(b"testValUpdated".to_vec().as_ref()),
            mt.get(b"testKey".to_vec().as_ref())
        );

        // Delete and then get is not found.
        mt.del(b"testKey".to_vec().as_ref());
        assert_eq!(None, mt.get(b"testKey".to_vec().as_ref()));

        // Put and get many.
        let kvs = vec![
            ("firstKey", "firstVal"),
            ("secondKey", "secondVal"),
            ("thirdKey", "thirdVal"),
        ];

        for kv in &kvs {
            mt.put(
                kv.0.as_bytes().to_vec().as_ref(),
                kv.1.as_bytes().to_vec().as_ref(),
            );
        }

        for kv in &kvs {
            assert_eq!(
                Some(kv.1.as_bytes().to_vec().as_ref()),
                mt.get(kv.0.as_bytes().to_vec().as_ref())
            );
        }
    }
}
