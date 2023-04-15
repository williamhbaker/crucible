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
