use std::collections::HashMap;

use crate::wal;

pub struct MemTable {
    // A that is present in the HashMap with a value of None represents a specific deletion record.
    data: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            data: HashMap::new(),
        }
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) {
        self.data.insert(key.to_vec(), Some(val.to_vec()));
    }

    pub fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        if let Some(val) = self.data.get(key) {
            // HashMap contains a record for this key, but the value might still be None if it was
            // from a deletion.
            val.as_ref()
        } else {
            // No record for this key. We have no knowledge of it ever existing or having been
            // deleted.
            None
        }
    }

    pub fn del(&mut self, key: &[u8]) {
        self.data.insert(key.to_vec(), None);
    }
}

impl From<wal::Wal> for MemTable {
    fn from(wal: wal::Wal) -> Self {
        let mut out = MemTable::new();

        wal.into_iter().for_each(|rec| match rec.op {
            wal::Operation::Put => out.put(&rec.key, &rec.val.unwrap()),
            wal::Operation::Delete => out.del(&rec.key),
        });

        out
    }
}
