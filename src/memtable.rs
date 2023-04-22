use std::collections::{hash_map, HashMap};

use crate::protocol::{ReadRecord, WriteRecord};

pub struct MemTable {
    // An entry that is present in the HashMap with a value of None represents a specific deletion
    // record.
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

pub struct Iter<'a> {
    inner: hash_map::Iter<'a, Vec<u8>, Option<Vec<u8>>>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = WriteRecord<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(key, val)| match val {
            Some(val) => WriteRecord::Exists { key, val },
            None => WriteRecord::Deleted { key },
        })
    }
}

impl<'a> IntoIterator for &'a MemTable {
    type Item = WriteRecord<'a>;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            inner: self.data.iter(),
        }
    }
}

impl FromIterator<ReadRecord> for MemTable {
    fn from_iter<I: IntoIterator<Item = ReadRecord>>(iter: I) -> Self {
        let mut out = MemTable::new();

        for i in iter {
            match i {
                ReadRecord::Exists { key, val } => out.put(&key, &val),
                ReadRecord::Deleted { key } => out.del(&key),
            }
        }

        out
    }
}
