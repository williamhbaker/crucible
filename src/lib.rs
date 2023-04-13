use std::path;

use crate::{memtable::MemTable, wal::Wal};

pub mod memtable;
pub mod wal;

const WAL_FILE_NAME: &'static str = "data.wal";

pub struct Store {
    memtable: MemTable,
    wal: Wal,
}

impl Store {
    pub fn new(data_dir: &path::Path) -> Self {
        // Read existing wal records into the initial memtable. For now we are leaving the wal alone
        // and always re-reading and only ever appending to it.
        let wal_file_path = data_dir.join(&WAL_FILE_NAME);
        let wal = Wal::new(&wal_file_path);

        let memtable = wal.into();

        Store {
            memtable,
            wal: Wal::new(&wal_file_path),
        }
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) {
        self.wal.append(wal::Operation::Put, &key, &val);
        self.memtable.put(key, val);
    }

    pub fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        self.memtable.get(key)
    }

    pub fn del(&mut self, key: &[u8]) {
        self.memtable.del(key)
    }
}
