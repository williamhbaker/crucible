use std::path;

use crate::{memtable::MemTable, protocol::WriteRecord, sst::Catalog, wal};

const WAL_FILE_NAME: &'static str = "data.wal";

pub struct Store {
    memtable: MemTable,
    wal: wal::Writer,
    catalog: Catalog,
}

impl Store {
    pub fn new(data_dir: &path::Path) -> Self {
        let wal_file_path = data_dir.join(&WAL_FILE_NAME);

        let mut sst = Catalog::new(&data_dir);

        // Convert any left-over wal file into an sst.
        if wal_file_path.try_exists().unwrap() {
            let memtable: MemTable = wal::Reader::new(&wal_file_path).into_iter().collect();
            sst.write_records(&memtable);
        }

        Store {
            memtable: MemTable::new(),
            wal: wal::Writer::new(&wal_file_path),
            catalog: sst,
        }
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) {
        self.wal.append(WriteRecord::Exists { key, val });
        self.memtable.put(key, val);
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(val) = self.memtable.get(key) {
            Some(val.to_vec())
        } else if let Some(rec) = self.catalog.get(key) {
            match rec {
                crate::protocol::ReadRecord::Exists { val, .. } => Some(val),
                crate::protocol::ReadRecord::Deleted { .. } => None,
            }
        } else {
            None
        }
    }

    pub fn del(&mut self, key: &[u8]) {
        self.wal.append(WriteRecord::Deleted { key });
        self.memtable.del(key)
    }
}
