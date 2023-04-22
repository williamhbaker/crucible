use std::{fs, path};

use uuid::Uuid;

use crate::{
    memtable::MemTable,
    protocol::WriteRecord,
    sst::{self, Catalog, SST},
    wal,
};

const WAL_FILE_NAME: &'static str = "data.wal";
const SST_EXT: &'static str = "sst";

pub struct Store {
    memtable: MemTable,
    wal: wal::Writer,
    sst: Catalog,
}

impl Store {
    pub fn new(data_dir: &path::Path) -> Self {
        let wal_file_path = data_dir.join(&WAL_FILE_NAME);

        // Convert any left-over wal file into an sst.
        if wal_file_path.try_exists().unwrap() {
            let memtable: MemTable = wal::Reader::new(&wal_file_path).into_iter().collect();
            let fname = Uuid::new_v4().to_string() + "." + SST_EXT;
            sst::write_records(&data_dir.join(&fname), &memtable);
        }

        // Index all existing sst files.
        let ssts = fs::read_dir(data_dir)
            .unwrap()
            .into_iter()
            .filter_map(|entry| {
                let entry = entry.unwrap();
                let path = entry.path();

                if !path.is_dir() {
                    if let Some(ext) = path.extension() {
                        if ext.eq_ignore_ascii_case(SST_EXT) {
                            return Some(SST::new(&path));
                        }
                    }
                }

                None
            })
            .collect();

        Store {
            memtable: MemTable::new(),
            wal: wal::Writer::new(&wal_file_path),
            sst: ssts,
        }
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) {
        self.wal.append(WriteRecord::Exists { key, val });
        self.memtable.put(key, val);
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(val) = self.memtable.get(key) {
            Some(val.to_vec())
        } else if let Some(rec) = self.sst.get(key) {
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
