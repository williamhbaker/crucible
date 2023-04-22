use std::path;

use protocol::WriteRecord;

use crate::memtable::MemTable;

pub mod memtable;
pub mod protocol;
pub mod sst;
pub mod wal;

const WAL_FILE_NAME: &'static str = "data.wal";

pub struct Store {
    memtable: MemTable,
    wal: wal::Writer,
}

impl Store {
    pub fn new(data_dir: &path::Path) -> Self {
        // Read existing wal records into the initial memtable. For now we are leaving the wal alone
        // and always re-reading and only ever appending to it.
        let wal_file_path = data_dir.join(&WAL_FILE_NAME);

        // TODO: Convert any existing wal file into an SST. Then initialize a new wal for this
        // invocation.
        let wal = wal::Writer::new(&wal_file_path);
        let memtable = wal::Reader::new(&wal_file_path).into_iter().collect();

        Store { memtable, wal }
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) {
        self.wal.append(WriteRecord::Exists { key, val });
        self.memtable.put(key, val);
    }

    pub fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        self.memtable.get(key)
    }

    pub fn del(&mut self, key: &[u8]) {
        self.wal.append(WriteRecord::Deleted { key });
        self.memtable.del(key)
    }
}
