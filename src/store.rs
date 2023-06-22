use std::{fs, io, path};

use crate::{
    compactor::compactor, memtable::MemTable, protocol::WriteRecord, sst::Catalog, wal, StoreError,
};

const WAL_FILE_NAME: &'static str = "data.wal";
const WAL_SIZE_LIMIT: u32 = 4 * 1024 * 1024;
const TABLE_SIZE_LIMIT: usize = 4 * 1024 * 1024;
const LEVEL_0_FILE_LIMIT: usize = 5;

pub struct Store {
    memtable: MemTable,
    wal: wal::Writer,
    catalog: Option<Catalog>,
    wal_size_limit: u32, // bytes
    wal_file_path: path::PathBuf,
    data_dir: path::PathBuf,
    compactor: compactor::Compactor,
}

impl Store {
    pub fn new(
        data_dir: &path::Path,
        wal_size_limit: Option<u32>,
        table_size_limit: Option<usize>,
        level_0_file_limit: Option<usize>,
    ) -> Result<Store, StoreError> {
        let wal_file_path = data_dir.join(&WAL_FILE_NAME);

        let mut sst = Catalog::new(&data_dir).map_err(|e| StoreError::CatalogInitialization(e))?;

        // Convert any left-over wal file into an sst.
        if let Some(len) = fs::metadata(&wal_file_path).ok().map(|meta| meta.len()) {
            if len > 0 {
                let memtable: MemTable = wal::Reader::new(&wal_file_path)
                    .map_err(|e| StoreError::WalRecovery(e))?
                    .into_iter()
                    .collect::<Result<MemTable, io::Error>>()
                    .map_err(|e| StoreError::WalRecovery(e))?;

                sst.write_records(&memtable) // Should be owned
                    .map_err(|e| StoreError::WalRecovery(e))?;
            }
        };

        Ok(Store {
            memtable: MemTable::new(),
            wal: wal::Writer::new(&wal_file_path).map_err(|e| StoreError::WalInitialization(e))?,
            catalog: Some(sst),
            wal_size_limit: wal_size_limit.unwrap_or(WAL_SIZE_LIMIT),
            wal_file_path,
            data_dir: data_dir.into(),
            compactor: compactor::Compactor::new(
                level_0_file_limit.unwrap_or(LEVEL_0_FILE_LIMIT),
                table_size_limit.unwrap_or(TABLE_SIZE_LIMIT),
                data_dir,
            ),
        })
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) -> io::Result<()> {
        self.exec_wal(|store| {
            store.wal.append(WriteRecord::Exists { key, val })?;
            store.memtable.put(key, val);
            Ok(())
        })
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        if let Some(val) = self.memtable.get(key) {
            Ok(Some(val.to_vec()))
        } else if let Some(rec) = self.catalog.as_ref().unwrap().get(key)? {
            match rec {
                crate::protocol::ReadRecord::Exists { val, .. } => Ok(Some(val)),
                crate::protocol::ReadRecord::Deleted { .. } => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    pub fn del(&mut self, key: &[u8]) -> io::Result<()> {
        self.exec_wal(|store| {
            store.wal.append(WriteRecord::Deleted { key })?;
            store.memtable.del(key);
            Ok(())
        })
    }

    fn exec_wal<T>(&mut self, mut f: T) -> io::Result<()>
    where
        T: FnMut(&mut Store) -> io::Result<()>,
    {
        f(self)?;

        if self.wal.size() > self.wal_size_limit {
            self.flush_memtable()?;
        }

        Ok(())
    }

    // TODO: Ideally this would be async.
    pub fn flush_memtable(&mut self) -> io::Result<()> {
        self.catalog
            .as_mut()
            .unwrap()
            .write_records(&self.memtable)?;
        self.wal = wal::Writer::new(&self.wal_file_path)?;
        self.memtable = MemTable::new();
        self.compactor
            .maybe_compact(self.catalog.take().unwrap().ssts)?;

        // TODO: Re-reading the entire SST catalog from disk every flush is going to be very
        // inefficient. This is a temporary placeholder.
        self.catalog = Some(Catalog::new(&self.data_dir)?);

        Ok(())
    }
}
