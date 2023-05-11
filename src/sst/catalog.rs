use std::{
    collections::HashMap,
    fs,
    io::{self, BufWriter, Write},
    path,
};

use crate::protocol::{ReadRecord, WriteRecord};

use super::{table_sequence, Table};

const SST_EXT: &'static str = "sst";

pub struct Catalog {
    ssts: Vec<Table>,
    watermark: usize,
    data_dir: path::PathBuf,
}

impl Catalog {
    pub fn new(data_dir: &path::Path) -> io::Result<Self> {
        let mut watermark = 0;

        let mut ssts = fs::read_dir(data_dir)?
            .into_iter()
            .collect::<io::Result<Vec<fs::DirEntry>>>()?
            .iter()
            .filter_map(|entry| {
                let path = entry.path();

                if !path.is_dir() {
                    if let Some(ext) = path.extension() {
                        if ext.eq_ignore_ascii_case(SST_EXT) {
                            // Parse the file stem (name without extension) to update the watermark.
                            let seq = table_sequence(&path);

                            if seq > watermark {
                                watermark = seq;
                            }

                            return Some(Table::new(&path));
                        }
                    }
                }

                None
            })
            .collect::<io::Result<Vec<Table>>>()?;

        // Tables must be sorted oldest to newest (ascending sequence). The newest tables will be
        // queried first on lookups. Newer tables have a higher sequence number.
        ssts.sort_unstable_by_key(|t| t.sequence);

        Ok(Catalog {
            ssts,
            watermark,
            data_dir: data_dir.to_owned(),
        })
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<ReadRecord>> {
        for sst in self.ssts.iter().rev() {
            if let Some(rec) = sst.get(key)? {
                return Ok(Some(rec));
            }
        }

        Ok(None)
    }

    pub fn write_records<'a, T: IntoIterator<Item = WriteRecord<'a>>>(
        &mut self,
        records: T,
    ) -> io::Result<()> {
        let mut sorted_records: Vec<WriteRecord> = records.into_iter().collect();
        sorted_records.sort_unstable_by_key(|v| v.key().to_vec());

        let mut path: path::PathBuf = path::PathBuf::from(&self.data_dir);
        path = path.join(format!("{}", &self.watermark + 1));
        path.set_extension(SST_EXT);

        let file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)?;

        let mut w = BufWriter::new(&file);

        let mut index_offsets: HashMap<&[u8], u32> = HashMap::new();

        let index_start = sorted_records.iter().try_fold(0, |written, record| {
            index_offsets.insert(record.key(), written);
            Ok::<u32, io::Error>(written + record.write_to(&mut w)? as u32)
        })?;

        for record in &sorted_records {
            let key = record.key();

            let offset = index_offsets
                .get(record.key())
                .expect("must get key that was just written");
            w.write(&offset.to_le_bytes())?;

            w.write(&(key.len() as u32).to_le_bytes())?;
            w.write(key)?;
        }

        // The final byte is the file offset where the index begins.
        w.write(&index_start.to_le_bytes())?;

        w.flush()?;
        file.sync_all()?;

        // TODO: Instead of reading in this file that was just written, build the SST index while
        // writing it.
        let new = Table::new(&path)?;

        // Add the new table, which must be the highest numbered, to the end of the list of tables.
        // This preserves the requirement that the tables be in order of oldest to newest.
        self.ssts.push(new);

        self.watermark += 1;

        Ok(())
    }
}
