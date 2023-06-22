use std::{
    collections::HashMap,
    fs,
    io::{self, BufWriter, Write},
    path,
};

use crate::protocol::{self, ReadRecord, WriteRecord, SST_EXT};

use super::Table;

pub struct Catalog {
    pub ssts: Vec<Vec<Table>>, // Index 0 is level 0, 1 is 1, etc.
    watermark: u32,
    data_dir: path::PathBuf,
}

impl Catalog {
    pub fn new(data_dir: &path::Path) -> io::Result<Self> {
        let mut dirs = fs::read_dir(data_dir)?
            .into_iter()
            .collect::<io::Result<Vec<fs::DirEntry>>>()?
            .into_iter()
            .filter(|entry| entry.path().is_dir())
            .collect::<Vec<fs::DirEntry>>();

        // Directories will be sorted ascending by the integer value of their name. Each of these
        // directories represents a compaction level. Level 0 is special and contains the flushed
        // memtables that have not undergone any compaction: These tables will have overlapping key
        // ranges. Tables at higher levels will not have overlapping key ranges.
        dirs.sort_unstable_by_key(|dir| {
            dir.file_name()
                .to_str()
                .expect("must convert dir name to string")
                .parse::<usize>()
                .expect("must parse dir name as usize")
        });

        let mut watermark = 0;
        let mut ssts = vec![];

        for (level, dir) in dirs.iter().enumerate() {
            let mut these_ssts = vec![];

            let files = fs::read_dir(dir.path()).unwrap();

            let mut files = files
                .into_iter()
                .collect::<io::Result<Vec<fs::DirEntry>>>()
                .unwrap()
                .into_iter()
                .filter(|file| file.path().is_file())
                .collect::<Vec<fs::DirEntry>>();

            // Sort files in level 0 in ascending order.
            if level == 0 {
                files.sort_unstable_by_key(|file| {
                    file.path()
                        .file_stem()
                        .unwrap()
                        .to_string_lossy()
                        .parse::<usize>()
                        .unwrap()
                });
            }

            files.into_iter().for_each(|file| {
                let path = file.path();
                if let Some(ext) = path.extension() {
                    if ext.eq_ignore_ascii_case(SST_EXT) {
                        if level == 0 {
                            // Parse the file stem (name without extension) to update the watermark.
                            // The files are already sorted in ascending order.
                            if let Some(seq) = table_sequence(&path) {
                                watermark = seq;
                            }
                        }

                        these_ssts.push(Table::new(&path).unwrap());
                    }
                }
            });

            ssts.push(these_ssts);
        }

        Ok(Catalog {
            ssts,
            watermark,
            data_dir: data_dir.to_owned(),
        })
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<ReadRecord>> {
        // Start at the lowest level (newest data) and check newest to oldest tables for the record.
        // The first one found is returned.
        for level in self.ssts.iter() {
            for sst in level.iter().rev() {
                if let Some(rec) = sst.get(key)? {
                    return Ok(Some(rec));
                }
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

        // Flush to level 0 exclusively.
        let mut path: path::PathBuf = path::PathBuf::from(&self.data_dir).join("0");
        // The level 0 directory may not exist yet.
        fs::create_dir_all(&path)?;

        path = path.join(format!("{}", &self.watermark + 1));
        path.set_extension(SST_EXT);

        let file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)?;

        let mut w = BufWriter::new(&file);

        let mut index_offsets: HashMap<&[u8], u32> = HashMap::new();

        // Write the records. After the records comes the index.
        let index_start = sorted_records.iter().try_fold(0, |written, record| {
            index_offsets.insert(record.key(), written);
            Ok::<u32, io::Error>(written + record.write_to(&mut w)? as u32)
        })?;

        // Write the index.
        for record in &sorted_records {
            let key = record.key();

            let offset = index_offsets
                .get(record.key())
                .expect("must get key that was just written");
            w.write(&offset.to_le_bytes())?;

            w.write(&(key.len() as u32).to_le_bytes())?;
            w.write(key)?;
        }

        // Write the footer.
        let footer = protocol::Footer {
            start_key: sorted_records
                .first()
                .expect("records must not be empty")
                .key()
                .to_owned(),
            end_key: sorted_records
                .last()
                .expect("records must not be empty")
                .key()
                .to_owned(),
            index_start,
            footer_length: None,
        };
        footer.write_to(&mut w)?;

        w.flush()?;
        file.sync_all()?;

        // TODO: Instead of reading in this file that was just written, build the SST index while
        // writing it.
        let new = Table::new(&path)?;

        // Add the new table, which must be the highest numbered, to the end of the list of level 0
        // tables. This preserves the requirement that the tables be in order of oldest to newest.
        if self.ssts.len() == 0 {
            self.ssts.push(Vec::new());
        }
        self.ssts[0].push(new);

        self.watermark += 1;

        Ok(())
    }
}

fn table_sequence(path: &path::Path) -> Option<u32> {
    path.file_stem().unwrap().to_string_lossy().parse().ok()
}
