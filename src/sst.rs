use std::{
    cmp,
    collections::HashMap,
    fs,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{self},
};

use crate::protocol::{ReadRecord, WriteRecord};

const SST_EXT: &'static str = "sst";

pub struct Catalog {
    ssts: Vec<Table>,
    watermark: usize,
    data_dir: path::PathBuf,
}

fn table_sequence(path: &path::Path) -> usize {
    path.file_stem().unwrap().to_string_lossy().parse().unwrap()
}

impl Catalog {
    pub fn new(data_dir: &path::Path) -> Self {
        let mut watermark = 0;

        let mut ssts: Vec<Table> = fs::read_dir(data_dir)
            .unwrap()
            .into_iter()
            .filter_map(|entry| {
                let entry = entry.unwrap();
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
            .collect();

        // Tables must be sorted oldest to newest. The newest tables will be queried first on
        // lookups.
        ssts.sort_unstable_by_key(|t| cmp::Reverse(t.sequence));

        Catalog {
            ssts,
            watermark,
            data_dir: data_dir.to_owned(),
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Option<ReadRecord> {
        for sst in self.ssts.iter_mut().rev() {
            if let Some(rec) = sst.get(key) {
                return Some(rec);
            }
        }

        None
    }

    pub fn write_records<'a, T: IntoIterator<Item = WriteRecord<'a>>>(&mut self, records: T) {
        let mut sorted_records: Vec<WriteRecord> = records.into_iter().collect();
        sorted_records.sort_unstable_by_key(|v| v.key().to_vec());

        let mut path: path::PathBuf = path::PathBuf::from(&self.data_dir);
        path = path.join(format!("{}", &self.watermark + 1));
        path.set_extension(SST_EXT);

        let file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)
            .unwrap();

        let mut w = BufWriter::new(&file);

        let mut index_offsets: HashMap<&[u8], u32> = HashMap::new();

        let index_start = sorted_records.iter().fold(0, |written, record| {
            index_offsets.insert(record.key(), written);
            written + record.write_to(&mut w)
        });

        for record in &sorted_records {
            let key = record.key();

            let offset = index_offsets.get(record.key()).unwrap();
            w.write(&offset.to_le_bytes()).unwrap();

            w.write(&(key.len() as u32).to_le_bytes()).unwrap();
            w.write(key).unwrap();
        }

        // The final byte is the file offset where the index begins.
        w.write(&index_start.to_le_bytes()).unwrap();

        w.flush().unwrap();
        file.sync_all().unwrap();

        // TODO: Instead of reading in this file that was just written, build the SST index while
        // writing it.
        let new = Table::new(&path);

        // Add the new table, which must be the highest numbered, to the end of the list of tables.
        // This preserves the requirement that the tables be in order of oldest to newest.
        self.ssts.push(new);

        self.watermark += 1;
    }
}

pub struct Table {
    index: Index,
    sequence: usize,
    r: BufReader<fs::File>,
}

struct Index(HashMap<Vec<u8>, u32>); // Keys (as byte slices) to file offsets

impl Index {
    fn get_offset(&self, key: &[u8]) -> Option<&u32> {
        self.0.get(key)
    }
}

impl FromIterator<IndexEntry> for Index {
    fn from_iter<I: IntoIterator<Item = IndexEntry>>(iter: I) -> Self {
        let mut map = HashMap::new();

        for i in iter {
            map.insert(i.key, i.offset);
        }

        Index(map)
    }
}

impl Table {
    pub fn new(path: &path::Path) -> Self {
        let file = fs::OpenOptions::new().read(true).open(path).unwrap();
        let mut r = BufReader::new(file);
        let index = IndexReader(&mut r).into_iter().collect();

        Table {
            index,
            sequence: table_sequence(&path),
            r,
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Option<ReadRecord> {
        match self.index.get_offset(key) {
            Some(offset) => {
                self.r.seek(SeekFrom::Start(*offset as u64)).unwrap();
                // There should always be a record here since we found it in the index.
                Some(ReadRecord::read_from(&mut self.r).unwrap())
            }
            None => None,
        }
    }
}

struct IndexReader<T: Read + Seek>(T);

impl<T: Read + Seek> IntoIterator for IndexReader<T> {
    type Item = IndexEntry;
    type IntoIter = IndexIter<T>;

    fn into_iter(mut self) -> Self::IntoIter {
        self.0.seek(SeekFrom::End(-4)).unwrap();

        // 4 bytes for the starting offset of the index in the file.
        let mut buf = [0; 4];
        self.0.read_exact(&mut buf).unwrap();

        let index_start = u32::from_le_bytes(buf[0..4].try_into().unwrap());

        self.0.seek(SeekFrom::Start(index_start as u64)).unwrap();

        IndexIter(self.0)
    }
}

struct IndexIter<T: Read>(T);

impl<T: Read> Iterator for IndexIter<T> {
    type Item = IndexEntry;

    fn next(&mut self) -> Option<Self::Item> {
        // Read record offset & key length. 4 bytes each.
        let mut buf = [0; 8];
        match self.0.read(&mut buf) {
            Ok(8) => (),
            Ok(4) => return None, // EOF since the footer is 4 bytes
            Ok(n) => panic!("bad header in index record, had {} bytes", n),
            Err(e) => panic!("could not read index record header: {}", e),
        }

        let offset = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let key_length = u32::from_le_bytes(buf[4..8].try_into().unwrap());

        let mut key = vec![0; key_length as usize];
        self.0.read_exact(&mut key).unwrap();

        Some(IndexEntry { key, offset })
    }
}

pub struct IndexEntry {
    key: Vec<u8>,
    offset: u32,
}
