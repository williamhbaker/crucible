use std::{
    cmp,
    collections::BinaryHeap,
    fs,
    io::{self, BufWriter, Write},
    path,
};

use uuid::Uuid;

use crate::protocol::{self, ReadRecord};

pub struct CombineTable<T>
where
    T: Iterator<Item = io::Result<ReadRecord>>,
{
    pub table: T,
    pub level: usize,
    pub sequence: Option<u32>,
}

// TODO: A lot of this is redundant with Catalog::write_records. It would be nice to consolidate
// these two.
pub fn combine_tables<T: Iterator<Item = io::Result<ReadRecord>>>(
    tables: Vec<CombineTable<T>>,
    size_limit: usize, // Excluding index
    output_level: u32,
    output_dir: &path::Path,
) -> io::Result<()> {
    let mut merge = MergeIter::new();

    for table in tables {
        merge.push_iter(table.table, table.level, table.sequence)?;
    }

    let mut merge = merge.peekable();

    loop {
        let fname = Uuid::new_v4();
        let path = output_dir.join(format!("{}", output_level));
        // Create the directory if it doesn't yet exist.
        fs::create_dir_all(&path)?;
        let mut path = path.join(fname.to_string());
        path.set_extension(protocol::SST_EXT);

        let file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)?;

        let mut w = BufWriter::new(&file);
        let mut written = 0;
        // Tuple of (key, offset)
        let mut index_offsets: Vec<(Vec<u8>, usize)> = Vec::new();

        let mut start_key = vec![];
        let mut end_key = vec![];
        if let Some(record) = merge.peek() {
            match record {
                Ok(r) => start_key = r.key().to_vec(),
                Err(_) => {
                    merge.next().expect("next must exist when peeked")?;
                }
            }
        }

        while written < size_limit {
            if let Some(record) = merge.next() {
                let record = record?;
                index_offsets.push((record.key().to_vec(), written));
                written += record.write_to(&mut w)?;
                end_key = record.key().to_vec();
            } else {
                break;
            }
        }

        // Hit the size limit or ran out of records, so now write out the index and finish the file.
        for (key, offset) in index_offsets.into_iter() {
            w.write(&(offset as u32).to_le_bytes())?;
            w.write(&(key.len() as u32).to_le_bytes())?;
            w.write(&key)?;
        }

        // Write the footer.
        let footer = protocol::Footer {
            start_key,
            end_key,
            index_start: written as u32,
            footer_length: None,
        };
        footer.write_to(&mut w)?;

        w.flush()?;
        file.sync_all()?;

        if merge.peek().is_none() {
            return Ok(());
        }
    }
}

struct IterBuf<T>
where
    T: Iterator<Item = io::Result<ReadRecord>>,
{
    iter: T,
    buf: Option<ReadRecord>,
    level: usize,
    sequence: Option<u32>,
}

impl<T> Ord for IterBuf<T>
where
    T: Iterator<Item = io::Result<ReadRecord>>,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (&self.buf, &other.buf) {
            (Some(s), Some(o)) => {
                if o.key() != s.key() {
                    // Always order by keys (ascending) as the primary criteria. If the keys are
                    // equal, order in ascending age.
                    return o.key().cmp(s.key());
                } else if self.level != other.level {
                    // Lower levels are newer.
                    return other.level.cmp(&self.level);
                }

                // Higher sequence within the same level (only possible in level 0) are newer.
                match (&self.sequence, &other.sequence) {
                    (Some(ss), Some(os)) => ss.cmp(&os),
                    (_, _) => {
                        unreachable!("cannot have equal keys within the same level and sequence")
                    }
                }
            }
            (Some(_), None) => cmp::Ordering::Greater,
            (None, Some(_)) => cmp::Ordering::Less,
            (None, None) => cmp::Ordering::Equal,
        }
    }
}

impl<T> PartialOrd for IterBuf<T>
where
    T: Iterator<Item = io::Result<ReadRecord>>,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Eq for IterBuf<T> where T: Iterator<Item = io::Result<ReadRecord>> {}

impl<T> PartialEq for IterBuf<T>
where
    T: Iterator<Item = io::Result<ReadRecord>>,
{
    fn eq(&self, other: &Self) -> bool {
        match (&self.buf, &other.buf) {
            (Some(s), Some(o)) => s.key() == o.key(),
            (None, None) => true,
            (None, Some(_)) => false,
            (Some(_), None) => false,
        }
    }
}

struct MergeIter<T>
where
    T: Iterator<Item = io::Result<ReadRecord>>,
{
    iters: BinaryHeap<IterBuf<T>>,
}

impl<T> MergeIter<T>
where
    T: Iterator<Item = io::Result<ReadRecord>>,
{
    pub fn new() -> Self {
        return MergeIter {
            iters: BinaryHeap::new(),
        };
    }

    pub fn push_iter(
        &mut self,
        mut iter: T,
        level: usize,
        sequence: Option<u32>,
    ) -> io::Result<()> {
        let buf = iter.next().transpose()?;
        Ok(self.iters.push(IterBuf {
            iter,
            buf,
            level,
            sequence,
        }))
    }
}

impl<T> Iterator for MergeIter<T>
where
    T: Iterator<Item = io::Result<ReadRecord>>,
{
    type Item = io::Result<ReadRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get the highest priority iterator.
        let mut n = self.iters.pop()?;
        let record = n.buf.expect("Buffer must not be None");

        // This might be the first of this key value and if it is it's the newest, so clear out
        // everything with the same key value.
        while let Some(nn) = self.iters.peek() {
            let next_record = nn.buf.as_ref().expect("Buffer must not be None");
            if record.key() == next_record.key() {
                // Pop and re-fill the buffer of the next iterator.
                if let Some(mut popped) = self.iters.pop() {
                    if let Some(buf) = popped.iter.next() {
                        let buf = match buf {
                            Ok(b) => b,
                            Err(e) => return Some(Err(e)),
                        };

                        popped.buf = Some(buf);
                        self.iters.push(popped);
                    }
                };
            } else {
                break;
            }
        }

        // Put this iterator back in, first re-filling its buffer, as long as the iterator isn't
        // empty.
        if let Some(new_buf) = n.iter.next() {
            let new_buf = match new_buf {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };

            n.buf = Some(new_buf);
            self.iters.push(n)
        }

        Some(Ok(record))
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use crate::sst::Catalog;

    use super::*;

    #[test]
    fn test_combine_tables() {
        let records_1 = vec![
            ReadRecord::Exists {
                key: b"key1".to_vec(),
                val: b"val1_1".to_vec(),
            },
            ReadRecord::Exists {
                key: b"key2".to_vec(),
                val: b"val2_1".to_vec(),
            },
        ]
        .into_iter()
        .map(|item| Ok(item))
        .collect::<Vec<io::Result<ReadRecord>>>()
        .into_iter();

        let records_2 = vec![
            ReadRecord::Exists {
                key: b"key2".to_vec(),
                val: b"val2_2".to_vec(),
            },
            ReadRecord::Exists {
                key: b"key3".to_vec(),
                val: b"val3_2".to_vec(),
            },
            ReadRecord::Exists {
                key: b"key4".to_vec(),
                val: b"val4_2".to_vec(),
            },
            ReadRecord::Deleted {
                key: b"key6".to_vec(),
            },
        ]
        .into_iter()
        .map(|item| Ok(item))
        .collect::<Vec<io::Result<ReadRecord>>>()
        .into_iter();

        let records_3 = vec![
            ReadRecord::Exists {
                key: b"key2".to_vec(),
                val: b"val2_3".to_vec(),
            },
            ReadRecord::Exists {
                key: b"key5".to_vec(),
                val: b"val5_3".to_vec(),
            },
        ]
        .into_iter()
        .map(|item| Ok(item))
        .collect::<Vec<io::Result<ReadRecord>>>()
        .into_iter();

        let records_4 = vec![ReadRecord::Exists {
            key: b"key6".to_vec(),
            val: b"val6_4".to_vec(),
        }]
        .into_iter()
        .map(|item| Ok(item))
        .collect::<Vec<io::Result<ReadRecord>>>()
        .into_iter();

        let tables = vec![
            CombineTable {
                table: records_1,
                level: 0,
                sequence: Some(0),
            },
            CombineTable {
                table: records_2,
                level: 0,
                sequence: Some(1),
            },
            CombineTable {
                table: records_3,
                level: 1,
                sequence: None,
            },
            CombineTable {
                table: records_4,
                level: 2,
                sequence: None,
            },
        ];

        let dir = TempDir::new("testing").unwrap();
        combine_tables(tables, 1024 * 1024, 1, dir.path()).unwrap();

        let catalog = Catalog::new(&dir.path()).unwrap();

        let cases = vec![
            (
                b"key1",
                ReadRecord::Exists {
                    key: b"key1".to_vec(),
                    val: b"val1_1".to_vec(),
                },
            ),
            (
                b"key2",
                ReadRecord::Exists {
                    key: b"key2".to_vec(),
                    val: b"val2_2".to_vec(),
                },
            ),
            (
                b"key3",
                ReadRecord::Exists {
                    key: b"key3".to_vec(),
                    val: b"val3_2".to_vec(),
                },
            ),
            (
                b"key4",
                ReadRecord::Exists {
                    key: b"key4".to_vec(),
                    val: b"val4_2".to_vec(),
                },
            ),
            (
                b"key5",
                ReadRecord::Exists {
                    key: b"key5".to_vec(),
                    val: b"val5_3".to_vec(),
                },
            ),
            (
                b"key6",
                ReadRecord::Deleted {
                    key: b"key6".to_vec(),
                },
            ),
        ];

        for (key, want) in cases.into_iter() {
            assert_eq!(Some(want), catalog.get(key).unwrap());
        }
    }
}
