use std::{fs, io, path};

use crate::sst::table::Table;

use super::combiner::{combine_tables, CombineTable};

pub struct Compactor {
    level_0_file_limit: usize,
    table_size_limit: usize,
    data_dir: path::PathBuf,
}

impl Compactor {
    pub fn new(level_0_file_limit: usize, table_size_limit: usize, data_dir: &path::Path) -> Self {
        Compactor {
            level_0_file_limit,
            table_size_limit,
            data_dir: data_dir.to_owned(),
        }
    }

    pub fn maybe_compact(&self, ssts: Vec<Vec<Table>>) -> io::Result<()> {
        if ssts[0].len() >= self.level_0_file_limit {
            self.compact_level_0(ssts)
        } else {
            Ok(())
        }
    }

    fn compact_level_0(&self, ssts: Vec<Vec<Table>>) -> io::Result<()> {
        let mut key_start = Vec::new();
        let mut key_end = Vec::new();

        let mut tables_to_delete = Vec::new();
        let mut tables_to_combine = Vec::new();

        let mut sst_iter = ssts.into_iter();

        for (i, table) in sst_iter
            .next()
            .expect("ssts must have at least 1 level 1 table")
            .into_iter()
            .enumerate()
        {
            if key_start.len() == 0 || table.key_start() < key_start {
                key_start = table.key_start();
            }

            if key_end.len() == 0 || table.key_end() > key_end {
                key_end = table.key_end();
            }

            tables_to_delete.push(table.path.clone());
            tables_to_combine.push(CombineTable {
                table: table.into_iter(),
                level: 0,
                sequence: Some(i as u32),
            });
        }

        if let Some(tables) = sst_iter.next() {
            for table in tables {
                if (table.key_start() >= key_start && table.key_start() <= key_end)
                    || (table.key_end() >= key_start && table.key_end() <= key_end)
                {
                    tables_to_delete.push(table.path.clone());
                    tables_to_combine.push(CombineTable {
                        table: table.into_iter(),
                        level: 1,
                        sequence: None,
                    });
                }
            }
        }

        combine_tables(tables_to_combine, self.table_size_limit, 1, &self.data_dir)?;

        for t in tables_to_delete {
            // TODO: This is unlikely to be stricly correct since there is no guarantee that the
            // file is immediately deleted.
            fs::remove_file(t)?;
        }

        Ok(())
    }
}
