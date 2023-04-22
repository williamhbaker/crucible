use std::{
    fs,
    io::{BufReader, BufWriter, Write},
    path,
};

use crate::protocol::{ReadRecord, WriteRecord};

pub struct Wal {
    file: fs::File,
    path: path::PathBuf,
}

impl Wal {
    pub fn new(path: &path::Path) -> Self {
        let file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .unwrap();

        Wal {
            file,
            path: path.into(),
        }
    }

    pub fn append(&mut self, rec: WriteRecord) {
        let mut w = BufWriter::new(&self.file); // TODO: Re-use?

        rec.write_to(&mut w);
        w.flush().unwrap();
        self.file.sync_all().unwrap();
    }
}

impl IntoIterator for Wal {
    type Item = ReadRecord;
    type IntoIter = Iter;

    fn into_iter(self) -> Self::IntoIter {
        let file = fs::OpenOptions::new()
            .read(true)
            .create(false)
            .open(self.path)
            .unwrap();

        Iter(BufReader::new(file))
    }
}

pub struct Iter(BufReader<fs::File>);

impl Iterator for Iter {
    type Item = ReadRecord;

    fn next(&mut self) -> Option<Self::Item> {
        ReadRecord::read_from(&mut self.0)
    }
}
