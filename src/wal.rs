use std::{
    fs,
    io::{BufReader, BufWriter, Write},
    path,
};

use crate::protocol::{ReadRecord, WriteRecord};

pub struct Writer {
    w: BufWriter<fs::File>,
}

impl Writer {
    pub fn new(path: &path::Path) -> Self {
        Writer {
            w: BufWriter::new(
                fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .create(true)
                    .open(path)
                    .unwrap(),
            ),
        }
    }

    pub fn append(&mut self, rec: WriteRecord) {
        rec.write_to(&mut self.w);
        self.w.flush().unwrap();
        // TODO: Compare to sync_data().
        self.w.get_ref().sync_all().unwrap();
    }
}

pub struct Reader(BufReader<fs::File>);

impl Reader {
    pub fn new(path: &path::Path) -> Self {
        Reader(BufReader::new(
            fs::OpenOptions::new()
                .read(true)
                .create(false)
                .open(path)
                .unwrap(),
        ))
    }
}

impl Iterator for Reader {
    type Item = ReadRecord;

    fn next(&mut self) -> Option<Self::Item> {
        ReadRecord::read_from(&mut self.0)
    }
}
