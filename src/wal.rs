use std::{
    fs,
    io::{self, BufReader, BufWriter, Write},
    path,
};

use crate::protocol::{ReadRecord, WriteRecord};

pub struct Writer {
    w: BufWriter<fs::File>,
}

impl Writer {
    pub fn new(path: &path::Path) -> io::Result<Self> {
        Ok(Writer {
            w: BufWriter::new(
                fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .create(true)
                    .open(path)?,
            ),
        })
    }

    pub fn append(&mut self, rec: WriteRecord) -> io::Result<usize> {
        let written = rec.write_to(&mut self.w)?;
        self.w.flush()?;
        // TODO: Compare to sync_data().
        self.w.get_ref().sync_all()?;
        Ok(written)
    }
}

pub struct Reader {
    r: BufReader<fs::File>,
    done: bool,
}

impl Reader {
    pub fn new(path: &path::Path) -> io::Result<Self> {
        Ok(Reader {
            r: BufReader::new(fs::OpenOptions::new().read(true).create(false).open(path)?),
            done: false,
        })
    }
}

impl Iterator for Reader {
    type Item = io::Result<ReadRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let next = ReadRecord::read_from(&mut self.r);
        if next.is_err() {
            self.done = true;
        }

        next.transpose()
    }
}
