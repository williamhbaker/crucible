use std::{
    fs,
    io::{self, BufReader, BufWriter, Write},
    path,
};

use crate::protocol::{ReadRecord, WriteRecord};

pub struct Writer {
    w: BufWriter<fs::File>,
    size: u32,
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
            size: 0,
        })
    }

    pub fn append(&mut self, rec: WriteRecord) -> io::Result<usize> {
        let written = rec.write_to(&mut self.w)?;
        self.w.flush()?;
        // TODO: Compare to sync_data().
        self.w.get_ref().sync_all()?;
        self.size += written as u32;
        Ok(written)
    }

    pub fn size(&self) -> u32 {
        self.size
    }
}

pub struct Reader {
    r: BufReader<fs::File>,
    done: bool,
    size: u32,
    read: u32,
}

impl Reader {
    pub fn new(path: &path::Path) -> io::Result<Self> {
        let f = fs::OpenOptions::new().read(true).create(false).open(path)?;
        let size = f.metadata()?.len() as u32;

        Ok(Reader {
            r: BufReader::new(f),
            done: false,
            size,
            read: 0,
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
            return Some(next);
        }

        let next = next.unwrap();
        self.read += next.size() as u32;
        if self.read == self.size {
            self.done = true;
        }

        Some(Ok(next))
    }
}
