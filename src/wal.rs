use std::{
    fs,
    io::{BufReader, BufWriter, Read, Write},
    path,
};

#[derive(Default)]
pub struct WalRecord {
    pub op: Operation,
    pub key: Vec<u8>,
    pub val: Vec<u8>,
}

#[derive(Default, PartialEq, Debug, Clone)]
pub enum Operation {
    #[default]
    Put,
    Delete,
}

impl Operation {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Operation::Put => &[b'0'],
            Operation::Delete => &[b'1'],
        }
    }
}

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

    pub fn append(&mut self, op: Operation, key: &[u8], val: &[u8]) {
        let mut w = BufWriter::new(&self.file); // TODO: Re-use?

        let key_length = key.len() as u32;
        let val_length = val.len() as u32;

        w.write(&op.as_bytes()).unwrap();
        w.write(&key_length.to_le_bytes()).unwrap();
        w.write(&val_length.to_le_bytes()).unwrap();
        w.write(&key).unwrap();
        w.write(&val).unwrap();
        w.flush().unwrap();

        self.file.sync_all().unwrap();
    }
}

pub struct IntoIter {
    r: BufReader<fs::File>,
}

impl Wal {
    pub fn into_iter(self) -> IntoIter {
        let file = fs::OpenOptions::new()
            .read(true)
            .create(false)
            .open(self.path)
            .unwrap();

        let r = BufReader::new(file);

        IntoIter { r }
    }
}

impl Iterator for IntoIter {
    type Item = WalRecord;

    fn next(&mut self) -> Option<Self::Item> {
        let mut rec = WalRecord::default();

        // Big enough for operation, key length, and val length
        // 1 byte + 4 bytes + 4 bytes
        let mut buf = [0; 9];

        // We might be at the end of the file, or it has a length of 0, or it has an incomplete
        // header portion.
        match self.r.read(&mut buf) {
            Ok(9) => (),
            Ok(0) => return None,
            Ok(n) => panic!("bad header in record, had {} bytes", n),
            Err(e) => panic!("could not read wal record hreader: {}", e),
        }

        match buf[0] {
            b'0' => rec.op = Operation::Put,
            b'1' => rec.op = Operation::Delete,
            b => panic!("invalid op byte {}", b),
        }

        let key_length = u32::from_le_bytes(buf[1..5].try_into().unwrap());
        let val_length = u32::from_le_bytes(buf[5..9].try_into().unwrap());

        rec.key = vec![0; key_length as usize];
        self.r.read_exact(&mut rec.key).unwrap();

        rec.val = vec![0; val_length as usize];
        self.r.read_exact(&mut rec.val).unwrap();

        Some(rec)
    }
}
