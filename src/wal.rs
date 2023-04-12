use std::{
    fs,
    io::{BufWriter, Write},
    path,
};

#[derive(Default)]
pub struct WalRecord {
    pub op: Operation,
    pub key: Vec<u8>,
    pub val: Vec<u8>,
}

#[derive(Default, PartialEq, Debug)]
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
}

impl Wal {
    pub fn new(path: &path::Path) -> Self {
        let file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .unwrap();

        Wal { file }
    }

    pub fn append(&mut self, op: &Operation, key: &[u8], val: &[u8]) {
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

#[cfg(test)]
mod tests {
    use std::io::{BufReader, Read};

    use super::*;
    use tempdir::TempDir;

    // TODO: This will probably be moved elsewhere soon, but is implemented here to allow for testing.
    fn read_wal(path: &path::Path) -> Vec<WalRecord> {
        let wal_file = fs::OpenOptions::new().read(true).open(path).unwrap();
        let mut r = BufReader::new(&wal_file);

        let mut out = Vec::new();

        // Big enough for operation, key length, and val length
        // 1 byte + 4 bytes + 4 bytes
        let mut buf = [0; 9];

        loop {
            let mut rec = WalRecord::default();

            // We might be at the end of the file, or it has a length of 0, or it has an incomplete
            // header portion.
            match r.read(&mut buf) {
                Ok(9) => (),
                Ok(0) => break,
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
            r.read_exact(&mut rec.key).unwrap();

            rec.val = vec![0; val_length as usize];
            r.read_exact(&mut rec.val).unwrap();

            out.push(rec);
        }

        out
    }

    #[test]
    fn test_writes() {
        let dir = TempDir::new("testing").unwrap();
        let file = "data.wal";
        let dir_path = dir.path();
        let file_path = dir_path.join(&file);

        let mut wal = Wal::new(&file_path);

        let records = vec![
            WalRecord {
                op: Operation::Put,
                key: b"key1".to_vec(),
                val: b"val1".to_vec(),
            },
            WalRecord {
                op: Operation::Put,
                key: b"key2".to_vec(),
                val: b"val2".to_vec(),
            },
            WalRecord {
                op: Operation::Delete,
                key: b"key1".to_vec(),
                val: b"val1".to_vec(),
            },
        ];

        for r in &records {
            wal.append(&r.op, &r.key, &r.val)
        }

        let got = read_wal(&file_path);

        got.iter().zip(records.iter()).for_each(|(got, want)| {
            assert_eq!(got.op, want.op);
            assert_eq!(got.key, want.key);
            assert_eq!(got.val, want.val);
        });
    }
}
