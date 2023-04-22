use std::{io::Read, io::Write};

const EXISTS_OP_BYTE: u8 = b'0';
const DELETED_OP_BYTE: u8 = b'1';

pub enum WriteRecord<'a> {
    Exists { key: &'a [u8], val: &'a [u8] },
    Deleted { key: &'a [u8] },
}

impl<'a> WriteRecord<'a> {
    pub fn write_to<T: Write>(&self, w: &mut T) -> u32 {
        let mut written = 0;

        let (op_byte, key, val) = match self {
            WriteRecord::Exists { key, val } => (EXISTS_OP_BYTE, key, Some(val)),
            WriteRecord::Deleted { key } => (DELETED_OP_BYTE, key, None),
        };

        let key_length = key.len() as u32;
        let val_length = if let Some(val) = val { val.len() } else { 0 } as u32;

        written += w.write(&vec![op_byte]).unwrap();
        written += w.write(&key_length.to_le_bytes()).unwrap();
        written += w.write(&val_length.to_le_bytes()).unwrap();

        written += w.write(key).unwrap();
        if let Some(val) = val {
            written += w.write(&val).unwrap();
        }

        written as u32
    }

    pub fn key(&self) -> &[u8] {
        match self {
            WriteRecord::Exists { key, .. } => key,
            WriteRecord::Deleted { key } => key,
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum ReadRecord {
    Exists { key: Vec<u8>, val: Vec<u8> },
    Deleted { key: Vec<u8> },
}

impl ReadRecord {
    pub fn read_from<R: Read>(reader: &mut R) -> Option<Self> {
        // Big enough for operation, key length, and val length
        // 1 byte + 4 bytes + 4 bytes
        let mut buf = [0; 9];

        // We might be at the end of the file, or it has a length of 0, or it has an incomplete
        // header portion.
        match reader.read(&mut buf) {
            Ok(9) => (),
            Ok(0) => return None,
            Ok(n) => panic!("bad header in record, had {} bytes", n),
            Err(e) => panic!("could not read wal record hreader: {}", e),
        }

        let key_length = u32::from_le_bytes(buf[1..5].try_into().unwrap());
        let mut key = vec![0; key_length as usize];
        reader.read_exact(&mut key).unwrap();

        match buf[0] {
            EXISTS_OP_BYTE => {
                let val_length = u32::from_le_bytes(buf[5..9].try_into().unwrap());
                let mut val = vec![0; val_length as usize];
                reader.read_exact(&mut val).unwrap();

                Some(ReadRecord::Exists { key: key, val: val })
            }
            DELETED_OP_BYTE => Some(ReadRecord::Deleted { key: key }),
            b => panic!("invalid op byte {}", b),
        }
    }
}
