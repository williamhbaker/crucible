use std::{
    io::Read,
    io::{self, Error, ErrorKind, Write},
};

const EXISTS_OP_BYTE: u8 = b'0';
const DELETED_OP_BYTE: u8 = b'1';

pub enum WriteRecord<'a> {
    Exists { key: &'a [u8], val: &'a [u8] },
    Deleted { key: &'a [u8] },
}

impl<'a> WriteRecord<'a> {
    pub fn write_to<T: Write>(&self, w: &mut T) -> io::Result<usize> {
        let mut written = 0;

        let (op_byte, key, val) = match self {
            WriteRecord::Exists { key, val } => (EXISTS_OP_BYTE, key, Some(val)),
            WriteRecord::Deleted { key } => (DELETED_OP_BYTE, key, None),
        };

        let key_length = key.len() as u32;
        let val_length = if let Some(val) = val { val.len() } else { 0 } as u32;

        written += w.write(&vec![op_byte])?;
        written += w.write(&key_length.to_le_bytes())?;
        written += w.write(&val_length.to_le_bytes())?;

        written += w.write(key)?;
        if let Some(val) = val {
            written += w.write(&val)?;
        }

        Ok(written)
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
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Option<Self>> {
        // Big enough for operation, key length, and val length
        // 1 byte + 4 bytes + 4 bytes
        let mut buf = [0; 9];

        if fill_buf(reader, &mut buf, 0)?.is_none() {
            return Ok(None);
        }

        let key_length = u32::from_le_bytes(
            buf[1..5]
                .try_into()
                .expect("must convert slice to byte array"),
        );
        let mut key = vec![0; key_length as usize];
        reader.read_exact(&mut key)?;

        match buf[0] {
            EXISTS_OP_BYTE => {
                let val_length = u32::from_le_bytes(
                    buf[5..9]
                        .try_into()
                        .expect("must convert slice to byte array"),
                );
                let mut val = vec![0; val_length as usize];
                reader.read_exact(&mut val)?;

                Ok(Some(ReadRecord::Exists { key: key, val: val }))
            }
            DELETED_OP_BYTE => Ok(Some(ReadRecord::Deleted { key: key })),
            b => panic!("invalid op byte {}", b),
        }
    }

    pub fn key(&self) -> &[u8] {
        match self {
            ReadRecord::Exists { key, .. } => key,
            ReadRecord::Deleted { key } => key,
        }
    }
}

pub fn fill_buf<R: Read>(
    reader: &mut R,
    buf: &mut [u8],
    trailer: usize,
) -> io::Result<Option<usize>> {
    let mut read = 0;
    let mut start = 0;
    let end = buf.len();

    loop {
        match reader.read(&mut buf[start..end]) {
            Ok(0) => {
                // EOF. If we've only read the trailer, it means we got here cleanly. Otherwise, we
                // couldn't read as much as we thought and should error.
                return if read == trailer {
                    Ok(None)
                } else {
                    Err(Error::from(ErrorKind::UnexpectedEof))
                };
            }
            Ok(n) if n == (end - start) => {
                // Filled the buffer, maybe not on the first pass.
                read += n;
                return Ok(Some(read));
            }
            Ok(n) => {
                // Read some bytes but not enough, so try again.
                read += n;
                start += n;
            }
            Err(e) => {
                match e.kind() {
                    ErrorKind::Interrupted => (), // Ignore & keep going
                    _ => return Err(e),
                }
            }
        };
    }
}
