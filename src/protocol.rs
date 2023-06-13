use std::{
    io::Read,
    io::{self, Error, ErrorKind, Seek, SeekFrom, Write},
};

const EXISTS_OP_BYTE: u8 = b'0';
const DELETED_OP_BYTE: u8 = b'1';
pub const SST_EXT: &'static str = "sst";

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

        // This should use a magic byte or something to tell if it's at the end, rather than the
        // weird other thing.
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

    pub fn write_to<T: Write>(&self, w: &mut T) -> io::Result<usize> {
        let mut written = 0;

        let (op_byte, key, val) = match self {
            ReadRecord::Exists { key, val } => (EXISTS_OP_BYTE, key, Some(val)),
            ReadRecord::Deleted { key } => (DELETED_OP_BYTE, key, None),
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
            ReadRecord::Exists { key, .. } => key,
            ReadRecord::Deleted { key } => key,
        }
    }
}

#[derive(Default)]
pub struct Footer {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub index_start: u32,
    // Includes the value for footer_length itself, which is 4 bytes. Will be None will initializing
    // a footer for a new table, but should always be Some(...) when decoding the footer from a
    // table.
    pub footer_length: Option<u32>,
}

impl Footer {
    pub fn new_from_reader<T: Read + Seek>(r: &mut T) -> io::Result<Self> {
        let mut footer = Footer {
            ..Default::default()
        };

        r.seek(SeekFrom::End(-4))?;

        let mut buf = [0; 4];
        let footer_length = read_u32(r, &mut buf)?;
        footer.footer_length = Some(footer_length);

        r.seek(SeekFrom::End(0 - footer_length as i64))?;

        let start_key_length = read_u32(r, &mut buf)?;
        footer.start_key = vec![0; start_key_length as usize];
        r.read_exact(&mut footer.start_key)?;

        let end_key_length = read_u32(r, &mut buf)?;
        footer.end_key = vec![0; end_key_length as usize];
        r.read_exact(&mut footer.end_key)?;

        footer.index_start = read_u32(r, &mut buf)?;

        Ok(footer)
    }

    pub fn write_to<T: Write>(&self, w: &mut T) -> io::Result<usize> {
        let mut written = 0;

        written += w.write(&(self.start_key.len() as u32).to_le_bytes())?;
        written += w.write(&self.start_key)?;
        written += w.write(&(self.end_key.len() as u32).to_le_bytes())?;
        written += w.write(&self.end_key)?;
        written += w.write(&self.index_start.to_le_bytes())?;
        written += w.write(&(written as u32 + 4).to_le_bytes())?;

        Ok(written)
    }
}

fn read_u32<T: Read>(r: &mut T, buf: &mut [u8; 4]) -> io::Result<u32> {
    r.read_exact(buf)?;

    Ok(u32::from_le_bytes(
        buf[0..4]
            .try_into()
            .expect("must convert slice to byte array"),
    ))
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
