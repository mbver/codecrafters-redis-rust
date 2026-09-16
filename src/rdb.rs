use std::{
    fs::File,
    io::{self, BufReader, Read},
    path::Path,
};

const OP_EOF: u8 = 0xFF;
const OP_METADATA: u8 = 0xFA;
const OP_DB: u8 = 0xFE;
const OP_DB_SIZE: u8 = 0xFB;
const OP_STRING_TYPE: u8 = 0x00;
const OP_EXPIRY_MS: u8 = 0xFC;
const OP_EXPIRY_SEC: u8 = 0xFD;
const TAG_UINT32: u8 = 0b10000000;
const TAG_UINT64: u8 = 0b10000001;
// should populate the redis instance, not only result
fn load_rdb_file(path: &Path) -> io::Result<()> {
    let mut file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut rdb_reader = RdbReader::new(reader);
    let _ = rdb_reader.read_header()?;

    Ok(())
}

struct RdbReader<R: Read> {
    reader: R,
}

impl<R: Read> RdbReader<R> {
    fn new(reader: R) -> Self {
        Self { reader }
    }

    fn read_header(&mut self) -> io::Result<String> {
        let mut buf = [0u8; 9];
        self.reader.read_exact(&mut buf)?;

        if &buf != b"REDIS0011" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid RDB header",
            ));
        }
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }

    fn read_length(&mut self) -> io::Result<u64> {
        let first = self.read_u8()?;
        let value = match first >> 6 {
            0b00 => (first & 0x3f) as u64,
            0b01 => {
                let second = self.read_u8()?;
                ((first & 0x3f) as u64) << 8 | second as u64
            }
            0b10 if first == TAG_UINT32 => {
                let mut buf = [0u8; 4];
                self.reader.read_exact(&mut buf)?;
                u32::from_be_bytes(buf) as u64
            }
            0b10 if first == TAG_UINT64 => {
                let mut buf = [0u8; 8];
                self.reader.read_exact(&mut buf)?;
                u64::from_be_bytes(buf)
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid length header",
                ));
            }
        };
        Ok(value)
    }

    fn read_u8(&mut self) -> io::Result<u8> {
        let mut buf = [0u8; 1];
        self.reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }
}
