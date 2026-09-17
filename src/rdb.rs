use std::{
    io::{self, BufReader, Read, Seek, SeekFrom},
    time::{Duration, SystemTime},
};

use crate::{helper::io_invalid_data, redis::Redis};

const OP_EOF: u8 = 0xFF;
const OP_METADATA: u8 = 0xFA;
const OP_DB: u8 = 0xFE;
const OP_DB_SIZE: u8 = 0xFB;
const OP_STRING_TYPE: u8 = 0x00;
const OP_EXPIRY_MS: u8 = 0xFC;
const OP_EXPIRY_SEC: u8 = 0xFD;

pub struct RdbReader<R: Read> {
    reader: BufReader<R>,
    metadata: Vec<(String, String)>,
    redis: Redis,
}

impl<R: Read + Seek> RdbReader<R> {
    pub fn new(reader: R) -> Self {
        let mut reader = BufReader::new(reader);
        let mut buf = vec![];
        let _ = reader.read_to_end(&mut buf);
        for byte in buf {
            print!("{byte:02x}");
        }
        println!("");
        let _ = reader.seek(SeekFrom::Start(0));
        Self {
            reader,
            metadata: Vec::new(),
            redis: Redis::new(),
        }
    }

    pub fn load(&mut self) -> io::Result<()> {
        self.read_header()?;
        println!("===== read header pass");
        let op_code = self.read_meta()?;
        println!("===== read meta pass");
        self.read_db(op_code)?;
        println!("===== read db pass");
        Ok(())
    }

    pub fn into_db(self) -> Redis {
        self.redis
    }

    fn read_header(&mut self) -> io::Result<()> {
        let mut buf = [0u8; 9];
        self.read_exact(&mut buf)?;

        if &buf != b"REDIS0011" {
            return Err(io_invalid_data("invalid RDB header"));
        }
        Ok(())
    }

    fn read_length(&mut self) -> io::Result<u64> {
        let first = self.read_u8()?;
        let value = match first >> 6 {
            0b00 => (first & 0x3f) as u64,
            0b01 => {
                let second = self.read_u8()?;
                ((first & 0x3f) as u64) << 8 | second as u64
            }
            0b10 => {
                let mut buf = [0u8; 4];
                self.reader.read_exact(&mut buf)?;
                u32::from_be_bytes(buf) as u64
            }
            0b11 => {
                let mut buf = [0u8; 8];
                self.read_exact(&mut buf)?;
                u64::from_be_bytes(buf)
            }
            _ => {
                return Err(io_invalid_data(format!("invalid length header: {}", first)));
            }
        };
        Ok(value)
    }

    fn read_u8(&mut self) -> io::Result<u8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.reader.read_exact(buf)
    }
    fn read_string(&mut self) -> io::Result<String> {
        let len = self.read_length()? as usize;
        println!("===== length is: {}", len);
        let mut buf = vec![0u8; len];
        self.read_exact(&mut buf)?;
        String::from_utf8(buf).map_err(|e| io_invalid_data(e))
    }

    fn read_meta(&mut self) -> io::Result<u8> {
        loop {
            println!("===== reading opcode");
            let op_code = self.read_u8()?;
            if op_code != OP_METADATA {
                return Ok(op_code);
            }
            println!("===== opcode pass");
            let key = self.read_string()?;
            println!("====== read key {}", key);
            let value = self.read_string()?;
            println!("===== read value {}", value);
            self.metadata.push((key, value));
        }
    }

    fn read_db(&mut self, op_code: u8) -> io::Result<()> {
        if op_code != OP_DB {
            return Err(io_invalid_data("invalid db_op_code"));
        }
        let _db_idx = self.read_length()?;
        let op_code = self.read_u8()?;
        if op_code != OP_DB_SIZE {
            return Err(io_invalid_data("invalid db_size_opcode"));
        }
        let _key_count = self.read_length()?;
        let _ttl_count = self.read_length()?;

        let mut prev_expiry: Option<SystemTime> = None;
        loop {
            match self.read_u8()? {
                OP_EXPIRY_MS => {
                    if prev_expiry.is_some() {
                        return Err(io_invalid_data("dangling expiry"));
                    }
                    let mut buf = [0u8; 8];
                    self.read_exact(&mut buf)?;
                    let ms = u64::from_le_bytes(buf);
                    let deadline = SystemTime::UNIX_EPOCH + Duration::from_millis(ms);
                    prev_expiry = Some(deadline);
                }
                OP_EXPIRY_SEC => {
                    if prev_expiry.is_some() {
                        return Err(io_invalid_data("dangling expiry"));
                    }
                    let mut buf = [0u8; 4];
                    self.read_exact(&mut buf)?;
                    let sec = u32::from_le_bytes(buf);
                    let deadline = SystemTime::UNIX_EPOCH + Duration::from_secs(sec as u64);
                    prev_expiry = Some(deadline)
                }
                OP_STRING_TYPE => {
                    let key = self.read_string()?;
                    let value = self.read_string()?;
                    self.redis.set(key, value, std::mem::take(&mut prev_expiry));
                }
                OP_EOF => {
                    if prev_expiry.is_some() {
                        return Err(io_invalid_data("dangling expiry"));
                    }
                    return Ok(());
                }
                _ => return Err(io_invalid_data("invalid opcode")),
            }
        }
    }
}
