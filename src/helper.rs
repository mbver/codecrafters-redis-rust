use std::io::Write;
use std::{error::Error, io, net::TcpStream};
pub fn io_invalid_data<E>(e: E) -> io::Error
where
    E: Into<Box<dyn Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::InvalidData, e)
}

pub fn write_slice_resp<T: AsRef<[u8]>>(stream: &mut TcpStream, items: &[T]) -> io::Result<()> {
    write!(stream, "*{}\r\n", items.len())?;
    for item in items {
        let item = item.as_ref();
        write!(stream, "${}\r\n", item.len())?;
        stream.write_all(item)?;
        stream.write_all(b"\r\n")?;
    }
    Ok(())
}
