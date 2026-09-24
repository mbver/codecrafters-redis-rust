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

pub fn is_allowed_in_subscribed_mode(command: &[String]) -> bool {
    let Some(first) = command.first() else {
        return false;
    };
    if first.eq_ignore_ascii_case("SUBSCRIBE")
        || first.eq_ignore_ascii_case("UNSUBSCRIBE")
        || first.eq_ignore_ascii_case("PSUBSCRIBE")
        || first.eq_ignore_ascii_case("PUNSUBSCRIBE")
        || first.eq_ignore_ascii_case("PING")
        || first.eq_ignore_ascii_case("QUIT")
    {
        return true;
    }
    false
}

pub fn encode_slice(command: &[String]) -> Vec<u8> {
    let mut out = Vec::new();
    write!(&mut out, "*{}\r\n", command.len()).unwrap();
    for arg in command {
        write!(&mut out, "${}\r\n{}\r\n", arg.len(), arg).unwrap();
    }
    out
}
