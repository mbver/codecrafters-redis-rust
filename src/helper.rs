use std::{error::Error, io};

pub fn io_invalid_data<E>(e: E) -> io::Error
where
    E: Into<Box<dyn Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::InvalidData, e)
}
