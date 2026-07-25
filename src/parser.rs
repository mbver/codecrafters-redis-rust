use std::io::{Read, Write};
use std::net::TcpStream;

#[derive(Debug)]
enum ParseState {
    // waiting for "*<count>\r\n"
    ReadingArrayHeader,
    // waiting for "$<len>\r\n" for the next element
    ReadingBulkHeader {
        remaining: usize,
        items: Vec<Vec<u8>>,
    },
    // waiting for <len> bytes + trailing \r\n
    ReadingBulkBody {
        remaining: usize,
        items: Vec<Vec<u8>>,
        bulk_len: usize,
    },
}

pub struct Parser {
    state: ParseState,
    buf: Vec<u8>,
}

impl Parser {
    pub fn new() -> Self {
        Parser {
            state: ParseState::ReadingArrayHeader,
            buf: Vec::new(),
        }
    }

    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Advance the state machine as far as possible.
    /// Returns Some(command) each time a full array is completed.
    pub fn try_parse(&mut self) -> Result<Option<Vec<Vec<u8>>>, String> {
        loop {
            match &mut self.state {
                ParseState::ReadingArrayHeader => {
                    let Some(line_end) = find_crlf(&self.buf) else {
                        return Ok(None);
                    };
                    if self.buf[0] != b'*' {
                        return Err(format!("expected '*', got {}", self.buf[0] as char));
                    }
                    let count: usize = std::str::from_utf8(&self.buf[1..line_end])
                        .map_err(|_| "bad utf8")?
                        .parse()
                        .map_err(|_| "bad count")?;

                    self.buf.drain(0..line_end + 2); // consume "*N\r\n"
                    self.state = ParseState::ReadingBulkHeader {
                        remaining: count,
                        items: Vec::with_capacity(count),
                    };
                    // loop again immediately, don't wait for more bytes
                }

                ParseState::ReadingBulkHeader { remaining, items } => {
                    if *remaining == 0 {
                        // array complete
                        let items = items.clone();
                        self.state = ParseState::ReadingArrayHeader;
                        return Ok(Some(items));
                    }
                    let Some(line_end) = find_crlf(&self.buf) else {
                        return Ok(None);
                    };
                    if self.buf[0] != b'$' {
                        return Err(format!("expected '$', got {}", self.buf[0] as char));
                    }
                    let len: usize = std::str::from_utf8(&self.buf[1..line_end])
                        .map_err(|_| "bad utf8")?
                        .parse()
                        .map_err(|_| "bad bulk len")?;

                    self.buf.drain(0..line_end + 2); // consume "$N\r\n"
                    self.state = ParseState::ReadingBulkBody {
                        remaining: *remaining,
                        items: std::mem::take(items),
                        bulk_len: len,
                    };
                }

                ParseState::ReadingBulkBody {
                    remaining,
                    items,
                    bulk_len,
                } => {
                    let total_needed = *bulk_len + 2; // payload + \r\n
                    if self.buf.len() < total_needed {
                        return Ok(None); // wait for more bytes
                    }
                    if &self.buf[*bulk_len..total_needed] != b"\r\n" {
                        return Err("expected CRLF after bulk string".to_string());
                    }

                    let mut items = std::mem::take(items);
                    items.push(self.buf[0..*bulk_len].to_vec());
                    self.buf.drain(0..total_needed); // consume payload + \r\n

                    self.state = ParseState::ReadingBulkHeader {
                        remaining: *remaining - 1,
                        items,
                    };
                }
            }
        }
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}
