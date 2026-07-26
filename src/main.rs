#![allow(unused_imports)]
use std::io::{self, Error, ErrorKind};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

mod parser;
use parser::Parser;

mod redis;
use redis::Redis;

use crate::redis::Value;

type SharedRedis = Arc<Mutex<Redis>>;

fn handle_client(mut stream: TcpStream, redis: SharedRedis) {
    let mut parser = Parser::new();
    let mut tmp = [0u8; 512];

    loop {
        while let Some(command) = parser.try_parse().unwrap_or_else(|e| {
            eprintln!("protocol error: {e}");
            std::process::exit(1); // or handle more gracefully
        }) {
            if let Err(e) = handle_command(&mut stream, command, redis.clone()) {
                eprintln!("io error: {:?}", e);
                return;
            }
        }

        let n = match stream.read(&mut tmp) {
            Ok(0) => return,
            Ok(n) => n,
            Err(e) => {
                eprintln!("read error: {e}");
                return;
            }
        };
        parser.feed(&tmp[..n]);
    }
}

fn handle_command(
    stream: &mut TcpStream,
    items: Vec<Vec<u8>>,
    redis: SharedRedis,
) -> io::Result<()> {
    let strs: Vec<String> = items
        .iter()
        .map(|b| String::from_utf8_lossy(b).to_string())
        .collect();
    match strs.as_slice() {
        [cmd, arg] if cmd.eq_ignore_ascii_case("ECHO") => {
            let resp = format!("${}\r\n{}\r\n", arg.len(), arg);
            stream.write_all(resp.as_bytes())
        }
        [cmd] if cmd.eq_ignore_ascii_case("PING") => stream.write_all(b"+PONG\r\n"),
        [cmd, key, value, args @ ..] if cmd.eq_ignore_ascii_case("SET") => {
            let deadline = match args {
                [] => None,
                [cmd, seconds] if cmd.eq_ignore_ascii_case("EX") => {
                    Some(Instant::now() + Duration::from_secs(seconds.parse().unwrap()))
                }
                [cmd, milliseconds] if cmd.eq_ignore_ascii_case("PX") => {
                    Some(Instant::now() + Duration::from_millis(milliseconds.parse().unwrap()))
                }
                _ => {
                    eprintln!("invalid argument: {:?}", args);
                    return Ok(());
                }
            };
            {
                let mut redis = redis.lock().unwrap();
                redis.set(key.to_string(), value.to_string(), deadline);
            }
            stream.write_all(b"+OK\r\n")
        }
        [cmd, key] if cmd.eq_ignore_ascii_case("GET") => {
            let res = {
                let mut redis = redis.lock().unwrap();
                redis.get_string(key).cloned()
            };

            match res {
                Some(s) => {
                    let v = format!("${}\r\n{}\r\n", s.len(), s.to_string());
                    stream.write_all(v.as_bytes())
                }
                None => stream.write_all(b"$-1\r\n"),
            }
        }
        [cmd, key, values @ ..] if cmd.eq_ignore_ascii_case("RPUSH") => {
            let res = {
                let mut redis = redis.lock().unwrap();
                redis.rpush(key, values)
            };
            match res {
                Ok(size) => {
                    write!(stream, ":{}\r\n", size)
                }
                Err(_) => stream.write_all(b"wrong type"),
            }
        }
        [cmd, key, start, end] if cmd.eq_ignore_ascii_case("LRANGE") => {
            let Ok(start) = start.parse::<isize>() else {
                return write!(stream, "invalid value of start: {start}");
            };
            let Ok(end) = end.parse::<isize>() else {
                return write!(stream, "invalid value of end: {end}");
            };
            let values = {
                let mut redis = redis.lock().unwrap();
                redis.lrange(key, start, end)
            };
            write!(stream, "*{}\r\n", values.len())?;
            for v in values {
                write!(stream, "${}\r\n", v.len())?;
                stream.write_all(v.as_bytes())?;
                stream.write_all(b"\r\n")?;
            }
            Ok(())
        }
        _ => stream.write_all(b"-ERR unknown command\r\n"),
    }
}

fn main() {
    println!("Logs from your program will appear here!");
    let redis = Arc::new(Mutex::new(Redis::new()));
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let redis = redis.clone();
                std::thread::spawn(move || {
                    handle_client(stream, redis);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
