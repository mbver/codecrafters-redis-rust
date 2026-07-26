#![allow(unused_imports)]
use std::io::{Error, ErrorKind};
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
            handle_command(&mut stream, command, redis.clone());
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

fn handle_command(stream: &mut TcpStream, items: Vec<Vec<u8>>, redis: SharedRedis) {
    let strs: Vec<String> = items
        .iter()
        .map(|b| String::from_utf8_lossy(b).to_string())
        .collect();
    match strs.as_slice() {
        [cmd, arg] if cmd.eq_ignore_ascii_case("ECHO") => {
            let resp = format!("${}\r\n{}\r\n", arg.len(), arg);
            stream.write_all(resp.as_bytes()).unwrap();
        }
        [cmd] if cmd.eq_ignore_ascii_case("PING") => {
            stream.write_all(b"+PONG\r\n").unwrap();
        }
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
                    return;
                }
            };
            {
                let mut redis = redis.lock().unwrap();
                redis.set(key.to_string(), value.to_string(), deadline);
            }
            stream.write_all(b"+OK\r\n").unwrap();
        }
        [cmd, key] if cmd.eq_ignore_ascii_case("GET") => {
            let res = {
                let mut redis = redis.lock().unwrap();
                redis.get(key).cloned()
            };

            match res {
                Some(s) => {
                    let v = format!("${}\r\n{}\r\n", s.len(), s.to_string());
                    stream.write_all(v.as_bytes()).unwrap();
                }
                None => {
                    stream.write_all(b"$-1\r\n").unwrap();
                }
            };
        }
        [cmd, key, values @ ..] if cmd.eq_ignore_ascii_case("RPUSH") => {
            let mut redis = redis.lock().unwrap();
            match redis.rpush(key, values) {
                Ok(size) => {
                    write!(stream, ":{}\r\n", size).unwrap();
                }
                Err(_) => {
                    write!(stream, "wrong type").unwrap();
                }
            }
        }
        _ => {
            stream.write_all(b"-ERR unknown command\r\n").unwrap();
        }
    }
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    let redis = Arc::new(Mutex::new(Redis::new()));
    // Uncomment the code below to pass the first stage
    //
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
