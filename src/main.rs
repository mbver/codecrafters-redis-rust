use std::sync::Arc;
use std::{env, net::TcpListener};

mod parser;

mod redis;

mod handler;

use crate::handler::{Client, RedisShared};

fn main() {
    println!("Logs from your program will appear here!");
    let args: Vec<String> = env::args().collect();
    let port = match args.as_slice() {
        [_, flag, port] if flag.eq("--port") => match port.parse::<u16>() {
            Ok(port) => port,
            Err(e) => {
                eprintln!("invalid port: {e}");
                std::process::exit(1);
            }
        },
        _ => 6379,
    };
    let redis = Arc::new(RedisShared::new());
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let mut client = Client::new(redis.clone());
                std::thread::spawn(move || {
                    client.handle(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
