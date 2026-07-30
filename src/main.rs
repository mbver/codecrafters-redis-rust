use std::net::TcpListener;
use std::sync::Arc;

mod parser;

mod redis;

mod handler;

mod config;

use crate::config::parse_args;
use crate::handler::{Handler, RedisShared, Role};

fn main() {
    println!("Logs from your program will appear here!");

    let config = parse_args();

    let redis = Arc::new(RedisShared::new());
    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).unwrap();
    println!("======= listening on port: {}", config.port);
    let role = match config.replica_of {
        Some(_) => Role::Slave,
        None => Role::Master,
    };

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let mut client = Handler::new(role.clone(), redis.clone());
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
