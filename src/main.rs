use std::net::TcpListener;
use std::sync::Arc;

mod parser;

mod redis;

mod handler;

use crate::handler::{Client, RedisShared};

fn main() {
    println!("Logs from your program will appear here!");
    let redis = Arc::new(RedisShared::new());
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

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
