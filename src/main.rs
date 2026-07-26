use std::net::TcpListener;
use std::sync::{Arc, Mutex};

mod parser;

mod redis;
use redis::Redis;

mod handler;
use handler::handle_client;

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
