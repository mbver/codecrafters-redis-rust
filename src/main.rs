#![allow(unused_imports)]
use std::io::{Error, ErrorKind};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

mod parser;
use parser::Parser;

fn handle_client(mut stream: TcpStream) {
    let mut parser = Parser::new();
    let mut tmp = [0u8; 512];

    loop {
        while let Some(command) = parser.try_parse().unwrap_or_else(|e| {
            eprintln!("protocol error: {e}");
            std::process::exit(1); // or handle more gracefully
        }) {
            handle_command(&mut stream, command);
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

fn handle_command(stream: &mut TcpStream, items: Vec<Vec<u8>>) {
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
        _ => {
            stream.write_all(b"-ERR unknown command\r\n").unwrap();
        }
    }
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                std::thread::spawn(move || {
                    handle_client(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
