#![allow(unused_imports)]
use std::io::{Error, ErrorKind};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

enum Command {
    Ping,
    Echo(String),
}

fn parse_command(input: &str) -> Result<Command, Error> {
    let parts: Vec<&str> = input.split(" ").collect();
    match parts.as_slice() {
        ["PING"] => Ok(Command::Ping),
        ["ECHO", args @ ..] => Ok(Command::Echo(args.join(" "))),
        _ => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("invalid input {}", input),
        )),
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut buf = [0; 512];

    loop {
        let bytes_read = stream.read(&mut buf).unwrap();
        if bytes_read == 0 {
            break;
        }
        let input = std::str::from_utf8(&buf[..bytes_read]).unwrap();
        let cmd = parse_command(input).unwrap();
        match cmd {
            Command::Ping => stream.write_all(b"+PONG\r\n").unwrap(),
            Command::Echo(s) => {
                let s = format!("{s}\r\n");
                stream.write_all(s.as_bytes()).unwrap();
            }
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
