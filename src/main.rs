use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

mod parser;

mod redis;

mod handler;

mod config;
use crate::config::parse_args;
use crate::handler::{Handler, MasterState, RedisShared, Role};
fn main() {
    println!("Logs from your program will appear here!");

    let config = parse_args();

    let redis = Arc::new(RedisShared::new());
    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).unwrap();
    let role = match config.replica_of {
        Some(_) => {
            let Some((host, port)) = &config.replica_of else {
                eprintln!("missing host port");
                std::process::exit(1);
            };
            let Ok(Some(mut stream)) = ping(host, *port) else {
                eprintln!("ping failed");
                std::process::exit(1);
            };
            if send_replconf(&mut stream, config.port).is_err() {
                eprintln!("replconf failed");
                std::process::exit(1);
            }
            if send_psync(&mut stream, None, -1).is_err() {
                eprintln!("psync failed");
                std::process::exit(1);
            }
            // Do we really need Slave role?
            // it is repetitive here
            let mut client = Handler::new(Role::Slave, redis.clone());
            std::thread::spawn(move || {
                client.handle_replication(stream);
            });
            Role::Slave
        }
        None => Role::Master(Arc::new(MasterState::new())),
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

fn ping(host: &str, port: u16) -> io::Result<Option<TcpStream>> {
    let mut stream = TcpStream::connect((host, port))?;
    stream.write_all(b"*1\r\n$4\r\nPING\r\n")?;
    let mut buf = [0; 512];
    let n = stream.read(&mut buf)?;
    if &buf[..n] != b"+PONG\r\n" {
        return Ok(None);
    }
    Ok(Some(stream))
}

fn send_replconf(stream: &mut TcpStream, port: u16) -> io::Result<()> {
    let port = port.to_string();
    write!(
        stream,
        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
        port.len(),
        port
    )?;
    let mut buf = [0; 512];
    let n = stream.read(&mut buf)?; // TODO: change to read_exact
    if &buf[..n] != b"+OK\r\n" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid replconf result",
        ));
    }

    write!(
        stream,
        "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
    )?;

    let n = stream.read(&mut buf)?;
    if &buf[..n] != b"+OK\r\n" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid replconf result",
        ));
    }
    Ok(())
}

fn send_psync(stream: &mut TcpStream, _: Option<String>, _: i64) -> io::Result<()> {
    write!(stream, "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")?;
    let mut buf = [0; 512];
    let _ = stream.read(&mut buf)?;
    // if &buf[..n] != b"+FULLRESYNC <REPL_ID> 0\r\n" {
    //     return Err(io::Error::new(
    //         io::ErrorKind::InvalidData,
    //         "invalid replconf result",
    //     ));
    // }
    Ok(())
}
