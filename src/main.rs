use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process;
use std::sync::Arc;

mod parser;

mod redis;

mod handler;

mod config;

mod rdb;

mod helper;

use crate::config::{Config, parse_args};
use crate::handler::{
    Handler, MasterState, RedisShared, Role, SlaveInfo, parse_bulk_len, readline_exact,
};
use crate::helper::{io_invalid_data, write_slice_resp};
use crate::rdb::RdbReader;
fn main() {
    println!("Logs from your program will appear here!");
    let config = parse_args();
    let redis = match load_db(&config) {
        Ok(redis) => redis,
        Err(_) => Arc::new(RedisShared::new()), // TODO: should handle the error?
    };

    let role = match &config.replica_of {
        Some((host, port)) => {
            start_replica(host, *port, config.port, redis.clone(), config.clone());
            Role::Slave(SlaveInfo { offset: 0 }) // TODO: slave info only applied in replication loop? it shouldn't be in main-loop? or replica shouldn't be in main-loop altogether?
        }
        None => Role::Master(Arc::new(MasterState::new())),
    };

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).unwrap();
    let mut replay_client = Handler::new(role.clone(), redis.clone(), config.clone());

    match replay_aof(&config, &mut replay_client) {
        Ok(_) => {}
        Err(e) => fatal(&format!("replay aof error {e}")),
    };

    let file = match create_aof(&config) {
        Ok(file) => file,
        Err(e) => fatal(&format!("create aof error: {e}")),
    };

    {
        let mut guard = redis.lock();
        guard.set_aof(file);
    }

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let mut client = Handler::new(role.clone(), redis.clone(), config.clone());
                std::thread::spawn(move || {
                    client.handle(stream);
                });
            }
            Err(e) => {
                println!("error:  {}", e);
            }
        }
    }
}

fn fatal(msg: &str) -> ! {
    eprintln!("{msg}");
    process::exit(1)
}

fn start_replica(host: &str, port: u16, my_port: u16, redis: Arc<RedisShared>, config: Config) {
    let Ok(Some(mut stream)) = ping(host, port) else {
        fatal("ping failed");
    };
    if send_replconf(&mut stream, my_port).is_err() {
        fatal("replconf failed");
    }
    if send_psync(&mut stream, None, -1).is_err() {
        fatal("psync failed");
    }
    let mut client = Handler::new(Role::Slave(SlaveInfo { offset: 0 }), redis, config);
    std::thread::spawn(move || {
        client.handle_replication(stream);
    });
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
    write_slice_resp(stream, &["REPLCONF", "listening-port", &port])?;
    let mut buf = [0; 512];
    let n = stream.read(&mut buf)?; // TODO: change to read_exact
    if &buf[..n] != b"+OK\r\n" {
        return Err(io_invalid_data("invalid replconf result"));
    }
    write_slice_resp(stream, &["REPLCONF", "capa", "psync2"])?;

    let n = stream.read(&mut buf)?;
    if &buf[..n] != b"+OK\r\n" {
        return Err(io_invalid_data("invalid replconf result"));
    }
    Ok(())
}

fn send_psync(stream: &mut TcpStream, _: Option<String>, _: i64) -> io::Result<()> {
    write_slice_resp(stream, &["PSYNC", "?", "-1"])?;
    let _ = readline_exact(stream)?;
    let line = readline_exact(stream)?;
    let data_len = parse_bulk_len(&line)?;
    let mut buf = vec![0u8; data_len];
    let _ = stream.read_exact(&mut buf)?; // TODO: the data isn't terminated with \r\n so we can't not use the command parser. find a way to use parser
    Ok(())
}

fn load_db(config: &Config) -> io::Result<Arc<RedisShared>> {
    let Some(dbfile) = &config.rdb_filename else {
        return Err(io_invalid_data("missing dbfile"));
    };
    let path = config.dir.join(dbfile);
    let file = File::open(path)?;
    let mut rdb_reader = RdbReader::new(file);
    rdb_reader.load()?;
    let db = rdb_reader.into_db();
    Ok(Arc::new(RedisShared::with_db(db)))
}

fn replay_aof(config: &Config, client: &mut Handler) -> io::Result<()> {
    if !config.append_only.eq_ignore_ascii_case("yes") {
        return Ok(());
    }
    let dir = config.dir.join(&config.append_dirname);
    if !dir.exists() {
        return Ok(());
    }
    let filename = &config.append_filename;
    let path = dir.join(format!("{}.manifest", filename));
    if !path.exists() {
        return Ok(());
    }
    let manifest = File::open(path)?;
    let reader = BufReader::new(manifest);

    for line in reader.lines() {
        let line = line?;
        let mut parts = line.split_whitespace();

        let _file = parts.next();
        let filename = parts.next();
        let Some(filename) = filename else {
            continue;
        };
        let _seq = parts.next();
        let _seq_value = parts.next();
        let _type = parts.next();
        let type_value = parts.next();

        let Some(type_value) = type_value else {
            continue;
        };
        if !type_value.eq("i") {
            continue;
        }
        let path = dir.join(filename);
        if !path.exists() {
            continue;
        }
        let file = File::open(path)?;
        client.handle_replay(Some(file))?;
    }
    Ok(())
}

fn create_aof(config: &Config) -> io::Result<Option<File>> {
    if !config.append_only.eq_ignore_ascii_case("yes") {
        return Ok(None);
    }
    let dir = config.dir.join(&config.append_dirname);
    std::fs::create_dir_all(&dir)?;
    let filename = &config.append_filename;
    let path = dir.join(format!("{}.1.incr.aof", filename));
    let file = OpenOptions::new().create(true).append(true).open(&path)?; // create the file before reading
    let path = dir.join(format!("{}.manifest", filename));
    let mut manifest = File::create(path)?;
    write!(manifest, "file {}.1.incr.aof seq 1 type i", filename)?;
    Ok(Some(file))
}
