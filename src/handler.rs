use std::io;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use std::{
    io::{Read, Write},
    net::TcpStream,
};

use crate::parser::Parser;
use crate::redis::Redis;

pub struct RedisShared {
    redis: Mutex<Redis>,
}

impl RedisShared {
    pub fn new() -> Self {
        Self {
            redis: Mutex::new(Redis::new()),
        }
    }
    fn lock(&self) -> MutexGuard<'_, Redis> {
        self.redis.lock().unwrap()
    }
}

type SharedRedis = Arc<RedisShared>;

pub fn handle_client(mut stream: TcpStream, redis: SharedRedis) {
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

fn handle_echo(stream: &mut TcpStream, arg: &String) -> io::Result<()> {
    let resp = format!("${}\r\n{}\r\n", arg.len(), arg);
    stream.write_all(resp.as_bytes())
}

fn handle_set(
    stream: &mut TcpStream,
    key: &String,
    value: &String,
    args: &[String],
    redis: SharedRedis,
) -> io::Result<()> {
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
        let mut redis = redis.lock();
        redis.set(key.to_string(), value.to_string(), deadline);
    }
    stream.write_all(b"+OK\r\n")
}

fn handle_get(stream: &mut TcpStream, key: &String, redis: SharedRedis) -> io::Result<()> {
    let res = {
        let mut redis = redis.lock();
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

fn handle_rpush(
    stream: &mut TcpStream,
    key: &String,
    values: &[String],
    redis: SharedRedis,
) -> io::Result<()> {
    let output = {
        let mut redis = redis.lock();
        let res = redis.rpush(key, values);
        match res {
            Ok(size) => {
                redis.notify_one(key);
                format!(":{}\r\n", size)
            }
            Err(_) => "wrong type".to_string(),
        }
    };
    stream.write_all(output.as_bytes())
}

fn handle_lpush(
    stream: &mut TcpStream,
    key: &String,
    values: &[String],
    redis: SharedRedis,
) -> io::Result<()> {
    let output = {
        let mut redis = redis.lock();
        let res = redis.lpush(key, values);
        match res {
            Ok(size) => {
                redis.notify_one(key);
                format!(":{}\r\n", size)
            }
            Err(_) => "wrong type".to_string(),
        }
    };
    stream.write_all(output.as_bytes())
}

fn handle_lrange(
    stream: &mut TcpStream,
    key: &String,
    start: &String,
    end: &String,
    redis: SharedRedis,
) -> io::Result<()> {
    let Ok(start) = start.parse::<isize>() else {
        return write!(stream, "invalid value of start: {start}");
    };
    let Ok(end) = end.parse::<isize>() else {
        return write!(stream, "invalid value of end: {end}");
    };
    let values = {
        let mut redis = redis.lock();
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

fn handle_llen(stream: &mut TcpStream, key: &String, redis: SharedRedis) -> io::Result<()> {
    let size = {
        let mut redis = redis.lock();
        redis.llen(key)
    };
    write!(stream, ":{}\r\n", size)
}

fn handle_lpop(
    stream: &mut TcpStream,
    key: &String,
    arg: &[String],
    redis: SharedRedis,
) -> io::Result<()> {
    if arg.len() > 1 {
        eprintln!("wrong argument: {:?}", arg);
        return Ok(());
    }
    let mut is_array = false;
    let num = match arg.first() {
        None => 1,
        Some(v) => match v.parse::<usize>() {
            Ok(n) => {
                is_array = true;
                n
            }
            Err(_) => {
                eprintln!("wrong argument {v}");
                return Ok(());
            }
        },
    };

    let res = {
        let mut redis = redis.lock();
        redis.lpop(key, num)
    };
    match res {
        Ok(None) => stream.write_all(b"$-1\r\n"),
        Ok(Some(values)) => {
            if !is_array {
                return write!(stream, "${}\r\n{}\r\n", values[0].len(), values[0]);
            }
            write!(stream, "*{}\r\n", values.len())?;
            for v in values {
                write!(stream, "${}\r\n", v.len())?;
                stream.write_all(v.as_bytes())?;
                stream.write_all(b"\r\n")?;
            }
            Ok(())
        }
        Err(_) => stream.write_all(b"wrong type"),
    }
}

fn handle_blpop(
    stream: &mut TcpStream,
    key: &String,
    arg: &String,
    redis: SharedRedis,
) -> io::Result<()> {
    let Ok(secs) = arg.parse::<f64>() else {
        eprintln!("wrong timeout argument: {arg}");
        return Ok(());
    };
    let timeout = Duration::from_secs_f64(secs);
    let mut db = redis.lock();
    loop {
        match db.lpop(key, 1) {
            Ok(Some(values)) if !values.is_empty() => {
                drop(db);
                write!(stream, "*{}\r\n", 1 + values.len())?;
                write!(stream, "${}\r\n{}\r\n", key.len(), key)?;
                for v in values {
                    write!(stream, "${}\r\n{}\r\n", v.len(), v)?;
                }
                return Ok(());
            }
            Ok(_) => {
                let waiter = db.add_waiter(key);
                if timeout.is_zero() {
                    db = waiter.wait(db).unwrap();
                } else {
                    let (guard, timeout_result) = waiter.wait_timeout(db, timeout).unwrap();
                    db = guard;
                    if timeout_result.timed_out() {
                        return write!(
                            stream,
                            "*3\r\n$5\r\nBLPOP\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                            key.len(),
                            key,
                            arg.len(),
                            arg
                        );
                    }
                }
                continue;
            }
            Err(_) => return stream.write_all(b"wrong type"),
        }
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
        [cmd, arg] if cmd.eq_ignore_ascii_case("ECHO") => handle_echo(stream, arg),

        [cmd] if cmd.eq_ignore_ascii_case("PING") => stream.write_all(b"+PONG\r\n"),

        [cmd, key, value, args @ ..] if cmd.eq_ignore_ascii_case("SET") => {
            handle_set(stream, key, value, args, redis)
        }
        [cmd, key] if cmd.eq_ignore_ascii_case("GET") => handle_get(stream, key, redis),
        [cmd, key, values @ ..] if cmd.eq_ignore_ascii_case("RPUSH") => {
            handle_rpush(stream, key, values, redis)
        }
        [cmd, key, values @ ..] if cmd.eq_ignore_ascii_case("LPUSH") => {
            handle_lpush(stream, key, values, redis)
        }
        [cmd, key, start, end] if cmd.eq_ignore_ascii_case("LRANGE") => {
            handle_lrange(stream, key, start, end, redis)
        }
        [cmd, key] if cmd.eq_ignore_ascii_case("LLEN") => handle_llen(stream, key, redis),
        [cmd, key, arg @ ..] if cmd.eq_ignore_ascii_case("LPOP") => {
            handle_lpop(stream, key, arg, redis)
        }
        [cmd, key, arg] if cmd.eq_ignore_ascii_case("BLPOP") => {
            handle_blpop(stream, key, arg, redis)
        }
        _ => stream.write_all(b"-ERR unknown command\r\n"),
    }
}
