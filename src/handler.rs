use std::io;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use std::{
    io::{Read, Write},
    net::TcpStream,
};

use crate::parser::Parser;
use crate::redis::{Redis, StreamEntry, Value};

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
            Err(e) => format!("{e}"),
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
            Err(e) => format!("{e}"),
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
        Err(e) => write!(stream, "{e}"),
    }
}

// TODO: consolidate the L/R semantics
fn handle_blpop(
    stream: &mut TcpStream,
    key: &String,
    arg: &String,
    redis: SharedRedis,
) -> io::Result<()> {
    let Ok(secs) = arg.parse::<f64>() else {
        return write!(stream, "-ERR invalid timeout: {arg}\r\n");
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
            Ok(res) => {
                let waiter = db.add_waiter(key);
                if timeout.is_zero() {
                    db = waiter.wait(db).unwrap();
                } else {
                    let (guard, timeout_result) = waiter.wait_timeout(db, timeout).unwrap();
                    db = guard;
                    if timeout_result.timed_out() {
                        if res.is_none() {
                            return stream.write_all(b"*-1\r\n");
                        }
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
            Err(e) => return write!(stream, "{e}"),
        }
    }
}

fn handle_type(stream: &mut TcpStream, key: &str, redis: SharedRedis) -> io::Result<()> {
    let t = {
        let mut redis = redis.lock();
        match redis.get_value(key) {
            Some(Value::String(_)) => "string",
            Some(Value::List(_)) => "list",
            Some(Value::Stream(_)) => "stream",
            None => "none",
        }
    };
    write!(stream, "+{t}\r\n")
}

fn is_at_least_two_and_even(len: usize) -> bool {
    len >= 2 && len.is_multiple_of(2)
}

fn handle_xadd(
    stream: &mut TcpStream,
    key: &str,
    id: &str,
    args: &[String],
    redis: SharedRedis,
) -> io::Result<()> {
    if !is_at_least_two_and_even(args.len()) {
        return write!(stream, "-ERR: wrong number of arguments: {}", args.len());
    }
    let output = {
        let mut redis = redis.lock();
        match redis.x_add(key, id, args) {
            Ok(id) => {
                println!("========= gonna notify");
                redis.notify_one(key);
                format!("${}\r\n{}\r\n", id.len(), id)
            }
            Err(e) => format!("-ERR {e}\r\n"),
        }
    };
    stream.write_all(output.as_bytes())
}

fn handle_xrange(
    stream: &mut TcpStream,
    key: &str,
    start_id: &str,
    end_id: &str,
    redis: SharedRedis,
) -> io::Result<()> {
    let res = {
        let mut redis = redis.lock();
        redis.x_range(key, start_id, end_id)
    };
    match res {
        Ok(entries) => {
            write!(stream, "*{}\r\n", entries.len())?;
            for e in entries {
                e.write_bulk(stream)?
            }
            Ok(())
        }
        Err(e) => {
            write!(stream, "-ERR {e}\r\n")
        }
    }
}

fn handle_xread(stream: &mut TcpStream, args: &[String], redis: SharedRedis) -> io::Result<()> {
    let len = args.len();
    if !is_at_least_two_and_even(len) {
        return write!(stream, "-ERR wrong number of arguments: {}", len);
    }
    let n = len / 2;
    let pairs: Vec<(&String, &String)> = args[..n].iter().zip(args[n..].iter()).collect();
    let res = {
        let mut redis = redis.lock();
        redis.x_read(&pairs)
    };
    match res {
        Ok(values) => write_x_read(stream, &values),
        Err(e) => write!(stream, "-ERR {e}"),
    }
}

fn write_x_read(stream: &mut TcpStream, values: &[(String, Vec<StreamEntry>)]) -> io::Result<()> {
    write!(stream, "*{}\r\n", values.len())?;
    for v in values {
        write!(stream, "*2\r\n")?;
        write!(stream, "${}\r\n{}\r\n", v.0.len(), v.0)?;
        write!(stream, "*{}\r\n", v.1.len())?;
        for entry in &v.1 {
            entry.write_bulk(stream)?;
        }
    }
    Ok(())
}

fn handle_block_xread(
    stream: &mut TcpStream,
    ms: &str,
    key: &String,
    id: &String,
    redis: SharedRedis,
) -> io::Result<()> {
    let Ok(ms) = ms.parse::<u64>() else {
        return write!(stream, "-ERR invalid timeout: {ms}\r\n");
    };
    let timeout = Duration::from_millis(ms);
    let mut db = redis.lock();
    loop {
        match db.x_read(&[(key, id)]) {
            Ok(values) if !values[0].1.is_empty() => {
                println!("============ write x read");
                return write_x_read(stream, &values);
            }
            Ok(_) => {
                println!("================ add waiter");
                let waiter = db.add_waiter(key);
                if timeout.is_zero() {
                    db = waiter.wait(db).unwrap();
                } else {
                    let (guard, timeout_result) = waiter.wait_timeout(db, timeout).unwrap();
                    db = guard;
                    if timeout_result.timed_out() {
                        println!("============ receive error");
                        return stream.write_all(b"*-1\r\n");
                    }
                }
                continue;
            }
            Err(e) => {
                println!("============ receive error");
                return write!(stream, "-ERR {e}\r\n");
            }
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
        [cmd, key] if cmd.eq_ignore_ascii_case("TYPE") => handle_type(stream, key, redis),
        [cmd, key, id, args @ ..] if cmd.eq_ignore_ascii_case("XADD") => {
            handle_xadd(stream, key, id, args, redis)
        }
        [cmd, key, start_id, end_id] if cmd.eq_ignore_ascii_case("XRANGE") => {
            handle_xrange(stream, key, start_id, end_id, redis)
        }
        [cmd, subcmd, args @ ..]
            if cmd.eq_ignore_ascii_case("XREAD") && subcmd.eq_ignore_ascii_case("STREAMS") =>
        {
            handle_xread(stream, args, redis)
        }
        [cmd, option, ms, subcmd, key, id]
            if cmd.eq_ignore_ascii_case("XREAD")
                && option.eq_ignore_ascii_case("BLOCK")
                && subcmd == "STREAMS" =>
        {
            handle_block_xread(stream, key, id, ms, redis)
        }
        _ => stream.write_all(b"-ERR unknown command\r\n"),
    }
}
