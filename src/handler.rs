use std::collections::HashMap;
use std::fmt::Display;
use std::io::{self, Error};
use std::ops::Add;
use std::str;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use std::{
    io::{Read, Write},
    net::TcpStream,
};

use std::fmt::Write as FmtWrite;

use crate::fatal;
use crate::parser::{Command, Parser};
use crate::redis::{EntryId, Redis, RedisError, StreamEntry, Value};

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

struct Transaction {
    commands: Vec<Vec<String>>,
}

impl Transaction {
    fn new() -> Self {
        Self {
            commands: Vec::new(),
        }
    }
}

enum ReplicationMessage {
    AddReplica(TcpStream),
    Command(Vec<u8>),
    Wait {
        num_replicas: usize,
        timeout: Duration,
        response: Sender<usize>,
    },
}

pub struct MasterState {
    info: MasterInfo,
    replication_tx: Sender<ReplicationMessage>,
}

#[derive(Clone)]
pub struct SlaveInfo {
    pub offset: usize,
}

impl MasterState {
    pub fn new() -> Self {
        let (tx, rx) = channel();
        std::thread::spawn({
            move || {
                replication_loop(rx);
            }
        });
        Self {
            info: MasterInfo::new(),
            replication_tx: tx,
        }
    }
}

// This signature seems not handle the error well. we seems violate throw early and catch late principle
fn replication_loop(rx: Receiver<ReplicationMessage>) {
    let mut replicas = Vec::<TcpStream>::new();
    let mut current_offset: usize = 0;
    while let Ok(msg) = rx.recv() {
        match msg {
            ReplicationMessage::AddReplica(stream) => {
                if let Err(e) = stream.set_read_timeout(Some(Duration::from_millis(100))) {
                    eprintln!("io error: {e}");
                    continue;
                };
                replicas.push(stream);
            }
            ReplicationMessage::Command(data) => {
                // TODO: a slow replica can block the whole loop and sender side is blocked
                // each replica should have its own dedicated thread and queue? and how to handle the slow replica
                // when the queue is full? ditch them?
                current_offset += data.len();
                replicas.retain_mut(|stream| stream.write_all(&data).is_ok());
            }
            ReplicationMessage::Wait {
                num_replicas,
                timeout,
                response,
            } => {
                let cmd = b"*3\r\n$8\r\nreplconf\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
                let deadline = Instant::now().add(timeout);
                let mut replica_count; // number of replicas caught up
                loop {
                    replica_count = 0;
                    replicas.iter_mut().for_each(|stream| {
                        println!(
                            "======== gonna send getack command, peer: {}",
                            stream.peer_addr().unwrap()
                        );
                        if stream.write_all(cmd).is_err() {
                            println!(
                                "========== stream write_all failed, peer: {}",
                                stream.peer_addr().unwrap()
                            );
                            return;
                        };
                        let Ok(offset) = read_ack_resp(stream) else {
                            println!(
                                "=========== read_ack_resp failed, peer: {}",
                                stream.peer_addr().unwrap()
                            );
                            return;
                        };
                        println!("====== offset: {}, current: {}", offset, current_offset);
                        if offset == current_offset {
                            replica_count += 1;
                        }
                        println!("======== replica_count now: {}", replica_count);
                    });
                    println!(
                        "========replicas: {}, replica_count {} after retain_mut",
                        replicas.len(),
                        replica_count
                    );
                    current_offset += cmd.len();
                    if replica_count >= num_replicas || Instant::now() > deadline {
                        break;
                    }
                }
                let _ = response.send(replica_count);
            }
        }
    }
}

fn read_ack_resp(stream: &mut TcpStream) -> io::Result<usize> {
    let header = readline_exact(stream)?;
    println!("====== header read {}", String::from_utf8_lossy(&header));
    if header != b"*3" {
        return Err(Error::new(io::ErrorKind::InvalidData, "invalid header"));
    }
    println!("========= header pass");
    let len = readline_exact(stream)?;
    if len != b"$8" {
        return Err(Error::new(io::ErrorKind::InvalidData, "invalid header"));
    }
    println!("======== len pass");
    let mut buf = vec![0u8; 10];
    stream.read_exact(&mut buf)?;
    if buf != b"REPLCONF\r\n" {
        return Err(Error::new(io::ErrorKind::InvalidData, "invalid command"));
    }
    println!("======== replconf pass");
    let len = readline_exact(stream)?;
    if len != b"$3" {
        return Err(Error::new(io::ErrorKind::InvalidData, "invalid header"));
    }
    println!("======== 5");

    let mut buf = vec![0u8; 5];
    stream.read_exact(&mut buf)?;
    if buf != b"ACK\r\n" {
        return Err(Error::new(io::ErrorKind::InvalidData, "invalid command"));
    }
    println!("======== 6");
    let len = readline_exact(stream)?;
    let len = parse_bulk_len(&len)?;
    let mut buf = vec![0u8; len + 2];
    stream.read_exact(&mut buf)?;
    if &buf[len..] != b"\r\n" {
        return Err(Error::new(io::ErrorKind::InvalidData, "crlf missing"));
    }
    println!("======== 7");
    let s = std::str::from_utf8(&buf[..len])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf8"))?;
    s.parse::<usize>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid length"))
}

pub fn readline_exact(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut line: Vec<u8> = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        if let Err(e) = stream.read_exact(&mut byte) {
            println!("============ e reading 1 byte: {}", e);
            return Err(e);
        };
        if byte[0] == b'\n' {
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return Ok(line);
        }
        println!("=========== read: {}", byte[0] as char);
        line.push(byte[0])
    }
}

pub fn parse_bulk_len(line: &[u8]) -> io::Result<usize> {
    let s = std::str::from_utf8(line)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf8"))?;
    println!("====== parse bulk length: 1, {}", s);
    let s = s
        .strip_prefix('$')
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing $"))?;
    println!("====== parse bulk length: 2");
    s.parse::<usize>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid length"))
}

#[derive(Clone)]
pub enum Role {
    Master(Arc<MasterState>),
    Slave(SlaveInfo),
}

impl Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Master(_) => write!(f, "master"),
            Role::Slave(_) => write!(f, "slave"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationId([u8; 40]);

impl Display for ReplicationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            f.write_char(byte as char)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct InvalidReplId;

impl ReplicationId {
    fn new(s: &str) -> Result<Self, InvalidReplId> {
        if s.len() != 40 || !s.chars().all(|ch| ch.is_alphanumeric()) {
            return Err(InvalidReplId);
        }
        let mut bytes = [0u8; 40];
        bytes.copy_from_slice(s.as_bytes());
        Ok(Self(bytes))
    }
}

struct MasterInfo {
    master_replid: ReplicationId,
    offset: u64,
}

impl MasterInfo {
    fn new() -> Self {
        Self {
            master_replid: ReplicationId::new("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb").unwrap(),
            offset: 0,
        }
    }
}

pub struct Handler {
    role: Role,
    redis: SharedRedis,
    transaction: Option<Transaction>,
    watch_list: HashMap<String, u64>,
}

fn is_write_command(command: &[String]) -> bool {
    let Some(cmd) = command.first() else {
        return false;
    };
    cmd.eq_ignore_ascii_case("SET")
        || cmd.eq_ignore_ascii_case("INCR")
        || cmd.eq_ignore_ascii_case("LPUSH")
        || cmd.eq_ignore_ascii_case("RPUSH")
}

fn encode_command(command: &[String]) -> Vec<u8> {
    let mut out = Vec::new();
    write!(&mut out, "*{}\r\n", command.len()).unwrap();
    for arg in command {
        write!(&mut out, "${}\r\n{}\r\n", arg.len(), arg).unwrap();
    }
    out
}

enum Next {
    Continue(TcpStream),
    Done,
}

fn read_loop<F>(mut stream: TcpStream, mut command_handler: F)
where
    F: FnMut(TcpStream, Command) -> Next, // Ok(true): keep going
{
    let mut parser = Parser::new();
    let mut tmp = [0u8; 512];

    loop {
        while let Some(command) = parser.try_parse().unwrap_or_else(|e| {
            eprintln!("protocol error: {e}");
            std::process::exit(1); // or handle more gracefully
        }) {
            match command_handler(stream, command) {
                Next::Continue(s) => stream = s,
                Next::Done => return,
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

impl Handler {
    pub fn new(role: Role, redis: SharedRedis) -> Self {
        Self {
            role,
            redis: redis,
            transaction: None,
            watch_list: HashMap::new(),
        }
    }
    pub fn handle(&mut self, stream: TcpStream) {
        read_loop(stream, |mut stream, command| {
            let command: Vec<String> = command
                .items
                .iter()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .collect();
            if let Some(cmd) = command.first()
                && cmd.eq_ignore_ascii_case("PSYNC")
            {
                if let Err(e) = self.handle_psync(stream) {
                    eprintln!("psync io error: {:?}", e);
                }
                return Next::Done;
            }
            let is_write = is_write_command(&command); // USE OPTION for encoded and eliminate is_write
            let encoded = encode_command(&command);
            if let Err(e) = self.handle_command(&mut stream, command) {
                eprintln!("io error: {:?}", e);
                return Next::Done;
            }
            if is_write {
                if let Role::Master(master) = &mut self.role {
                    let tx = master.replication_tx.clone();
                    let _ = tx.send(ReplicationMessage::Command(encoded));
                }
            }
            Next::Continue(stream)
        });
    }

    pub fn handle_replication(&mut self, stream: TcpStream) {
        read_loop(stream, |mut stream, command| {
            self.handle_repl_command(command, &mut stream);
            Next::Continue(stream)
        });
    }

    fn handle_repl_command(&mut self, command: Command, stream: &mut TcpStream) {
        let num_bytes = command.num_bytes;
        let command: Vec<String> = command
            .items
            .iter()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .collect();
        match command.as_slice() {
            [cmd, key, value, args @ ..] if cmd.eq_ignore_ascii_case("SET") => {
                let _ = self.handle_set_internal(key, value, args);
            }
            [cmd, key, values @ ..] if cmd.eq_ignore_ascii_case("RPUSH") => {
                let _ = self.handle_rpush_internal(key, values);
            }
            [cmd, key, values @ ..] if cmd.eq_ignore_ascii_case("LPUSH") => {
                let _ = self.handle_lpush_internal(key, values);
            }
            [cmd, key, arg @ ..] if cmd.eq_ignore_ascii_case("LPOP") => {
                let _ = self.handle_lpop_internal(key, arg);
            }
            [cmd, key] if cmd.eq_ignore_ascii_case("INCR") => {
                let _ = self.handle_incr_internal(key);
            }
            [cmd, subcmd, arg]
                if cmd.eq_ignore_ascii_case("replconf")
                    && subcmd.eq_ignore_ascii_case("getack")
                    && arg == "*" =>
            {
                let Role::Slave(SlaveInfo { offset }) = self.role else {
                    fatal("handle_repl_command is called in non-slave node");
                };
                let offset = offset.to_string();
                if let Err(e) = write!(
                    stream,
                    "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                    offset.len(),
                    offset
                ) {
                    eprintln!("io error: {e}");
                }
            }
            [cmd] if cmd.eq_ignore_ascii_case("PING") => {
                // do nothing for non-write command
            }

            _ => {
                unreachable!()
            }
        }
        let Role::Slave(SlaveInfo { offset }) = &mut self.role else {
            fatal("handle_repl_command is called in non-slave node");
        };
        *offset += num_bytes;
    }

    fn handle_command(&mut self, stream: &mut TcpStream, command: Vec<String>) -> io::Result<()> {
        match command.as_slice() {
            [cmd] if cmd.eq_ignore_ascii_case("EXEC") => return self.handle_exec(stream),
            [cmd] if cmd.eq_ignore_ascii_case("DISCARD") => return self.handle_discard(stream),
            [cmd, keys @ ..] if cmd.eq_ignore_ascii_case("WATCH") => {
                return self.handle_watch(stream, keys);
            }
            [cmd] if cmd.eq_ignore_ascii_case("UNWATCH") => return self.handle_unwatch(stream),
            _ => {}
        }
        if let Some(tx) = self.transaction.as_mut() {
            tx.commands.push(command);
            return stream.write_all(b"+QUEUED\r\n");
        }
        match command.as_slice() {
            [cmd, arg] if cmd.eq_ignore_ascii_case("ECHO") => self.handle_echo(stream, arg),

            [cmd] if cmd.eq_ignore_ascii_case("PING") => stream.write_all(b"+PONG\r\n"),

            [cmd, key, value, args @ ..] if cmd.eq_ignore_ascii_case("SET") => {
                self.handle_set(stream, key, value, args)
            }
            [cmd, key] if cmd.eq_ignore_ascii_case("GET") => self.handle_get(stream, key),
            [cmd, key, values @ ..] if cmd.eq_ignore_ascii_case("RPUSH") => {
                self.handle_rpush(stream, key, values)
            }
            [cmd, key, values @ ..] if cmd.eq_ignore_ascii_case("LPUSH") => {
                self.handle_lpush(stream, key, values)
            }
            [cmd, key, start, end] if cmd.eq_ignore_ascii_case("LRANGE") => {
                self.handle_lrange(stream, key, start, end)
            }
            [cmd, key] if cmd.eq_ignore_ascii_case("LLEN") => self.handle_llen(stream, key),
            [cmd, key, arg @ ..] if cmd.eq_ignore_ascii_case("LPOP") => {
                self.handle_lpop(stream, key, arg)
            }
            [cmd, key, arg] if cmd.eq_ignore_ascii_case("BLPOP") => {
                self.handle_blpop(stream, key, arg)
            }
            [cmd, key] if cmd.eq_ignore_ascii_case("TYPE") => self.handle_type(stream, key),
            [cmd, key, id, args @ ..] if cmd.eq_ignore_ascii_case("XADD") => {
                self.handle_xadd(stream, key, id, args)
            }
            [cmd, key, start_id, end_id] if cmd.eq_ignore_ascii_case("XRANGE") => {
                self.handle_xrange(stream, key, start_id, end_id)
            }
            [cmd, subcmd, args @ ..]
                if cmd.eq_ignore_ascii_case("XREAD") && subcmd.eq_ignore_ascii_case("STREAMS") =>
            {
                self.handle_xread(stream, args)
            }
            [cmd, option, ms, subcmd, key, id]
                if cmd.eq_ignore_ascii_case("XREAD")
                    && option.eq_ignore_ascii_case("BLOCK")
                    && subcmd.eq_ignore_ascii_case("STREAMS") =>
            {
                self.handle_block_xread(stream, ms, key, id)
            }
            [cmd, key] if cmd.eq_ignore_ascii_case("INCR") => self.handle_incr(stream, key),
            [cmd] if cmd.eq_ignore_ascii_case("MULTI") => self.handle_multi(stream),
            [cmd, subcmd] if cmd.eq_ignore_ascii_case("INFO") => self.handle_info(stream, subcmd),
            [cmd, ..] if cmd.eq_ignore_ascii_case("REPLCONF") => self.handle_replconf(stream),
            [cmd, num_replicas, timeout] if cmd.eq_ignore_ascii_case("WAIT") => {
                self.handle_wait(stream, num_replicas, timeout)
            }
            _ => {
                return stream.write_all(b"-ERR unknown command\r\n");
            }
        }
    }
    fn handle_echo(&self, stream: &mut TcpStream, arg: &String) -> io::Result<()> {
        let resp = format!("${}\r\n{}\r\n", arg.len(), arg);
        stream.write_all(resp.as_bytes())
    }

    fn handle_set_internal(&mut self, key: &String, value: &String, args: &[String]) {
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
                return;
            }
        };
        {
            let mut redis = self.redis.lock();
            redis.set(key.to_string(), value.to_string(), deadline);
        }
    }

    fn handle_set(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        value: &String,
        args: &[String],
    ) -> io::Result<()> {
        self.handle_set_internal(key, value, args);
        stream.write_all(b"+OK\r\n")
    }

    fn handle_get(&mut self, stream: &mut TcpStream, key: &String) -> io::Result<()> {
        let res = {
            let mut redis = self.redis.lock();
            redis.get(key)
        };

        match res {
            Ok(Some(s)) => {
                let v = format!("${}\r\n{}\r\n", s.len(), s.to_string());
                stream.write_all(v.as_bytes())
            }
            Ok(None) => stream.write_all(b"$-1\r\n"),
            Err(e) => write!(stream, "-ERR {e}\r\n"),
        }
    }

    fn handle_rpush_internal(&mut self, key: &String, values: &[String]) -> String {
        let mut redis = self.redis.lock();
        let res = redis.rpush(key, values);
        match res {
            Ok(size) => {
                redis.notify_one(key);
                format!(":{}\r\n", size)
            }
            Err(e) => format!("{e}"),
        }
    }

    fn handle_rpush(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        values: &[String],
    ) -> io::Result<()> {
        let output = self.handle_rpush_internal(key, values);
        stream.write_all(output.as_bytes())
    }

    fn handle_lpush_internal(&mut self, key: &String, values: &[String]) -> String {
        let mut redis = self.redis.lock();
        let res = redis.lpush(key, values);
        match res {
            Ok(size) => {
                redis.notify_one(key);
                format!(":{}\r\n", size)
            }
            Err(e) => format!("{e}"),
        }
    }

    fn handle_lpush(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        values: &[String],
    ) -> io::Result<()> {
        let output = self.handle_lpush_internal(key, values);
        stream.write_all(output.as_bytes())
    }

    fn handle_lrange(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        start: &String,
        end: &String,
    ) -> io::Result<()> {
        let Ok(start) = start.parse::<isize>() else {
            return write!(stream, "invalid value of start: {start}");
        };
        let Ok(end) = end.parse::<isize>() else {
            return write!(stream, "invalid value of end: {end}");
        };
        let values = {
            let mut redis = self.redis.lock();
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

    fn handle_llen(&mut self, stream: &mut TcpStream, key: &String) -> io::Result<()> {
        let size = {
            let mut redis = self.redis.lock();
            redis.llen(key)
        };
        write!(stream, ":{}\r\n", size)
    }

    fn handle_lpop_internal(
        &mut self,
        key: &String,
        arg: &[String],
    ) -> Result<Option<Vec<String>>, RedisError> {
        if arg.len() > 1 {
            eprintln!("wrong argument: {:?}", arg);
            return Err(RedisError::WrongArgument);
        }
        let num = match arg.first() {
            None => 1,
            Some(v) => match v.parse::<usize>() {
                Ok(n) => n,
                Err(_) => {
                    eprintln!("wrong argument {v}");
                    return Err(RedisError::WrongArgument);
                }
            },
        };

        let mut redis = self.redis.lock();
        redis.lpop(key, num)
    }

    fn handle_lpop(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        arg: &[String],
    ) -> io::Result<()> {
        let res = self.handle_lpop_internal(key, arg);
        let is_array = arg.first().is_some();

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
    // NOT SURE HOW TO DO THIS IN REPLICATION MODE
    fn handle_blpop(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        arg: &String,
    ) -> io::Result<()> {
        let Ok(secs) = arg.parse::<f64>() else {
            return write!(stream, "-ERR invalid timeout: {arg}\r\n");
        };
        let timeout = Duration::from_secs_f64(secs);
        let mut db = self.redis.lock();
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

    fn handle_type(&mut self, stream: &mut TcpStream, key: &str) -> io::Result<()> {
        let t = {
            let mut redis = self.redis.lock();
            match redis.get_value(key) {
                Some(Value::Integer(_)) => "integer",
                Some(Value::String(_)) => "string",
                Some(Value::List(_)) => "list",
                Some(Value::Stream(_)) => "stream",
                None => "none",
            }
        };
        write!(stream, "+{t}\r\n")
    }

    fn handle_xadd_internal(&mut self, key: &str, id: &str, args: &[String]) -> String {
        let mut redis = self.redis.lock();
        match redis.x_add(key, id, args) {
            Ok(id) => {
                redis.notify_one(key);
                format!("${}\r\n{}\r\n", id.len(), id)
            }
            Err(e) => format!("-ERR {e}\r\n"),
        }
    }

    fn handle_xadd(
        &mut self,
        stream: &mut TcpStream,
        key: &str,
        id: &str,
        args: &[String],
    ) -> io::Result<()> {
        if !is_at_least_two_and_even(args.len()) {
            return write!(stream, "-ERR: wrong number of arguments: {}", args.len());
        }
        let output = self.handle_xadd_internal(key, id, args);
        stream.write_all(output.as_bytes())
    }

    fn handle_xrange(
        &mut self,
        stream: &mut TcpStream,
        key: &str,
        start_id: &str,
        end_id: &str,
    ) -> io::Result<()> {
        let res = {
            let mut redis = self.redis.lock();
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

    fn handle_xread(&mut self, stream: &mut TcpStream, args: &[String]) -> io::Result<()> {
        let len = args.len();
        if !is_at_least_two_and_even(len) {
            return write!(stream, "-ERR wrong number of arguments: {}", len);
        }
        let n = len / 2;
        let pairs: Vec<(&String, &String)> = args[..n].iter().zip(args[n..].iter()).collect();
        let res = {
            let mut redis = self.redis.lock();
            redis.x_read(&pairs)
        };
        match res {
            Ok(values) => write_x_read(stream, &values),
            Err(e) => write!(stream, "-ERR {e}"),
        }
    }

    fn handle_block_xread(
        &mut self,
        stream: &mut TcpStream,
        ms: &str,
        key: &String,
        id: &String,
    ) -> io::Result<()> {
        let Ok(ms) = ms.parse::<u64>() else {
            return write!(stream, "-ERR invalid timeout: {ms}\r\n");
        };
        let timeout = Duration::from_millis(ms);
        let mut db = self.redis.lock();
        let id = if id == "$" {
            match db.get_stream(key) {
                Ok(Some(s)) => s.last_id().to_string(),
                _ => EntryId::ZERO.to_string(),
            }
        } else {
            id.to_string()
        };
        loop {
            match db.x_read(&[(key, &id)]) {
                Ok(values) if !values[0].1.is_empty() => {
                    return write_x_read(stream, &values);
                }
                Ok(_) => {
                    let waiter = db.add_waiter(key);
                    if timeout.is_zero() {
                        db = waiter.wait(db).unwrap();
                    } else {
                        let (guard, timeout_result) = waiter.wait_timeout(db, timeout).unwrap();
                        db = guard;
                        if timeout_result.timed_out() {
                            return stream.write_all(b"*-1\r\n");
                        }
                    }
                    continue;
                }
                Err(e) => {
                    return write!(stream, "-ERR {e}\r\n");
                }
            }
        }
    }

    fn handle_incr_internal(&mut self, key: &str) -> Result<u64, RedisError> {
        let mut redis = self.redis.lock();
        redis.incr(key)
    }

    fn handle_incr(&mut self, stream: &mut TcpStream, key: &str) -> io::Result<()> {
        let res = self.handle_incr_internal(key);
        match res {
            Ok(num) => {
                write!(stream, ":{}\r\n", num)
            }
            Err(_) => {
                write!(stream, "-ERR value is not an integer or out of range\r\n")
            }
        }
    }
    fn handle_multi(&mut self, stream: &mut TcpStream) -> io::Result<()> {
        self.transaction = Some(Transaction::new());
        stream.write_all(b"+OK\r\n")
    }
    fn handle_exec(&mut self, stream: &mut TcpStream) -> io::Result<()> {
        let Some(tx) = self.transaction.take() else {
            return stream.write_all(b"-ERR EXEC without MULTI\r\n");
        };
        {
            let redis = self.redis.lock();
            if self
                .watch_list
                .iter()
                .any(|(k, v)| *v != redis.get_version(k))
            {
                self.watch_list.clear();
                return stream.write_all(b"*-1\r\n");
            }
        }
        write!(stream, "*{}\r\n", tx.commands.len())?;
        for cmd in tx.commands {
            self.handle_command(stream, cmd)?;
        }
        self.watch_list.clear();
        Ok(())
    }
    fn handle_discard(&mut self, stream: &mut TcpStream) -> io::Result<()> {
        let Some(_) = self.transaction.take() else {
            return stream.write_all(b"-ERR DISCARD without MULTI\r\n");
        };
        self.watch_list.clear();
        write!(stream, "+OK\r\n")
    }
    fn handle_watch(&mut self, stream: &mut TcpStream, keys: &[String]) -> io::Result<()> {
        if self.transaction.is_some() {
            return stream.write_all(b"-ERR WATCH inside MULTI is not allowed\r\n");
        }
        {
            let redis = self.redis.lock();
            for key in keys {
                self.watch_list
                    .insert(key.to_string(), redis.get_version(key));
            }
        }
        write!(stream, "+OK\r\n")
    }
    fn handle_unwatch(&mut self, stream: &mut TcpStream) -> io::Result<()> {
        self.watch_list.clear();
        write!(stream, "+OK\r\n")
    }
    fn handle_info(&mut self, stream: &mut TcpStream, subcmd: &str) -> io::Result<()> {
        match subcmd {
            "replication" => {
                let info = if let Role::Master(master) = &self.role {
                    let master_info = &master.info;
                    format!(
                        "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
                        self.role, master_info.master_replid, master_info.offset,
                    )
                } else {
                    format!("role:{}\r\n", self.role)
                };
                write!(stream, "${}\r\n{}\r\n", info.len(), info)?;
            }
            _ => {}
        }
        Ok(())
    }
    fn handle_replconf(&mut self, stream: &mut TcpStream) -> io::Result<()> {
        stream.write_all(b"+OK\r\n")
    }
    fn handle_psync(&mut self, mut stream: TcpStream) -> io::Result<()> {
        let Role::Master(master) = &mut self.role else {
            return Err(io::Error::other("PSYNC called from non-master"));
        };

        let master_info = &master.info;
        let hex_content = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        let rdb_data = hex::decode(hex_content).unwrap();
        write!(stream, "+FULLRESYNC {} 0\r\n", master_info.master_replid)?;
        write!(stream, "${}\r\n", rdb_data.len())?;
        stream.write_all(&rdb_data)?;

        let tx = master.replication_tx.clone();
        let _ = tx.send(ReplicationMessage::AddReplica(stream));

        Ok(())
    }

    fn handle_wait(
        &mut self,
        stream: &mut TcpStream,
        num_replicas: &str,
        timeout: &str,
    ) -> io::Result<()> {
        let num_replicas = num_replicas
            .parse::<usize>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let timeout = timeout
            .parse::<u64>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let timeout = Duration::from_millis(timeout);
        let Role::Master(master) = &self.role else {
            return Err(io::Error::other("WAIT called from non-master"));
        };
        let (tx, rx) = channel::<usize>();
        println!("============== gonna send wait message");
        let _ = master.replication_tx.send(ReplicationMessage::Wait {
            num_replicas,
            timeout,
            response: tx,
        });
        // TODO: is this error ok?
        let replica_count = rx
            .recv()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        println!("============== replica count {}", replica_count);
        // let replica_count = master.replica_count.load(Ordering::Acquire);
        write!(stream, ":{}\r\n", replica_count)
    }
}

fn is_at_least_two_and_even(len: usize) -> bool {
    len >= 2 && len.is_multiple_of(2)
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
