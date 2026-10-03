use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::io::BufReader;
use std::str;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};
use std::{io, thread};
use std::{
    io::{Read, Write},
    net::TcpStream,
};

use std::fmt::Write as FmtWrite;

use crate::auth::{Auth, DEFAULT_USER, User};
use crate::config::Config;
use crate::fatal;
use crate::helper::{encode_geopos_res, encode_slice, write_err, write_slice_resp};
use crate::helper::{io_invalid_data, is_allowed_in_subscribed_mode};
use crate::parser::{Command, Parser};
use crate::pubsub::WriteMsg;
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
    pub fn with_db(db: Redis) -> Self {
        Self {
            redis: Mutex::new(db),
        }
    }
    pub fn lock(&self) -> MutexGuard<'_, Redis> {
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
                replicas.push(stream);
            }
            ReplicationMessage::Command(data) => {
                // TODO: a slow replica can block the whole loop and sender side is blocked
                // each replica should have its own dedicated thread and queue? and how to handle the slow replica
                // when the queue is full? ditch them?
                current_offset += data.len();
                replicas.retain_mut(|stream| stream.write_all(&data).is_ok());
            }
            ReplicationMessage::Wait { timeout, response } => {
                if current_offset == 0 {
                    let _ = response.send(replicas.len());
                    continue;
                }
                let cmd = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
                // let deadline = Instant::now().add(timeout);
                let mut replica_count: usize; // number of replicas caught up
                replica_count = 0;
                std::thread::scope(|scope| {
                    let handles: Vec<_> = replicas
                        .iter_mut()
                        .map(|stream| {
                            scope.spawn(|| {
                                stream.write_all(cmd)?;
                                stream.set_read_timeout(Some(timeout))?;
                                read_ack_resp(stream)
                            })
                        })
                        .collect();

                    for handle in handles {
                        match handle.join() {
                            Ok(Ok(offset)) => {
                                if offset >= current_offset {
                                    replica_count += 1;
                                }
                            }
                            Ok(Err(e)) => {
                                eprintln!("error in handle join: {}", e);
                            }
                            _ => {} // TODO: handle this error?
                        }
                    }
                });
                current_offset += cmd.len();

                let _ = response.send(replica_count);
            }
        }
    }
}

// TODO: this fit perfectly with command parser, find a way to use the parser
fn read_ack_resp(stream: &mut TcpStream) -> io::Result<usize> {
    let header = readline_exact(stream)?;
    if header != b"*3" {
        return Err(io_invalid_data("invalid header"));
    }
    let len = readline_exact(stream)?;
    if len != b"$8" {
        return Err(io_invalid_data("invalid header"));
    }
    let mut buf = vec![0u8; 10];
    stream.read_exact(&mut buf)?;
    if buf != b"REPLCONF\r\n" {
        return Err(io_invalid_data("expect replconf command"));
    }
    let len = readline_exact(stream)?;
    if len != b"$3" {
        return Err(io_invalid_data("expect length of 3"));
    }

    let mut buf = vec![0u8; 5];
    stream.read_exact(&mut buf)?;
    if buf != b"ACK\r\n" {
        return Err(io_invalid_data("expect ack"));
    }
    let len = readline_exact(stream)?;
    let len = parse_bulk_len(&len)?;
    let mut buf = vec![0u8; len + 2];
    stream.read_exact(&mut buf)?;
    if &buf[len..] != b"\r\n" {
        return Err(io_invalid_data("crlf missing"));
    }
    let s = std::str::from_utf8(&buf[..len]).map_err(|_| io_invalid_data("invalid utf8"))?;
    s.parse::<usize>()
        .map_err(|_| io_invalid_data("invalid length"))
}

pub fn readline_exact(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut line: Vec<u8> = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        if let Err(e) = stream.read_exact(&mut byte) {
            return Err(e);
        };
        if byte[0] == b'\n' {
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return Ok(line);
        }
        line.push(byte[0])
    }
}

pub fn parse_bulk_len(line: &[u8]) -> io::Result<usize> {
    let s = std::str::from_utf8(line).map_err(|_| io_invalid_data("invalid utf8"))?;
    let s = s
        .strip_prefix('$')
        .ok_or_else(|| io_invalid_data("missing $"))?;
    s.parse::<usize>()
        .map_err(|_| io_invalid_data("invalid usize"))
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
    id: u64,
    role: Role,
    redis: SharedRedis,
    auth: Arc<Auth>,
    transaction: Option<Transaction>,
    watch_list: HashMap<String, u64>,
    config: Config,
    pubsub_tx: Option<Sender<WriteMsg>>,
    user: User,
    is_authenticated: bool,
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

    'outer: loop {
        while let Some(command) = parser.try_parse().unwrap_or_else(|e| {
            eprintln!("protocol error: {e}");
            std::process::exit(1); // or handle more gracefully
        }) {
            match command_handler(stream, command) {
                Next::Continue(s) => stream = s,
                Next::Done => {
                    break 'outer;
                }
            }
        }

        let n = match stream.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                eprintln!("read error: {e}");
                break;
            }
        };
        parser.feed(&tmp[..n]);
    }
}

impl Handler {
    pub fn new(id: u64, role: Role, redis: SharedRedis, auth: Arc<Auth>, config: Config) -> Self {
        Self {
            id,
            role,
            redis,
            auth,
            transaction: None,
            watch_list: HashMap::new(),
            config,
            pubsub_tx: None,
            user: User::new(DEFAULT_USER.to_string()),
            is_authenticated: false,
        }
    }
    pub fn handle(&mut self, stream: TcpStream) {
        read_loop(stream, |mut stream, command| {
            let command: Vec<String> = command
                .items
                .iter()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .collect();

            // handle_auth login command first
            // combining 2 if-let and conditional
            if let [cmd, name, pwd] = command.as_slice()
                && cmd.eq_ignore_ascii_case("AUTH")
                && let Err(e) = self.handle_auth(&mut stream, name, pwd)
            {
                eprintln!("error handling auth: {e}");
                return Next::Done;
            }
            // check authetication later
            if !self.is_authenticated && self.auth.default_auth() {
                self.is_authenticated = true;
                self.user = User::new(DEFAULT_USER.to_string()); // TODO: initial user name is redundant?
                self.user.set_nopass();
            }
            if !self.is_authenticated {
                if let Err(e) = write_err(&mut stream, "-ERR: NOAUTH Authentication required.") {
                    eprintln!("error writing to stream: {e}");
                    return Next::Done;
                }
                return Next::Continue(stream);
            }

            if self.pubsub_tx.is_some() {
                return self.handle_subscribed(stream, command);
            }
            self.handle_normal(stream, command)
        });
        self.cleanup_pubsub();
    }

    fn handle_auth(
        &mut self,
        stream: &mut TcpStream,
        name: &String,
        pwd: &String,
    ) -> io::Result<()> {
        if self.auth.auth(name, pwd) {
            self.is_authenticated = true;
            self.user = User::new(name.to_string());
            self.user.set_pwd(self.auth.pwd_of(name).unwrap());
            return stream.write_all(b"+OK\r\n");
        }
        stream.write_all(b"-ERR WRONGPASS invalid username-password pair or user is disabled.")
    }

    fn handle_normal(&mut self, mut stream: TcpStream, command: Vec<String>) -> Next {
        if let Some(cmd) = command.first()
            && cmd.eq_ignore_ascii_case("PSYNC")
        {
            if let Err(e) = self.handle_psync(stream) {
                eprintln!("psync io error: {:?}", e);
            }
            return Next::Done;
        }
        let is_write = is_write_command(&command); // USE OPTION for encoded and eliminate is_write
        let encoded = encode_slice(&command);
        let cmd_log = command.clone();
        if let Err(e) = self.handle_command(&mut stream, command) {
            eprintln!("handle command: io error: {:?}, command: {:?}", e, cmd_log);
            return Next::Done;
        }
        if is_write {
            if let Role::Master(master) = &mut self.role {
                let tx = master.replication_tx.clone();
                let _ = tx.send(ReplicationMessage::Command(encoded));
            }
        }
        Next::Continue(stream)
    }

    fn handle_subscribed(&mut self, mut stream: TcpStream, command: Vec<String>) -> Next {
        if command.len() == 0 {
            return Next::Continue(stream);
        }
        if !is_allowed_in_subscribed_mode(&command) {
            if write!(stream, "-ERR can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n", command.first().unwrap().to_ascii_lowercase()).is_err() {
                eprintln!("error writing response to stream");
                return Next::Done;
            }
        }
        let res = match command.as_slice() {
            [cmd, arg] if cmd.eq_ignore_ascii_case("SUBSCRIBE") => {
                self.handle_subscribe(&mut stream, arg)
            }
            [cmd] if cmd.eq_ignore_ascii_case("PING") => {
                let tx = self.pubsub_tx.as_ref().unwrap().clone();
                tx.send(encode_slice(&["pong".to_string(), "".to_string()]))
                    .map_err(io::Error::other)
            }
            [cmd, channel, msg] if cmd.eq_ignore_ascii_case("PUBLISH") => {
                self.handle_publish(&mut stream, channel, msg)
            }
            [cmd, channel] if cmd.eq_ignore_ascii_case("UNSUBSCRIBE") => {
                self.handle_unsubscribe(&mut stream, channel)
            }
            _ => Ok(()),
        };
        if res.is_err() {
            eprintln!("error handling command in subscribed mode: {:?}", command);
            return Next::Done;
        }
        return Next::Continue(stream); // TODO: implement subscribe mode handler
    }

    pub fn handle_replay(&mut self, file: Option<File>) -> io::Result<()> {
        let Some(file) = file else { return Ok(()) };
        let mut reader = BufReader::new(file);
        let mut parser = Parser::new();
        let mut tmp = [0u8; 512];
        loop {
            while let Some(command) = parser.try_parse().unwrap_or_else(|e| {
                eprintln!("protocol error: {e}");
                std::process::exit(1);
            }) {
                let command: Vec<String> = command
                    .items
                    .iter()
                    .map(|b| String::from_utf8_lossy(b).to_string()) // TODO: move to Vec<u8>, no lossy things anymore
                    .collect();
                match command.as_slice() {
                    [cmd, key, value, args @ ..] if cmd.eq_ignore_ascii_case("SET") => {
                        let _ = self.handle_set_internal(key, value, args, None);
                    }
                    [cmd, key, values @ ..] if cmd.eq_ignore_ascii_case("RPUSH") => {
                        let _ = self.handle_rpush_internal(key, values, None);
                    }
                    [cmd, key, values @ ..] if cmd.eq_ignore_ascii_case("LPUSH") => {
                        let _ = self.handle_lpush_internal(key, values, None);
                    }
                    [cmd, key, arg @ ..] if cmd.eq_ignore_ascii_case("LPOP") => {
                        let _ = self.handle_lpop_internal(key, arg);
                    }
                    [cmd, key] if cmd.eq_ignore_ascii_case("INCR") => {
                        let _ = self.handle_incr_internal(key, None);
                    }
                    _ => return Err(io_invalid_data(format!("invalid command {:?}", command))),
                }
            }
            let n = reader.read(&mut tmp)?;
            if n == 0 {
                return Ok(());
            }
            parser.feed(&tmp[..n]);
        }
    }

    pub fn handle_replication(&mut self, stream: TcpStream) {
        // AUTH: we don't check authentication in replication
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
                let _ = self.handle_set_internal(key, value, args, None);
            }
            [cmd, key, values @ ..] if cmd.eq_ignore_ascii_case("RPUSH") => {
                let _ = self.handle_rpush_internal(key, values, None);
            }
            [cmd, key, values @ ..] if cmd.eq_ignore_ascii_case("LPUSH") => {
                let _ = self.handle_lpush_internal(key, values, None);
            }
            [cmd, key, arg @ ..] if cmd.eq_ignore_ascii_case("LPOP") => {
                let _ = self.handle_lpop_internal(key, arg);
            }
            [cmd, key] if cmd.eq_ignore_ascii_case("INCR") => {
                let _ = self.handle_incr_internal(key, None);
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
                if let Err(e) = write_slice_resp(stream, &["REPLCONF", "ACK", &offset]) {
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
        let aof = if is_write_command(&command) {
            Some(encode_slice(&command))
        } else {
            None
        };
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
                self.handle_set(stream, key, value, args, aof)
            }
            [cmd, key] if cmd.eq_ignore_ascii_case("GET") => self.handle_get(stream, key),
            [cmd, key, values @ ..] if cmd.eq_ignore_ascii_case("RPUSH") => {
                self.handle_rpush(stream, key, values, aof)
            }
            [cmd, key, values @ ..] if cmd.eq_ignore_ascii_case("LPUSH") => {
                self.handle_lpush(stream, key, values, aof)
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
            [cmd, key] if cmd.eq_ignore_ascii_case("INCR") => self.handle_incr(stream, key, aof),
            [cmd] if cmd.eq_ignore_ascii_case("MULTI") => self.handle_multi(stream),
            [cmd, subcmd] if cmd.eq_ignore_ascii_case("INFO") => self.handle_info(stream, subcmd),
            [cmd, ..] if cmd.eq_ignore_ascii_case("REPLCONF") => self.handle_replconf(stream),
            [cmd, num_replicas, timeout] if cmd.eq_ignore_ascii_case("WAIT") => {
                self.handle_wait(stream, num_replicas, timeout)
            }
            [cmd, subcmd, arg]
                if cmd.eq_ignore_ascii_case("CONFIG") && subcmd.eq_ignore_ascii_case("GET") =>
            {
                self.handle_config_get(stream, arg)
            }
            [cmd, arg] if cmd.eq_ignore_ascii_case("KEYS") && arg.eq_ignore_ascii_case("*") => {
                self.handle_get_all_keys(stream)
            }
            [cmd, arg] if cmd.eq_ignore_ascii_case("SUBSCRIBE") => {
                self.handle_subscribe(stream, arg)
            }
            [cmd, channel, msg] if cmd.eq_ignore_ascii_case("PUBLISH") => {
                self.handle_publish(stream, channel, msg)
            }
            [cmd, channel] if cmd.eq_ignore_ascii_case("UNSUBSCRIBE") => {
                self.handle_unsubscribe(stream, channel)
            }
            [cmd, key, score, member] if cmd.eq_ignore_ascii_case("ZADD") => {
                self.handle_zadd(stream, key, score, member)
            }
            [cmd, key, member] if cmd.eq_ignore_ascii_case("ZRANK") => {
                self.handle_zrank(stream, key, member)
            }
            [cmd, key, start, end] if cmd.eq_ignore_ascii_case("ZRANGE") => {
                self.handle_zrange(stream, key, start, end)
            }
            [cmd, key] if cmd.eq_ignore_ascii_case("ZCARD") => self.handle_zcard(stream, key),
            [cmd, key, name] if cmd.eq_ignore_ascii_case("ZSCORE") => {
                self.handle_zscore(stream, key, name)
            }
            [cmd, key, name] if cmd.eq_ignore_ascii_case("ZREM") => {
                self.handle_zremove(stream, key, name)
            }
            [cmd, key, longitude, latitude, location] if cmd.eq_ignore_ascii_case("GEOADD") => {
                self.handle_geoadd(stream, key, longitude, latitude, location)
            }
            [cmd, key, locations @ ..] if cmd.eq_ignore_ascii_case("GEOPOS") => {
                self.handle_geopos(stream, key, locations)
            }
            [cmd, key, first, second] if cmd.eq_ignore_ascii_case("GEODIST") => {
                self.handle_geodist(stream, key, first, second)
            }
            [cmd, key, mode, longitude, latitude, option, dist, unit]
                if cmd.eq_ignore_ascii_case("GEOSEARCH") =>
            {
                self.handle_geosearch(stream, key, mode, longitude, latitude, option, dist, unit)
            }
            [cmd, subcmd]
                if cmd.eq_ignore_ascii_case("ACL") && subcmd.eq_ignore_ascii_case("WHOAMI") =>
            {
                self.handle_acl_whoami(stream)
            }
            [cmd, subcmd, name]
                if cmd.eq_ignore_ascii_case("ACL") && subcmd.eq_ignore_ascii_case("GETUSER") =>
            {
                self.handle_acl_getuser(stream, name)
            }
            [cmd, subcmd, name, pwd]
                if cmd.eq_ignore_ascii_case("ACL") && subcmd.eq_ignore_ascii_case("SETUSER") =>
            {
                self.handle_acl_setuser(stream, name, pwd)
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

    fn handle_set_internal(
        &mut self,
        key: &String,
        value: &String,
        args: &[String],
        aof: Option<Vec<u8>>,
    ) -> io::Result<()> {
        let deadline = match args {
            [] => None,
            [cmd, seconds] if cmd.eq_ignore_ascii_case("EX") => {
                Some(SystemTime::now() + Duration::from_secs(seconds.parse().unwrap()))
            }
            [cmd, milliseconds] if cmd.eq_ignore_ascii_case("PX") => {
                Some(SystemTime::now() + Duration::from_millis(milliseconds.parse().unwrap()))
            }
            _ => {
                return Err(io_invalid_data(format!("invalid argument: {:?}", args)));
            }
        };
        {
            let mut redis = self.redis.lock();
            redis.set(key.to_string(), value.to_string(), deadline);
            redis.write_aof(aof)
        }
    }

    fn handle_set(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        value: &String,
        args: &[String],
        aof: Option<Vec<u8>>,
    ) -> io::Result<()> {
        self.handle_set_internal(key, value, args, aof)?;
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

    fn handle_rpush_internal(
        &mut self,
        key: &String,
        values: &[String],
        aof: Option<Vec<u8>>,
    ) -> io::Result<String> {
        let mut redis = self.redis.lock();
        let res = redis.rpush(key, values);
        redis.write_aof(aof)?;
        match res {
            Ok(size) => {
                redis.notify_one(key);
                Ok(format!(":{}\r\n", size))
            }
            Err(e) => Ok(format!("{e}")),
        }
    }

    fn handle_rpush(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        values: &[String],
        aof: Option<Vec<u8>>,
    ) -> io::Result<()> {
        let output = self.handle_rpush_internal(key, values, aof)?;
        stream.write_all(output.as_bytes())
    }

    fn handle_lpush_internal(
        &mut self,
        key: &String,
        values: &[String],
        aof: Option<Vec<u8>>,
    ) -> io::Result<String> {
        let mut redis = self.redis.lock();
        let res = redis.lpush(key, values);
        redis.write_aof(aof)?;
        match res {
            Ok(size) => {
                redis.notify_one(key);
                Ok(format!(":{}\r\n", size))
            }
            Err(e) => Ok(format!("{e}")),
        }
    }

    fn handle_lpush(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        values: &[String],
        aof: Option<Vec<u8>>,
    ) -> io::Result<()> {
        let output = self.handle_lpush_internal(key, values, aof)?;
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
        write_slice_resp(stream, &values)?;
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
                write_slice_resp(stream, &values)?;
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
                    let mut items = Vec::with_capacity(1 + values.len());
                    items.push(key);
                    items.extend(values.iter());
                    write_slice_resp(stream, &items)?;
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
                            return write_slice_resp(stream, &["BLPOP", key, arg]);
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
                Some(Value::Zset(_)) => "zset",
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

    fn handle_incr_internal(&mut self, key: &str, aof: Option<Vec<u8>>) -> Result<u64, RedisError> {
        let mut redis = self.redis.lock();
        let num = redis.incr(key)?;
        redis
            .write_aof(aof)
            .map_err(|e| RedisError::WriteAofError(e))?;
        return Ok(num);
    }

    fn handle_incr(
        &mut self,
        stream: &mut TcpStream,
        key: &str,
        aof: Option<Vec<u8>>,
    ) -> io::Result<()> {
        let res = self.handle_incr_internal(key, aof);
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
        let _ = num_replicas
            .parse::<usize>()
            .map_err(|e| io_invalid_data(e))?; // num_replicas isn't used yet
        let timeout = timeout.parse::<u64>().map_err(|e| io_invalid_data(e))?;
        let timeout = Duration::from_millis(timeout);
        let Role::Master(master) = &self.role else {
            return Err(io::Error::other("WAIT called from non-master"));
        };
        let (tx, rx) = channel::<usize>();
        let _ = master.replication_tx.send(ReplicationMessage::Wait {
            timeout,
            response: tx,
        });
        // TODO: is this error ok?
        let replica_count = rx
            .recv()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("receive error: {}", e)))?;
        // let replica_count = master.replica_count.load(Ordering::Acquire);
        write!(stream, ":{}\r\n", replica_count)
    }

    fn handle_config_get(&mut self, stream: &mut TcpStream, arg: &String) -> io::Result<()> {
        match arg.as_str() {
            "dir" => {
                let dir = self.config.dir.to_string_lossy().into_owned();
                write_slice_resp(stream, &["dir", &dir])?;
            }
            "dbfilename" => {
                let dbfilename = self.config.rdb_filename.as_deref().unwrap_or("");
                write_slice_resp(stream, &["dbfilename", dbfilename])?;
            }
            "appendonly" => write_slice_resp(stream, &["appendonly", &self.config.append_only])?,
            "appenddirname" => {
                write_slice_resp(stream, &["appenddirname", &self.config.append_dirname])?
            }
            "appendfilename" => {
                write_slice_resp(stream, &["appendfilename", &self.config.append_filename])?
            }
            "appendfsync" => write_slice_resp(stream, &["appendfsync", &self.config.append_fsync])?,
            _ => {}
        }
        Ok(())
    }
    fn handle_get_all_keys(&mut self, stream: &mut TcpStream) -> io::Result<()> {
        let mut redis = self.redis.lock();
        let keys = redis.get_all_keys();
        let out = encode_slice(&keys);
        stream.write_all(&out)
    }
    fn handle_subscribe(
        &mut self,
        stream: &mut TcpStream,
        channel_name: &String,
    ) -> io::Result<()> {
        if self.pubsub_tx.is_none() {
            let (tx, rx) = channel();
            self.pubsub_tx = Some(tx);
            // self.mode = Mode::Subscribed; // TODO: it is redundant, think about removing
            let mut write_stream = stream.try_clone()?;
            thread::spawn(move || {
                for msg in rx {
                    if write_stream.write_all(&msg).is_err() {
                        break;
                    }
                }
            });
        }
        let tx = self.pubsub_tx.as_ref().unwrap();
        let mut redis = self.redis.lock();
        let len = redis.subscribe(channel_name.clone(), self.id, tx.clone());
        let output = format!(
            "*3\r\n$9\r\nsubscribe\r\n${}\r\n{}\r\n:{}\r\n",
            channel_name.len(),
            channel_name,
            len
        )
        .into_bytes();
        tx.send(output).map_err(io::Error::other)?;
        Ok(())
    }
    fn handle_publish(
        &mut self,
        stream: &mut TcpStream,
        channel: &str,
        msg: &str,
    ) -> io::Result<()> {
        let num_subscribers = self.redis.lock().publish(channel, msg);
        let reply = format!(":{}\r\n", num_subscribers).into_bytes();
        match self.pubsub_tx.as_ref() {
            // TODO: refactor? into a method?
            Some(tx) => tx.send(reply).map_err(io::Error::other),
            None => stream.write_all(&reply),
        }
    }
    fn handle_unsubscribe(&mut self, stream: &mut TcpStream, channel: &str) -> io::Result<()> {
        let num_channels = self.redis.lock().unsubscribe(channel, self.id);
        let reply = format!(
            "*3\r\n$11\r\nunsubscribe\r\n${}\r\n{}\r\n:{}\r\n",
            channel.len(),
            channel,
            num_channels
        )
        .into_bytes();
        match self.pubsub_tx.as_ref() {
            Some(tx) => tx.send(reply.clone()).map_err(io::Error::other),
            None => stream.write_all(&reply),
        }
    }
    fn handle_zadd(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        score: &String,
        member: &String,
    ) -> io::Result<()> {
        let score: f64 = score.parse().map_err(io_invalid_data)?;
        let res = self.redis.lock().z_add(key, score, member);
        match res {
            Ok(size) => write!(stream, ":{}\r\n", size),
            Err(e) => write!(stream, "-ERR: {}\r\n", e),
        }
    }
    fn handle_zrank(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        member: &String,
    ) -> io::Result<()> {
        let res = self.redis.lock().z_rank(key, member);
        match res {
            Ok(Some(r)) => write!(stream, ":{}\r\n", r),
            Ok(None) => write!(stream, "$-1\r\n"),
            Err(e) => write!(stream, "-ERR: {}", e),
        }
    }
    fn handle_zrange(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        start: &String,
        end: &String,
    ) -> io::Result<()> {
        let start: isize = start.parse().map_err(io_invalid_data)?;
        let end: isize = end.parse().map_err(io_invalid_data)?;
        let res = self.redis.lock().z_range(key, start, end);
        match res {
            Ok(v) => {
                let v: Vec<String> = v.into_iter().map(|item| item.name).collect();
                let output = encode_slice(v.as_slice());
                stream.write_all(&output)
            }
            Err(e) => {
                write!(stream, "-ERR: {e}")
            }
        }
    }

    fn handle_zcard(&mut self, stream: &mut TcpStream, key: &String) -> io::Result<()> {
        let res = self.redis.lock().z_card(key);
        match res {
            Ok(Some(size)) => {
                write!(stream, ":{}\r\n", size)
            }
            Ok(None) => {
                write!(stream, ":{}\r\n", 0)
            }
            Err(e) => write!(stream, "-ERR: {}\r\n", e),
        }
    }
    fn handle_zscore(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        name: &String,
    ) -> io::Result<()> {
        let res = self.redis.lock().z_score(key, name);
        match res {
            Ok(Some(score)) => {
                let score = score.to_string();
                write!(stream, "${}\r\n{}\r\n", score.len(), score)
            }
            Ok(None) => {
                write!(stream, "$-1\r\n")
            }
            Err(e) => write!(stream, "-ERR: {}\r\n", e),
        }
    }
    fn handle_zremove(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        name: &String,
    ) -> io::Result<()> {
        let res = self.redis.lock().z_remove(key, name);
        match res {
            Ok(true) => stream.write_all(b":1\r\n"),
            Ok(false) => stream.write_all(b":0\r\n"),
            Err(e) => write_err(stream, e),
        }
    }
    fn handle_geoadd(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        longitude: &String,
        latitude: &String,
        location: &String,
    ) -> io::Result<()> {
        let longitude: f64 = longitude.parse().map_err(io_invalid_data)?;
        let latitude: f64 = latitude.parse().map_err(io_invalid_data)?;
        let res = self
            .redis
            .lock()
            .geo_add(key, longitude, latitude, location);
        match res {
            Ok(n) => write!(stream, ":{}\r\n", n),
            Err(e) => {
                let e = match e {
                    RedisError::WrongArgument => {
                        format!("ERR invalid longitude,latitude pair {longitude}, {latitude}")
                    }
                    e => format!("{e}"),
                };
                write_err(stream, e)
            }
        }
    }
    fn handle_geopos(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        locations: &[String],
    ) -> io::Result<()> {
        let res = self.redis.lock().geo_pos(key, locations);
        match res {
            Ok(points) => stream.write_all(&encode_geopos_res(&points)),
            Err(e) => write_err(stream, e),
        }
    }

    fn handle_geodist(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        first: &String,
        second: &String,
    ) -> io::Result<()> {
        let res = self.redis.lock().geo_dist(key, first, second);
        match res {
            Ok(Some(dist)) => {
                let dist = dist.to_string();
                write!(stream, "${}\r\n{}\r\n", dist.len(), dist)
            }
            Ok(None) => stream.write_all(b"$-1\r\n"),
            Err(e) => write_err(stream, e),
        }
    }

    fn handle_geosearch(
        &mut self,
        stream: &mut TcpStream,
        key: &String,
        mode: &String,
        longitude: &String,
        latitude: &String,
        option: &String,
        dist: &String,
        unit: &String,
    ) -> io::Result<()> {
        if !mode.eq_ignore_ascii_case("FROMLONLAT") || !option.eq_ignore_ascii_case("BYRADIUS") {
            return write_err(stream, "invalid mode or option");
        }
        let mut dist: f64 = dist.parse().map_err(io_invalid_data)?;
        match unit.as_str() {
            "m" => {}
            "km" => {
                dist *= 100.0;
            }
            "mi" => dist *= 1609.344,
            _ => return Err(io_invalid_data("invalid unit")),
        }
        let longitude: f64 = longitude.parse().map_err(io_invalid_data)?;
        let latitude: f64 = latitude.parse().map_err(io_invalid_data)?;
        let res = self.redis.lock().geo_search(key, longitude, latitude, dist);
        match res {
            Ok(locations) => stream.write_all(&encode_slice(&locations)),
            Err(e) => write_err(stream, e),
        }
    }
    fn handle_acl_whoami(&mut self, stream: &mut TcpStream) -> io::Result<()> {
        let name = self.user.name();
        write!(stream, "${}\r\n{}\r\n", name.len(), name,)
    }
    fn handle_acl_getuser(&mut self, stream: &mut TcpStream, name: &String) -> io::Result<()> {
        if name != &self.user.name() {
            return write_err(stream, "user name not match");
        }
        let mut out = Vec::new();
        write!(&mut out, "*4\r\n$5\r\nflags\r\n").unwrap();
        let enabled_flags = self.user.enabled_flags();
        out.extend_from_slice(&encode_slice(&enabled_flags));
        write!(&mut out, "$9\r\npasswords\r\n").unwrap();
        // TODO: should pwd Option is enough? nopass is redundant?
        if self.user.nopass() {
            write!(&mut out, "*0\r\n").unwrap();
        } else {
            let hash = hex::encode(self.user.pwd().unwrap());
            out.extend_from_slice(&encode_slice(&vec![hash]));
        }
        stream.write_all(&out)
    }
    fn handle_acl_setuser(
        &mut self,
        stream: &mut TcpStream,
        name: &String,
        pwd: &String,
    ) -> io::Result<()> {
        if name != &self.user.name() {
            return write_err(stream, "username not maching");
        }
        let Some(pwd) = pwd.strip_prefix(">") else {
            return write_err(stream, "need > rule for password");
        };
        let hash = self.auth.set_pwd(name, pwd);
        self.user.set_pwd(hash);
        stream.write_all(b"+OK\r\n")
    }

    fn cleanup_pubsub(&mut self) {
        if self.pubsub_tx.is_some() {
            self.pubsub_tx.take();
            self.redis.lock().cleanup_subscription(self.id);
        }
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
