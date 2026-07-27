use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    str::FromStr,
    sync::{Arc, Condvar},
    time::Instant,
};

#[derive(Debug)]
pub enum RedisError {
    WrongType,
    StreamIdParseError,
    InvalidStreamId,
}

impl Display for RedisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisError::WrongType => write!(f, "wrong type"),
            RedisError::StreamIdParseError => write!(f, "stream id parse error"),
            RedisError::InvalidStreamId => write!(f, "invalid stream id"),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    ms: u64,
    sq: u64,
}

impl StreamId {
    pub const ZERO: Self = Self { ms: 0, sq: 0 };
}

impl FromStr for StreamId {
    type Err = RedisError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (ms, sq) = s.split_once('-').ok_or(RedisError::StreamIdParseError)?;
        Ok(StreamId {
            ms: ms
                .parse::<u64>()
                .map_err(|_| RedisError::StreamIdParseError)?,
            sq: sq
                .parse::<u64>()
                .map_err(|_| RedisError::StreamIdParseError)?,
        })
    }
}

#[derive(Clone)]
pub struct StreamEntry {
    id: StreamId,
    fields: Vec<(String, String)>,
}

impl StreamEntry {
    pub fn new(id: StreamId) -> Self {
        Self {
            id: id,
            fields: Vec::new(),
        }
    }
    pub fn add(&mut self, key: String, value: String) {
        self.fields.push((key, value));
    }
}

#[derive(Clone)]
pub struct Stream {
    entries: Vec<StreamEntry>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
    pub fn add(&mut self, entry: StreamEntry) {
        self.entries.push(entry);
    }

    fn validate(&self, entry_id: StreamId) -> bool {
        let last = self.entries.last().map_or(StreamId::ZERO, |e| e.id);
        entry_id > last
    }
}

#[derive(Clone)]
pub enum Value {
    String(String),
    List(VecDeque<String>),
    Stream(Stream),
}

pub struct Redis {
    store: HashMap<String, Value>,
    expire: HashMap<String, Instant>,
    waiters: HashMap<String, Arc<Condvar>>,
}

impl Redis {
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
            expire: HashMap::new(),
            waiters: HashMap::new(),
        }
    }
    pub fn set(&mut self, key: String, value: String, deadline: Option<Instant>) {
        self.store.insert(key.clone(), Value::String(value));
        match deadline {
            Some(deadline) => {
                let _ = self.expire.insert(key, deadline);
            }
            None => {
                let _ = self.expire.remove(&key);
            }
        }
    }

    pub fn get_value(&mut self, key: &str) -> Option<&Value> {
        let expired = self
            .expire
            .get(key)
            .is_some_and(|deadline| *deadline <= Instant::now());
        if expired {
            self.expire.remove(key);
            self.store.remove(key);
            return None;
        }
        self.store.get(key)
    }

    pub fn get_string(&mut self, key: &str) -> Option<&String> {
        let Some(Value::String(s)) = self.get_value(key) else {
            return None;
        };
        Some(s)
    }

    pub fn get_list(&mut self, key: &str) -> Option<&VecDeque<String>> {
        let Some(Value::List(l)) = self.get_value(key) else {
            return None;
        };
        Some(l)
    }

    fn get_mut_value(&mut self, key: &str) -> Option<&mut Value> {
        let expired = self
            .expire
            .get(key)
            .is_some_and(|deadline| *deadline <= Instant::now());
        if expired {
            self.expire.remove(key);
            self.store.remove(key);
            return None;
        }
        self.store.get_mut(key)
    }

    pub fn rpush(&mut self, key: &str, values: &[String]) -> Result<usize, RedisError> {
        match self.get_mut_value(key) {
            Some(Value::List(l)) => {
                for v in values {
                    l.push_back(v.clone());
                }
                Ok(l.len())
            }
            Some(_) => Err(RedisError::WrongType),
            None => {
                let mut l = VecDeque::new();
                for v in values {
                    l.push_back(v.clone());
                }
                self.store.insert(key.to_string(), Value::List(l));
                Ok(values.len())
            }
        }
    }

    pub fn lpush(&mut self, key: &str, values: &[String]) -> Result<usize, RedisError> {
        match self.get_mut_value(key) {
            Some(Value::List(l)) => {
                for v in values {
                    l.push_front(v.clone());
                }
                Ok(l.len())
            }
            Some(_) => Err(RedisError::WrongType),
            None => {
                let mut l = VecDeque::new();
                for v in values {
                    l.push_back(v.clone());
                }
                self.store.insert(key.to_string(), Value::List(l));
                Ok(values.len())
            }
        }
    }
    pub fn lrange(&mut self, key: &str, start: isize, end: isize) -> Vec<String> {
        let Some(l) = self.get_list(key) else {
            return Vec::new();
        };
        let start = normalize_idx(start, l.len());
        let end = normalize_idx(end, l.len());
        if start >= end {
            return Vec::new();
        }

        if start >= l.len() {
            return Vec::new();
        }
        let end = end.min(l.len() - 1);
        l.range(start..=end).cloned().collect()
    }
    pub fn llen(&mut self, key: &str) -> usize {
        let Some(l) = self.get_list(key) else {
            return 0;
        };
        l.len()
    }
    pub fn lpop(&mut self, key: &str, num: usize) -> Result<Option<Vec<String>>, RedisError> {
        match self.get_mut_value(key) {
            Some(Value::List(l)) => {
                let num = num.min(l.len());
                let res = l.drain(0..num).collect();
                Ok(Some(res))
            }
            Some(_) => Err(RedisError::WrongType),
            None => Ok(None),
        }
    }
    pub fn add_waiter(&mut self, key: &str) -> Arc<Condvar> {
        self.waiters
            .entry(key.to_string())
            .or_insert(Arc::new(Condvar::new()))
            .clone()
    }
    pub fn notify_one(&mut self, key: &str) {
        if let Some(cv) = self.waiters.get(key) {
            cv.notify_one();
        }
    }
    pub fn x_add(&mut self, key: &str, id: &str, args: &[String]) -> Result<String, RedisError> {
        let stream_id = StreamId::from_str(id)?;
        let mut entry = StreamEntry::new(stream_id);
        for chunk in args.chunks_exact(2) {
            let (k, v) = (&chunk[0], &chunk[1]);
            entry.add(k.clone(), v.clone());
        }
        match self.get_mut_value(key) {
            Some(Value::Stream(s)) => {
                if !s.validate(entry.id) {
                    return Err(RedisError::InvalidStreamId);
                }
                s.add(entry)
            }
            Some(_) => return Err(RedisError::WrongType),
            None => {
                let mut stream = Stream::new();
                if !stream.validate(entry.id) {
                    return Err(RedisError::InvalidStreamId);
                }
                stream.add(entry);
                self.store.insert(key.to_string(), Value::Stream(stream));
            }
        }
        Ok(id.to_string())
    }
}

fn normalize_idx(idx: isize, length: usize) -> usize {
    let idx = if idx < 0 { length as isize + idx } else { idx };
    idx.max(0) as usize
}
