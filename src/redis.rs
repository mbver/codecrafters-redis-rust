use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    fs::File,
    io::{self, Write},
    str::FromStr,
    sync::{Arc, Condvar, mpsc::Sender},
    time::{SystemTime, UNIX_EPOCH},
    u64,
};

use crate::{
    geospatial::{GeoPoint, decode, encode, validate},
    pubsub::{PubSubRegistry, WriteMsg},
    zset::{Member, Zset, ZsetError},
};

#[derive(Debug)]
pub enum RedisError {
    WrongArgument,
    WrongType,
    StreamIdParseError,
    InvalidStreamId(String),
    WriteAofError(io::Error),
    ZsetError(ZsetError),
}

impl Display for RedisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisError::WrongArgument => write!(f, "wrong argument"),
            RedisError::WrongType => write!(f, "wrong type"),
            RedisError::StreamIdParseError => write!(f, "stream id parse error"),
            RedisError::InvalidStreamId(e) => write!(f, "{e}"),
            RedisError::WriteAofError(e) => write!(f, "{e}"),
            RedisError::ZsetError(e) => write!(f, "{e}"),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntryId {
    ms: u64,
    seq: u64,
}

impl EntryId {
    pub const ZERO: Self = Self { ms: 0, seq: 0 };
    pub const MAX: Self = Self {
        ms: u64::MAX,
        seq: u64::MAX,
    };
}

impl Display for EntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl FromStr for EntryId {
    type Err = RedisError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (ms, seq) = match s.split_once('-') {
            Some((ms, seq)) => (ms, Some(seq)),
            None => (s, None),
        };

        let ms = ms
            .parse::<u64>()
            .map_err(|_| RedisError::StreamIdParseError)?;
        let seq = match seq {
            Some(seq) => seq
                .parse::<u64>()
                .map_err(|_| RedisError::StreamIdParseError)?,
            None => u64::MAX,
        };
        Ok(EntryId { ms, seq })
    }
}

#[derive(Clone)]
pub struct StreamEntry {
    id: EntryId,
    fields: Vec<(String, String)>,
}

impl StreamEntry {
    pub fn new(id: EntryId) -> Self {
        Self {
            id: id,
            fields: Vec::new(),
        }
    }
    pub fn add(&mut self, key: String, value: String) {
        self.fields.push((key, value));
    }

    pub fn write_bulk<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let id = self.id.to_string();
        write!(w, "*2\r\n")?;
        write!(w, "${}\r\n{}\r\n", id.len(), id)?;
        write!(w, "*{}\r\n", 2 * self.fields.len())?;
        for (k, v) in &self.fields {
            write!(w, "${}\r\n{}\r\n", k.len(), k)?;
            write!(w, "${}\r\n{}\r\n", v.len(), v)?;
        }
        Ok(())
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

    fn validate(&self, entry_id: EntryId) -> Result<(), RedisError> {
        if entry_id == EntryId::ZERO {
            return Err(RedisError::InvalidStreamId(
                "The ID specified in XADD must be greater than 0-0".to_string(),
            ));
        }
        match self.entries.last() {
            Some(last) => {
                if entry_id <= last.id {
                    return Err( RedisError::InvalidStreamId("The ID specified in XADD is equal or smaller than the target stream top item".to_string()));
                }
            }
            None => {}
        }
        Ok(())
    }

    fn next_sq(&self, ms: u64) -> u64 {
        match self.entries.last() {
            Some(last) if last.id.ms == ms => last.id.seq + 1,
            Some(_) => 0,
            None if ms == 0 => 1,
            None => 0,
        }
    }
    fn generate_id(&self) -> EntryId {
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let seq = match self.entries.last() {
            Some(last) if last.id.ms == ms => last.id.seq + 1,
            _ => 0,
        };
        EntryId { ms, seq }
    }

    fn parse_entry_id(&self, s: &str) -> Result<EntryId, RedisError> {
        if s == "*" {
            return Ok(self.generate_id());
        }
        let (ms, sq) = s.split_once('-').ok_or(RedisError::StreamIdParseError)?;
        let ms = ms
            .parse::<u64>()
            .map_err(|_| RedisError::StreamIdParseError)?;
        let seq = if sq == "*" {
            self.next_sq(ms)
        } else {
            sq.parse::<u64>()
                .map_err(|_| RedisError::StreamIdParseError)?
        };
        Ok(EntryId { ms, seq })
    }

    pub fn last_id(&self) -> EntryId {
        match self.entries.last() {
            Some(entry) => entry.id,
            None => EntryId::ZERO,
        }
    }
}

pub enum Value {
    Integer(u64),
    String(String),
    List(VecDeque<String>),
    Stream(Stream),
    Zset(Zset),
}

pub struct Redis {
    store: HashMap<String, Value>,
    versions: HashMap<String, u64>,
    expire: HashMap<String, SystemTime>,
    waiters: HashMap<String, Arc<Condvar>>,
    aof_file: Option<File>,
    pubsub: PubSubRegistry,
}

impl Redis {
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
            versions: HashMap::new(),
            expire: HashMap::new(),
            waiters: HashMap::new(),
            aof_file: None,
            pubsub: PubSubRegistry::new(),
        }
    }
    pub fn set_aof(&mut self, file: Option<File>) {
        self.aof_file = file
    }
    fn incr_version(&mut self, key: &str) {
        let v = self.versions.entry(key.to_string()).or_insert(0);
        *v += 1;
    }
    pub fn get_version(&self, key: &str) -> u64 {
        self.versions.get(key).copied().unwrap_or(0)
    }
    pub fn set(&mut self, key: String, value: String, deadline: Option<SystemTime>) {
        self.incr_version(&key);
        match value.parse::<u64>() {
            Ok(i) => self.store.insert(key.clone(), Value::Integer(i)),
            Err(_) => self.store.insert(key.clone(), Value::String(value)),
        };
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
            .is_some_and(|deadline| *deadline <= SystemTime::now());
        if expired {
            self.incr_version(key);
            self.expire.remove(key);
            self.store.remove(key);
            return None;
        }
        self.store.get(key)
    }

    pub fn get(&mut self, key: &str) -> Result<Option<String>, RedisError> {
        match self.get_value(key) {
            Some(Value::String(s)) => Ok(Some(s.clone())),
            Some(Value::Integer(n)) => Ok(Some(n.to_string())),
            Some(_) => Err(RedisError::WrongType),
            None => Ok(None),
        }
    }

    pub fn get_list(&mut self, key: &str) -> Result<Option<&VecDeque<String>>, RedisError> {
        match self.get_mut_value(key) {
            Some(Value::List(l)) => Ok(Some(l)),
            Some(_) => Err(RedisError::WrongType),
            None => Ok(None),
        }
    }

    pub fn get_stream(&mut self, key: &str) -> Result<Option<&Stream>, RedisError> {
        match self.get_mut_value(key) {
            Some(Value::Stream(s)) => Ok(Some(s)),
            Some(_) => Err(RedisError::WrongType),
            None => Ok(None),
        }
    }

    fn get_mut_list(&mut self, key: &str) -> Result<Option<&mut VecDeque<String>>, RedisError> {
        match self.get_mut_value(key) {
            Some(Value::List(l)) => Ok(Some(l)),
            Some(_) => Err(RedisError::WrongType),
            None => Ok(None),
        }
    }

    fn get_mut_stream(&mut self, key: &str) -> Result<Option<&mut Stream>, RedisError> {
        match self.get_mut_value(key) {
            Some(Value::Stream(s)) => Ok(Some(s)),
            Some(_) => Err(RedisError::WrongType),
            None => Ok(None),
        }
    }

    fn get_mut_number(&mut self, key: &str) -> Result<Option<&mut u64>, RedisError> {
        match self.get_mut_value(key) {
            Some(Value::Integer(i)) => Ok(Some(i)),
            Some(_) => Err(RedisError::WrongType),
            None => Ok(None),
        }
    }

    fn get_mut_zset(&mut self, key: &str) -> Result<Option<&mut Zset>, RedisError> {
        match self.get_mut_value(key) {
            Some(Value::Zset(zset)) => Ok(Some(zset)),
            Some(_) => Err(RedisError::WrongType),
            None => Ok(None),
        }
    }
    fn get_mut_zset_or_insert(&mut self, key: &str) -> Result<&mut Zset, RedisError> {
        match self.get_mut_value_or_insert(key, || Value::Zset(Zset::new())) {
            Value::Zset(zset) => Ok(zset),
            _ => Err(RedisError::WrongType),
        }
    }

    fn get_mut_value(&mut self, key: &str) -> Option<&mut Value> {
        let expired = self
            .expire
            .get(key)
            .is_some_and(|deadline| *deadline <= SystemTime::now());
        if expired {
            self.incr_version(key);
            self.expire.remove(key);
            self.store.remove(key);
            return None;
        }
        self.store.get_mut(key)
    }

    fn get_mut_value_or_insert<F>(&mut self, key: &str, default: F) -> &mut Value
    where
        F: FnOnce() -> Value,
    {
        let expired = self
            .expire
            .get(key)
            .is_some_and(|deadline| *deadline <= SystemTime::now());
        if expired {
            self.incr_version(key);
            self.expire.remove(key);
            self.store.remove(key);
        }
        self.store.entry(key.to_string()).or_insert_with(default)
    }

    pub fn rpush(&mut self, key: &str, values: &[String]) -> Result<usize, RedisError> {
        let list = loop {
            match self.get_mut_list(key)? {
                Some(list) => break list,
                None => {
                    self.store
                        .insert(key.to_string(), Value::List(VecDeque::new()));
                }
            }
        };
        for v in values {
            list.push_back(v.clone());
        }
        let len = list.len();
        self.incr_version(key);
        Ok(len)
    }

    pub fn lpush(&mut self, key: &str, values: &[String]) -> Result<usize, RedisError> {
        let list = loop {
            match self.get_mut_list(key)? {
                Some(list) => break list,
                None => {
                    self.store
                        .insert(key.to_string(), Value::List(VecDeque::new()));
                }
            }
        };
        for v in values {
            list.push_front(v.clone());
        }
        let len = list.len();
        self.incr_version(key);
        Ok(len)
    }

    pub fn lrange(&mut self, key: &str, start: isize, end: isize) -> Vec<String> {
        let Ok(Some(l)) = self.get_list(key) else {
            // TODO: handle error
            return Vec::new();
        };
        // both indices are inclusive
        let start = normalize_idx(start, l.len());
        let end = normalize_idx(end, l.len());

        if start > end {
            return Vec::new();
        }
        if start >= l.len() {
            return Vec::new();
        }
        let end = end.min(l.len() - 1);
        l.range(start..=end).cloned().collect()
    }

    pub fn llen(&mut self, key: &str) -> usize {
        let Ok(Some(l)) = self.get_list(key) else {
            // TODO: handle error
            return 0;
        };
        l.len()
    }
    pub fn lpop(&mut self, key: &str, num: usize) -> Result<Option<Vec<String>>, RedisError> {
        match self.get_mut_value(key) {
            Some(Value::List(l)) => {
                let num = num.min(l.len());
                let res = l.drain(0..num).collect();
                self.incr_version(key);
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
        let stream = {
            loop {
                match self.get_mut_stream(key)? {
                    Some(s) => break s,
                    None => {
                        self.store
                            .insert(key.to_string(), Value::Stream(Stream::new()));
                    }
                }
            }
        };
        let entry_id = stream.parse_entry_id(id)?;
        stream.validate(entry_id)?;
        let mut entry = StreamEntry::new(entry_id);
        for chunk in args.chunks_exact(2) {
            let (k, v) = (&chunk[0], &chunk[1]);
            entry.add(k.clone(), v.clone());
        }
        stream.add(entry);
        self.incr_version(key);
        Ok(entry_id.to_string())
    }

    pub fn x_range(
        &mut self,
        key: &str,
        start_id: &str,
        end_id: &str,
    ) -> Result<Vec<StreamEntry>, RedisError> {
        let Some(stream) = self.get_mut_stream(key)? else {
            return Ok(Vec::new());
        };
        let start_id = if start_id == "-" {
            EntryId::ZERO
        } else {
            EntryId::from_str(start_id)?
        };
        let end_id = if end_id == "-" || end_id == "+" {
            EntryId::MAX
        } else {
            EntryId::from_str(end_id)?
        };
        Ok(stream
            .entries
            .iter()
            .filter(|entry| entry.id >= start_id && entry.id <= end_id)
            .cloned()
            .collect())
    }

    pub fn x_read(
        &mut self,
        pairs: &[(&String, &String)],
    ) -> Result<Vec<(String, Vec<StreamEntry>)>, RedisError> {
        let mut result = Vec::<(String, Vec<StreamEntry>)>::new();
        for (key, start_id) in pairs {
            let start_id = EntryId::from_str(start_id)?;
            let mut entries = Vec::new();
            match self.get_stream(key)? {
                Some(s) => {
                    entries.extend(s.entries.iter().filter(|e| e.id > start_id).cloned());
                }
                None => {}
            }
            result.push((key.to_string(), entries));
        }
        Ok(result)
    }

    pub fn z_add(
        &mut self,
        key: &String,
        score: f64,
        member: &String,
    ) -> Result<usize, RedisError> {
        let zset = self.get_mut_zset_or_insert(key)?;
        zset.add(score, member)
            .map_err(|e| RedisError::ZsetError(e))
    }

    pub fn z_rank(&mut self, key: &String, member: &String) -> Result<Option<usize>, RedisError> {
        let Some(zset) = self.get_mut_zset(key)? else {
            return Ok(None);
        };
        Ok(zset.rank(member))
    }

    pub fn z_range(
        &mut self,
        key: &String,
        start: isize,
        end: isize,
    ) -> Result<Vec<Member>, RedisError> {
        let Some(zset) = self.get_mut_zset(key)? else {
            return Ok(Vec::new());
        };
        Ok(zset.range(start, end))
    }

    pub fn z_card(&mut self, key: &String) -> Result<Option<usize>, RedisError> {
        let Some(zset) = self.get_mut_zset(key)? else {
            return Ok(None);
        };
        Ok(Some(zset.card()))
    }

    pub fn z_score(&mut self, key: &String, name: &String) -> Result<Option<f64>, RedisError> {
        let Some(zset) = self.get_mut_zset(key)? else {
            return Ok(None);
        };
        Ok(zset.score(name))
    }

    pub fn z_remove(&mut self, key: &String, name: &String) -> Result<bool, RedisError> {
        let Some(zset) = self.get_mut_zset(key)? else {
            return Ok(false);
        };
        Ok(zset.remove(name))
    }

    pub fn geo_add(
        &mut self,
        key: &String,
        longitude: f64,
        latitude: f64,
        location: &String,
    ) -> Result<usize, RedisError> {
        if !validate(longitude, latitude) {
            return Err(RedisError::WrongArgument); // TODO: craft more descriptive error for debugging
        }
        let score = encode(longitude, latitude) as f64;
        self.z_add(key, score, location)
    }

    pub fn geo_pos(
        &mut self,
        key: &String,
        locations: &[String],
    ) -> Result<Vec<Option<GeoPoint>>, RedisError> {
        let Some(zset) = self.get_mut_zset(key)? else {
            return Ok(vec![None]);
        };
        let mut result = Vec::with_capacity(locations.len());
        for location in locations {
            let Some(score) = zset.score(location) else {
                result.push(None);
                continue;
            };
            result.push(Some(decode(score as u64)));
        }
        Ok(result)
    }

    pub fn incr(&mut self, key: &str) -> Result<u64, RedisError> {
        match self.get_mut_number(key)? {
            Some(num) => {
                *num += 1;
                let res = *num;
                self.incr_version(key);
                Ok(res)
            }
            None => {
                self.set(key.to_string(), "1".to_string(), None);
                Ok(1)
            }
        }
    }

    pub fn remove_all_expired(&mut self) {
        let now = SystemTime::now();
        let expired: Vec<_> = self
            .expire
            .iter()
            .filter(|&(_, deadline)| deadline <= &now)
            .map(|(key, _)| key.clone())
            .collect();
        for key in expired {
            self.expire.remove(&key);
            self.store.remove(&key);
        }
    }
    pub fn get_all_keys(&mut self) -> Vec<String> {
        self.remove_all_expired();
        self.store.keys().cloned().collect()
    }
    pub fn write_aof(&mut self, data: Option<Vec<u8>>) -> io::Result<()> {
        let (Some(file), Some(data)) = (&mut self.aof_file, data) else {
            return Ok(());
        };
        file.write_all(&data)
    }
    pub fn subscribe(&mut self, channel: String, id: u64, tx: Sender<WriteMsg>) -> usize {
        self.pubsub.subscribe(channel, id, tx)
    }
    pub fn unsubscribe(&mut self, channel: &str, id: u64) -> usize {
        self.pubsub.unsubscribe(channel, id)
    }
    pub fn cleanup_subscription(&mut self, id: u64) {
        self.pubsub.cleanup(id);
    }
    pub fn publish(&mut self, channel: &str, msg: &str) -> usize {
        self.pubsub.publish(channel, msg)
    }
}

// start index is inclusive
fn normalize_idx(idx: isize, length: usize) -> usize {
    let idx = if idx < 0 { length as isize + idx } else { idx };
    idx.max(0) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lrange() {
        let mut redis = Redis::new();
        let res = redis.rpush(
            "orange",
            &[
                "grape".to_string(),
                "pineaple".to_string(),
                "mango".to_string(),
                "pear".to_string(),
                "apple".to_string(),
            ],
        );
        assert!(matches!(res, Ok(5)));
        let res = redis.lpop("orange", 4);
        assert!(res.is_ok());
        let res = res.unwrap();
        let expected = vec![
            "grape".to_string(),
            "pineaple".to_string(),
            "mango".to_string(),
            "pear".to_string(),
        ];
        assert_eq!(res, Some(expected));
        let res = redis.lrange("orange", 0, -1);
        assert_eq!(res, vec!["apple".to_string()])
    }

    #[test]
    fn test_geoadd() {
        let mut redis = Redis::new();
        let res = redis.geo_add(
            &"blueberry".to_string(),
            36.604650524882196,
            -28.48608690301177,
            &"apple".to_string(),
        );
        assert!(res.is_ok());
        let n = res.unwrap();
        assert_eq!(n, 1);
        let res = redis.z_score(&"blueberry".to_string(), &"apple".to_string()); // TODO: change to &str API
        assert!(res.is_ok());
        let score = res.unwrap();
        assert!(score.is_some());
        let score = score.unwrap();
        assert_eq!(score, 2596566547861250.0);
    }
}
