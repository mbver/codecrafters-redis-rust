use std::{
    collections::{HashMap, VecDeque},
    time::Instant,
};

#[derive(Clone)]
pub enum Value {
    String(String),
    List(VecDeque<String>),
}

pub enum RedisError {
    WrongType,
}
pub struct Redis {
    store: HashMap<String, Value>,
    expire: HashMap<String, Instant>,
}

impl Redis {
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
            expire: HashMap::new(),
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

    fn get_value(&mut self, key: &str) -> Option<&Value> {
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
            Some(Value::String(_)) => Err(RedisError::WrongType),
            Some(Value::List(l)) => {
                for v in values {
                    l.push_back(v.clone());
                }
                Ok(l.len())
            }
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
            Some(Value::String(_)) => Err(RedisError::WrongType),
            Some(Value::List(l)) => {
                for v in values.iter().rev() {
                    l.push_front(v.clone());
                }
                Ok(l.len())
            }
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
}

fn normalize_idx(idx: isize, length: usize) -> usize {
    let idx = if idx < 0 { length as isize + idx } else { idx };
    idx.max(0) as usize
}
