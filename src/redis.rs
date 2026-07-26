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
    pub fn get(&mut self, key: &str) -> Option<&String> {
        let expired = self
            .expire
            .get(key)
            .is_some_and(|deadline| *deadline <= Instant::now());
        if expired {
            self.expire.remove(key);
            self.store.remove(key);
            return None;
        }
        let Some(Value::String(s)) = self.store.get(key) else {
            return None;
        };
        Some(s)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
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

    pub fn rpush(&mut self, key: &str, value: String) -> Result<usize, RedisError> {
        match self.get_mut(key) {
            Some(Value::String(_)) => Err(RedisError::WrongType),
            Some(Value::List(l)) => {
                l.push_back(value);
                Ok(l.len())
            }
            None => {
                let mut l = VecDeque::new();
                l.push_back(value);
                self.store.insert(key.to_string(), Value::List(l));
                Ok(1)
            }
        }
    }
}
