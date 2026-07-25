use std::{collections::HashMap, time::Instant};

pub struct Redis {
    store: HashMap<String, String>,
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
        self.store.insert(key.clone(), value);
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
        self.store.get(key)
    }
}
