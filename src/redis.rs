use std::collections::HashMap;

pub struct Redis {
    store: HashMap<String, String>,
}

impl Redis {
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
        }
    }
    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }
    pub fn get(&self, key: &str) -> Option<&String> {
        self.store.get(key)
    }
}
