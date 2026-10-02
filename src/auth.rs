use std::{collections::HashMap, sync::RwLock};

use sha2::{Digest, Sha256};

pub const DEFAULT_USER: &'static str = "default";

pub struct Auth {
    acl: RwLock<HashMap<String, [u8; 32]>>,
}

fn hash_pwd(pwd: &str) -> [u8; 32] {
    let digest = Sha256::digest(pwd.as_bytes());
    digest.into()
}

impl Auth {
    pub fn new() -> Self {
        Self {
            acl: RwLock::new(HashMap::new()),
        }
    }
    pub fn default_auth(&self) -> bool {
        let acl = self.acl.read().unwrap();
        acl.is_empty()
    }
    pub fn auth(&self, name: &str, pwd: &str) -> bool {
        let acl = self.acl.read().unwrap();
        let Some(current_hash) = acl.get(name) else {
            return false;
        };
        return &hash_pwd(pwd) == current_hash;
    }

    pub fn set_pwd(&mut self, name: &str, pwd: &str) {
        let mut acl = self.acl.write().unwrap();
        acl.insert(name.to_string(), hash_pwd(pwd));
    }
}
