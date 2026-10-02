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

struct AuthFlags {
    nopass: bool,
}

impl AuthFlags {
    fn enabled(&self) -> Vec<String> {
        let mut res = Vec::new();
        if self.nopass {
            res.push("nopass".to_string());
        }
        res
    }
}

pub struct User {
    name: String,
    flags: AuthFlags,
}

impl User {
    pub fn new(name: String) -> Self {
        Self {
            name,
            flags: AuthFlags { nopass: false },
        }
    }
    pub fn set_nopass(&mut self) {
        self.flags.nopass = true
    }
    pub fn name(&self) -> String {
        self.name.clone()
    }
    pub fn enabled_flags(&self) -> Vec<String> {
        self.flags.enabled()
    }
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
