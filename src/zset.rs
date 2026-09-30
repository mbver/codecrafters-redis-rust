use std::{collections::HashMap, fmt::Display, mem};

use crate::{
    skiplist::{self, SkipList},
    zset,
};

#[derive(Clone)]
pub struct Member {
    name: String,
    score: f64,
}

impl PartialEq for Member {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.name == other.name
    }
}

impl Eq for Member {}

impl PartialOrd for Member {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Member {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.name.cmp(&other.name))
    }
}

pub struct Zset {
    dict: HashMap<String, f64>,
    skiplist: SkipList<Member>,
}

#[derive(Debug)]
pub enum ZsetError {
    InvalidScore,
}

impl Display for ZsetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ZsetError::InvalidScore => write!(f, "invalid score"),
        }
    }
}

impl Zset {
    pub fn new() -> Self {
        Self {
            dict: HashMap::new(),
            skiplist: SkipList::new(),
        }
    }
    pub fn add(&mut self, score: f64, member: &str) -> Result<usize, ZsetError> {
        if score.is_nan() {
            return Err(ZsetError::InvalidScore);
        }
        let prev_len = self.skiplist.len();
        if let Some(old_score) = self.dict.get(member) {
            if old_score == &score {
                return Ok(0); // item already exists and unchanged
            }
            self.skiplist.remove(&Member {
                name: member.to_string(),
                score: *old_score,
            });
        }
        self.dict.insert(member.to_string(), score);
        self.skiplist.insert(Member {
            name: member.to_string(),
            score,
        });
        Ok(self.skiplist.len() - prev_len)
    }

    pub fn rank(&self, member: &str) -> Option<usize> {
        let Some(score) = self.dict.get(member) else {
            return None;
        };

        self.skiplist.rank(&Member {
            name: member.to_string(),
            score: *score,
        })
    }
}
