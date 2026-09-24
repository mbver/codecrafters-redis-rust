use std::{
    collections::{HashMap, HashSet},
    sync::mpsc::Sender,
};

pub type WriteMsg = Vec<u8>;

pub struct PubSubRegistry {
    subscriptions: HashMap<String, HashMap<u64, Sender<WriteMsg>>>,
    client_channels: HashMap<u64, HashSet<String>>, // use for quick search in unsubscribe handling
}

impl PubSubRegistry {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            client_channels: HashMap::new(),
        }
    }
    pub fn subscribe(&mut self, channel: String, id: u64, tx: Sender<WriteMsg>) -> usize {
        let subscribers = self.subscriptions.entry(channel.clone()).or_default();
        subscribers.insert(id, tx);

        let channels = self.client_channels.entry(id).or_default();
        channels.insert(channel);
        channels.len()
    }
    pub fn unsubscribe(&mut self, channel: &str, id: u64) -> usize {
        if let Some(channels) = self.client_channels.get_mut(&id) {
            channels.remove(channel);
            if channels.is_empty() {
                self.client_channels.remove(&id);
            }
        }

        let Some(subscribers) = self.subscriptions.get_mut(channel) else {
            return 0;
        };
        subscribers.remove(&id);
        let remaining = subscribers.len();
        if remaining == 0 {
            self.subscriptions.remove(channel);
        }
        remaining
    }
    pub fn cleanup(&mut self, id: u64) {
        let Some(channels) = self.client_channels.get(&id) else {
            return;
        };
        for ch in channels {
            if let Some(subscribers) = self.subscriptions.get_mut(ch) {
                subscribers.remove(&id);
                if subscribers.len() == 0 {
                    self.subscriptions.remove(ch);
                }
            }
        }
        self.client_channels.remove(&id);
    }
    pub fn publish(&mut self, channel: &str, msg: &str) -> usize {
        let Some(subscribers) = self.subscriptions.get(channel) else {
            return 0;
        };

        subscribers.len()
    }
}
