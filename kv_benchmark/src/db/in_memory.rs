use super::KvStore;
use anyhow::Result;
use async_trait::async_trait;
use rustc_hash::{FxHashMap, FxHasher};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

const IN_MEMORY_SHARDS: usize = 128;
type Shard = FxHashMap<String, Vec<Vec<u8>>>;

#[derive(Clone)]
pub struct InMemoryStore {
    shards: Arc<Vec<Mutex<Shard>>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(IN_MEMORY_SHARDS);
        for _ in 0..IN_MEMORY_SHARDS {
            shards.push(Mutex::new(FxHashMap::default()));
        }
        Self { shards: Arc::new(shards) }
    }

    fn get_shard_index<K: Hash>(&self, key: &K) -> usize {
        let mut hasher = FxHasher::default();
        key.hash(&mut hasher);
        hasher.finish() as usize % IN_MEMORY_SHARDS
    }
}

#[async_trait]
impl KvStore for InMemoryStore {
    fn clone_box(&self) -> Box<dyn KvStore + Send + Sync> { Box::new(self.clone()) }

    async fn set(&self, key: String, value: Vec<u8>) -> Result<()> {
        let index = self.get_shard_index(&key);
        let mut shard = self.shards[index].lock().unwrap();
        shard.insert(key, vec![value]);
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let index = self.get_shard_index(&key);
        let shard = self.shards[index].lock().unwrap();
        Ok(shard.get(key).and_then(|v| v.first().cloned()))
    }

    async fn lpush(&self, key: &str, value: Vec<u8>) -> Result<()> {
        let index = self.get_shard_index(&key);
        let mut shard = self.shards[index].lock().unwrap();
        let list = shard.entry(key.to_string()).or_default();
        list.insert(0, value);
        Ok(())
    }

    async fn ltrim(&self, key: &str, start: isize, stop: isize) -> Result<()> {
        let index = self.get_shard_index(&key);
        let mut shard = self.shards[index].lock().unwrap();
        if let Some(list) = shard.get_mut(key) {
            if start == 0 && stop >= 0 {
                let current_len = list.len();
                let effective_stop_idx = (stop + 1) as usize;
                if effective_stop_idx < current_len {
                    list.truncate(effective_stop_idx);
                }
            }
        }
        Ok(())
    }

    async fn lrange(&self, key: &str, start: isize, stop: isize) -> Result<Vec<Vec<u8>>> {
        let index = self.get_shard_index(&key);
        let shard = self.shards[index].lock().unwrap();
        if let Some(list) = shard.get(key) {
            if start == 0 && stop >= 0 {
                let end = (stop + 1).min(list.len() as isize) as usize;
                return Ok(list[..end].to_vec());
            }
        }
        Ok(vec![])
    }

    async fn batch_set(&self, items: Vec<(String, Vec<u8>)>) -> Result<()> {
        for (key, value) in items { self.set(key, value).await?; }
        Ok(())
    }

    async fn batch_get(&self, keys: Vec<&str>) -> Result<Vec<Vec<u8>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key).await?.unwrap_or_default());
        }
        Ok(results)
    }

    async fn batch_lpush_ltrim(&self, items: Vec<(&str, Vec<u8>)>, history_len: isize) -> Result<()> {
        for (key, value) in items {
            self.lpush(key, value).await?;
            self.ltrim(key, 0, history_len - 1).await?;
        }
        Ok(())
    }

    async fn batch_lrange(&self, keys: Vec<&str>, start: isize, stop: isize) -> Result<Vec<Vec<Vec<u8>>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.lrange(key, start, stop).await?);
        }
        Ok(results)
    }
}