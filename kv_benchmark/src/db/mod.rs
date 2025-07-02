use anyhow::Result;
use async_trait::async_trait;

pub mod in_memory;
pub mod redis;

#[async_trait]
pub trait KvStore: Send + Sync {
    fn clone_box(&self) -> Box<dyn KvStore + Send + Sync>;
    async fn set(&self, key: String, value: Vec<u8>) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn lpush(&self, key: &str, value: Vec<u8>) -> Result<()>;
    async fn ltrim(&self, key: &str, start: isize, stop: isize) -> Result<()>;
    async fn lrange(&self, key: &str, start: isize, stop: isize) -> Result<Vec<Vec<u8>>>;
    async fn batch_set(&self, items: Vec<(String, Vec<u8>)>) -> Result<()>;
    async fn batch_get(&self, keys: Vec<&str>) -> Result<Vec<Vec<u8>>>;
    async fn batch_lpush_ltrim(&self, items: Vec<(&str, Vec<u8>)>, history_len: isize) -> Result<()>;
    async fn batch_lrange(&self, keys: Vec<&str>, start: isize, stop: isize) -> Result<Vec<Vec<Vec<u8>>>>;
}

impl Clone for Box<dyn KvStore + Send + Sync> {
    fn clone(&self) -> Box<dyn KvStore + Send + Sync> {
        self.clone_box()
    }
}