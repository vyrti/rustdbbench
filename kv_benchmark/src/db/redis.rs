use super::KvStore;
use anyhow::Result;
use async_trait::async_trait;
use redis::{aio::MultiplexedConnection, AsyncCommands, Client};

#[derive(Clone)]
pub struct RedisStore {
    conn: MultiplexedConnection,
}

impl RedisStore {
    pub async fn new(url: &str) -> Result<Self> {
        let client = Client::open(url)?;
        Ok(Self { conn: client.get_multiplexed_async_connection().await? })
    }
}

#[async_trait]
impl KvStore for RedisStore {
    fn clone_box(&self) -> Box<dyn KvStore + Send + Sync> { Box::new(self.clone()) }

    async fn set(&self, key: String, value: Vec<u8>) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.set::<_, _, ()>(key, value).await?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut conn = self.conn.clone();
        Ok(conn.get(key).await?)
    }

    async fn lpush(&self, key: &str, value: Vec<u8>) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.lpush::<_, _, ()>(key, value).await?;
        Ok(())
    }

    async fn ltrim(&self, key: &str, start: isize, stop: isize) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.ltrim::<_, ()>(key, start, stop).await?;
        Ok(())
    }

    async fn lrange(&self, key: &str, start: isize, stop: isize) -> Result<Vec<Vec<u8>>> {
        let mut conn = self.conn.clone();
        Ok(conn.lrange(key, start, stop).await?)
    }

    async fn batch_set(&self, items: Vec<(String, Vec<u8>)>) -> Result<()> {
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        for (key, value) in items {
            pipe.set(key, value).ignore();
        }
        pipe.query_async::<()>(&mut conn).await?;
        Ok(())
    }

    async fn batch_get(&self, keys: Vec<&str>) -> Result<Vec<Vec<u8>>> {
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        for key in keys {
            pipe.get(key);
        }
        Ok(pipe.query_async(&mut conn).await?)
    }

    async fn batch_lpush_ltrim(&self, items: Vec<(&str, Vec<u8>)>, history_len: isize) -> Result<()> {
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        for (key, value) in items {
            pipe.lpush(key, value).ltrim(key, 0, history_len - 1).ignore();
        }
        pipe.query_async::<()>(&mut conn).await?;
        Ok(())
    }

    async fn batch_lrange(&self, keys: Vec<&str>, start: isize, stop: isize) -> Result<Vec<Vec<Vec<u8>>>> {
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        for key in keys {
            pipe.lrange(key, start, stop);
        }
        Ok(pipe.query_async(&mut conn).await?)
    }
}