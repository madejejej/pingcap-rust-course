extern crate failure;

#[macro_use] extern crate failure_derive;

use std::collections::HashMap;
use std::path::Path;

pub struct KvStore {
    map: HashMap<String, String>,
}

#[derive(Debug, Fail)]
pub enum Error {
  #[fail(display = "test")]
  Test {}
}

pub type Result<T> = std::result::Result<T, Error>;

impl KvStore {
    pub fn new() -> KvStore {
        let map: HashMap<String, String> = HashMap::new();

        KvStore { map }
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.map.get(&key) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.map.insert(key, value);
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.map.remove(&key);
        Ok(())
    }

    pub fn open(path: &Path) -> Result<KvStore> {
        let map: HashMap<String, String> = HashMap::new();
        Ok(KvStore { map })
    }
}
