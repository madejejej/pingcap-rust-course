use std::collections::HashMap;
use std::path::Path;
use std::error;

pub struct KvStore {
    map: HashMap<String, String>,
}

pub type Result<T> = std::result::Result<T, Box<dyn error::Error>>;

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
