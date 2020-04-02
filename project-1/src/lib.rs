#[macro_use]
extern crate log;
extern crate env_logger;

use failure::Fail;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub struct KvStore {
    map: HashMap<String, CommandPosition>,
    writer: BufWriter<File>,
    reader: BufReader<File>,
}

#[derive(Debug)]
struct CommandPosition {
    offset: u64,
    length: u64,
}

pub type Result<T> = std::result::Result<T, KvError>;

#[derive(Debug, Serialize, Deserialize)]
enum Operation {
    Set { key: String, value: String },
    Remove { key: String },
}

#[derive(Debug, Fail)]
pub enum KvError {
    #[fail(display = "{}", _0)]
    Io(#[cause] std::io::Error),
    #[fail(display = "{}", _0)]
    Serialization(#[cause] serde_json::Error),
    #[fail(display = "Key not found {}", key)]
    KeyNotFound { key: String },
}

impl From<std::io::Error> for KvError {
    fn from(err: std::io::Error) -> KvError {
        KvError::Io(err)
    }
}

impl From<serde_json::Error> for KvError {
    fn from(err: serde_json::Error) -> KvError {
        KvError::Serialization(err)
    }
}

impl KvStore {
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.map.get(&key) {
            Some(CommandPosition { offset, length }) => {
                self.reader.seek(SeekFrom::Start(*offset))?;
                let mut buffer = vec![0; *length as usize];
                self.reader.read_exact(&mut buffer)?;
                let operation: Operation = serde_json::from_slice(&buffer)?;

                match operation {
                    Operation::Set { value, .. } => Ok(Some(value)),
                    Operation::Remove { .. } => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let op = Operation::Set {
            key: key.clone(),
            value,
        };
        let bytes = serde_json::to_vec(&op)?;

        let length = bytes.len() as u64;
        let offset = self.writer.seek(SeekFrom::Current(0))?;

        // TODO: handle partial writes and write errors
        self.writer.write(&bytes)?;
        self.writer.flush()?;
        self.map.insert(key, CommandPosition { offset, length });
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.map.contains_key(&key) {
            let op = Operation::Remove { key: key.clone() };

            let bytes = serde_json::to_vec(&op)?;
            self.writer.write(&bytes)?;
            self.writer.flush()?;
            self.map.remove(&key).ok_or(KvError::KeyNotFound { key })?;
            Ok(())
        } else {
            Err(KvError::KeyNotFound { key: key })
        }
    }

    pub fn open(path: &Path) -> Result<KvStore> {
        let path = path.join("1.log");
        let write_file = OpenOptions::new()
            .append(true)
            .create(true)
            .write(true)
            .open(&path)?;

        let read_file = File::open(path)?;

        let mut writer = BufWriter::with_capacity(512 * 1024, write_file);
        writer.seek(SeekFrom::End(0))?;
        let mut reader = BufReader::with_capacity(512 * 1024, read_file);
        let mut map: HashMap<String, CommandPosition> = HashMap::new();
        let mut pos: u64 = reader.seek(SeekFrom::Start(0))?;
        let mut stream =
            serde_json::Deserializer::from_reader(&mut reader).into_iter::<Operation>();

        while let Some(operation) = stream.next() {
            let new_pos = stream.byte_offset() as u64;

            match operation? {
                Operation::Set { key, value: _ } => map.insert(
                    key,
                    CommandPosition {
                        offset: pos,
                        length: new_pos - pos,
                    },
                ),
                Operation::Remove { key } => map.remove(&key),
            };

            pos = new_pos;
        }

        debug!("Opened file: {:?}", map);

        let store = KvStore {
            map,
            writer,
            reader,
        };

        Ok(store)
    }
}
