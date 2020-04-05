use crate::{KvError, Result};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub struct KvStore {
    map: HashMap<String, CommandPosition>,
    writer: BufWriter<File>,
    current_generation: usize,
    readers: Vec<BufReader<File>>,
}

#[derive(Debug)]
struct CommandPosition {
    offset: u64,
    length: u64,
    generation: usize,
}

#[derive(Debug, Serialize, Deserialize)]
enum Operation {
    Set { key: String, value: String },
    Remove { key: String },
}

impl KvStore {
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.map.get(&key) {
            Some(CommandPosition { offset, length, generation }) => {
                let reader = &mut self.readers[*generation - 1];

                reader.seek(SeekFrom::Start(*offset))?;
                let mut buffer = vec![0; *length as usize];
                reader.read_exact(&mut buffer)?;
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
        let generation = self.current_generation;

        // TODO: handle partial writes and write errors
        self.writer.write(&bytes)?;
        self.writer.flush()?;
        self.map.insert(key, CommandPosition { offset, length, generation });
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

        let current_generation = 1;

        while let Some(operation) = stream.next() {
            let new_pos = stream.byte_offset() as u64;

            match operation? {
                Operation::Set { key, value: _ } => map.insert(
                    key,
                    CommandPosition {
                        offset: pos,
                        length: new_pos - pos,
                        generation: current_generation,
                    },
                ),
                Operation::Remove { key } => map.remove(&key),
            };

            pos = new_pos;
        }

        let readers = vec![reader];

        let store = KvStore {
            map,
            writer,
            current_generation,
            readers,
        };

        Ok(store)
    }
}
