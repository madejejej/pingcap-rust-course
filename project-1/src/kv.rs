use crate::{KvError, Result};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAX_SEGMENT_SIZE: u64 = 1024 * 1024; // 1 MB

pub struct KvStore {
    map: HashMap<String, CommandPosition>,
    writer: BufWriter<File>,
    current_generation: usize,
    readers: Vec<BufReader<File>>,
    path: PathBuf,
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
            Some(CommandPosition {
                offset,
                length,
                generation,
            }) => {
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
        let mut offset = self.writer.seek(SeekFrom::Current(0))?;

        if length + offset > MAX_SEGMENT_SIZE {
            self.current_generation += 1;
            let (reader, writer) = create_new_segment(&self.path, self.current_generation)?;
            self.readers.push(reader);
            self.writer = writer;
            offset = self.writer.seek(SeekFrom::Current(0))?;
        }

        let generation = self.current_generation;

        // TODO: handle partial writes and write errors
        self.writer.write(&bytes)?;
        self.writer.flush()?;
        self.map.insert(
            key,
            CommandPosition {
                offset,
                length,
                generation,
            },
        );
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
        let mut current_generation = 1;
        let mut map: HashMap<String, CommandPosition> = HashMap::new();
        let mut readers: Vec<BufReader<File>> = vec![];

        loop {
            let file_path = path.join(format!("{}.log", current_generation));

            if !file_path.exists() {
                break;
            }

            let read_file = File::open(file_path)?;

            if read_file.metadata()?.len() == 0 {
              break;
            }

            let mut reader = BufReader::with_capacity(512 * 1024, read_file);
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
                            generation: current_generation,
                        },
                    ),
                    Operation::Remove { key } => map.remove(&key),
                };

                pos = new_pos;
            }

            readers.push(reader);
            current_generation += 1;
        }

        let (reader, writer) = create_new_segment(&PathBuf::from(path), current_generation)?;
        readers.push(reader);

        let store = KvStore {
            map,
            writer,
            current_generation,
            readers,
            path: PathBuf::from(path),
        };

        Ok(store)
    }
}


fn create_new_segment(path: &PathBuf, generation: usize) -> Result<(BufReader<File>, BufWriter<File>)> {
    let path = path.join(format!("{}.log", generation));
    let write_file = OpenOptions::new()
        .append(true)
        .create(true)
        .write(true)
        .open(&path)?;

    let read_file = File::open(path)?;

    let mut writer = BufWriter::with_capacity(512 * 1024, write_file);
    writer.seek(SeekFrom::End(0))?;
    let reader = BufReader::with_capacity(512 * 1024, read_file);

    Ok((reader, writer))
}
