use failure::Fail;

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

pub type Result<T> = std::result::Result<T, KvError>;
