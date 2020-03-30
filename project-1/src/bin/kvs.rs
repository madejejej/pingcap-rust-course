use kvs::{self, Result};
use std::path::Path;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvstore", about = "Simple key-value store")]
enum Opt {
    Get { key: String },
    Set { key: String, value: String },
    Rm { key: String },
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    let mut kvstore = kvs::KvStore::open(Path::new("."))?;

    match opt {
        Opt::Get { key } => {
            let value = kvstore.get(key)?;

            match value {
                Some(val) => println!("{}", val),
                None => println!("key not found"),
            };
        }
        Opt::Set { key, value } => {
            kvstore.set(key, value)?;
        }
        Opt::Rm { key } => {
            kvstore.remove(key)?;
        }
    };

    Ok(())
}
