#[macro_use]
extern crate clap;

use kvs;

fn main() {
    let matches = clap_app!(kvstore =>
      (version: env!("CARGO_PKG_VERSION"))
      (author: env!("CARGO_PKG_AUTHORS"))
      (@subcommand get =>
       (@arg KEY: +required "Specifies which key should be fetched")
       )
      (@subcommand set =>
       (@arg KEY: +required "Specifies which key should be changed")
       (@arg VALUE: +required "Value to be saved")
       )
      (@subcommand rm =>
       (@arg KEY: +required "Specifies which key should be removed")
       )
    )
    .get_matches();

    let mut store = kvs::KvStore::new();

    if let Some(matches) = matches.subcommand_matches("get") {
        let key: String = matches.value_of("KEY").unwrap().to_string();
        panic!("unimplemented");
    } else if let Some(matches) = matches.subcommand_matches("set") {
        let key = matches.value_of("KEY").unwrap().to_string();
        let value = matches.value_of("VALUE").unwrap().to_string();

        store.set(key, value);
        panic!("unimplemented");
    } else if let Some(matches) = matches.subcommand_matches("rm") {
        let key = matches.value_of("KEY").unwrap().to_string();

        store.remove(key);
        panic!("unimplemented");
    } else {
        panic!();
    }
}
