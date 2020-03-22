use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "kvstore", about = "Simple key-value store")]
enum Opt {
    Get { key: String },
    Set { key: String, value: String },
    Rm { key: String },
}

fn main() {
    let opt = Opt::from_args();

    match opt {
        Opt::Get { key } => panic!("unimplemented"),
        Opt::Set { key, value } => panic!("unimplemented"),
        Opt::Rm { key } => panic!("unimplemented"),
        _ => panic!(),
    }
}
