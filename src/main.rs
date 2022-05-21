mod cli;
mod config;
mod reader;
mod writer;

use cli::{arg_parse, command};
use config::Conf;
use reader::Reader;
use writer::Writer;

fn main() {
    let mut reader = Reader::new();
    let mut writer = Writer::new();

    let mut cmd = command();
    cmd = arg_parse(&mut reader, cmd);
    cmd = arg_parse(&mut writer, cmd);

    let m = cmd.get_matches();

    reader.configure(&m);
    writer.configure(&m);

    println!(
        "schema-file-path: {}",
        reader.schema_file_path().as_ref().unwrap()
    );
    println!("batch-size: {}", reader.batch_size().unwrap());
    println!("compression: {}", writer.compression().as_ref().unwrap());
}
