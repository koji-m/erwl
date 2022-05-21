mod cli;
mod config;
mod extracter;
mod reader;
mod writer;

use cli::{arg_parse, command};
use config::Conf;
use extracter::Extracter;
use reader::Reader;
use writer::Writer;

fn main() {
    let mut extracter = Extracter::new();
    let mut reader = Reader::new();
    let mut writer = Writer::new();

    let mut cmd = command();
    cmd = arg_parse(&mut extracter, cmd);
    cmd = arg_parse(&mut reader, cmd);
    cmd = arg_parse(&mut writer, cmd);

    let m = cmd.get_matches();

    reader.configure(&m);
    writer.configure(&m);

    println!("input-file: {}", extracter.input_file().as_ref().unwrap());
    println!(
        "schema-file-path: {}",
        reader.schema_file_path().as_ref().unwrap()
    );
    println!("batch-size: {}", reader.batch_size().unwrap());
    println!("compression: {}", writer.compression().as_ref().unwrap());
}
