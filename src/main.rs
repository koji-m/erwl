mod cli;
mod config;
mod extracter;
mod loader;
mod reader;
mod writer;

use cli::{arg_parse, command};
use config::Conf;
use extracter::Extracter;
use loader::Loader;
use reader::Reader;
use writer::Writer;

fn main() {
    let mut extracter = Extracter::new();
    let mut reader = Reader::new();
    let mut writer = Writer::new();
    let mut loader = Loader::new();

    let mut cmd = command();
    cmd = arg_parse(&mut extracter, cmd);
    cmd = arg_parse(&mut reader, cmd);
    cmd = arg_parse(&mut writer, cmd);
    cmd = arg_parse(&mut loader, cmd);

    let m = cmd.get_matches();

    extracter.configure(&m);
    reader.configure(&m);
    writer.configure(&m);
    loader.configure(&m);

    println!("input-file: {}", extracter.input_file().as_ref().unwrap());
    println!(
        "schema-file-path: {}",
        reader.schema_file_path().as_ref().unwrap()
    );
    println!("batch-size: {}", reader.batch_size().unwrap());
    println!("compression: {}", writer.compression().as_ref().unwrap());
    println!("s3-bucket: {}", loader.bucket().as_ref().unwrap());
    println!("key-prefix: {}", loader.key_prefix().as_ref().unwrap());
}
