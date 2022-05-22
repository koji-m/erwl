mod cli;
mod extracter;
mod loader;
mod reader;
mod writer;

use cli::{arg_parse, command};
use extracter::Extracter;
use loader::Loader;
use reader::Reader;
use writer::Writer;

#[tokio::main]
async fn main() {
    let extracter_args = Extracter::cmd_args();
    let reader_args = Reader::cmd_args();
    let writer_args = Writer::cmd_args();
    let loader_args = Loader::cmd_args();

    let mut cmd = command();
    cmd = arg_parse(&extracter_args, cmd);
    cmd = arg_parse(&reader_args, cmd);
    cmd = arg_parse(&writer_args, cmd);
    cmd = arg_parse(&loader_args, cmd);

    let m = cmd.get_matches();

    let extracter = Extracter::new(&m);
    let reader = Reader::new(&m);
    let writer = Writer::new(&m);
    let loader = Loader::new(&m).await;

    extracter.forward_batches(reader, writer, loader).await;
}
