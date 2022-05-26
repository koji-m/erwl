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

    #[cfg(feature = "async-extracter")]
    let extracter = Extracter::new(&m).await;
    #[cfg(not(feature = "async-extracter"))]
    let extracter = Extracter::new(&m);
    #[cfg(feature = "async-reader")]
    let reader = Reader::new(&m, extracter).await;
    #[cfg(not(feature = "async-reader"))]
    let reader = Reader::new(&m, extracter);
    #[cfg(feature = "async-writer")]
    let writer = Writer::new(&m, reader).await;
    #[cfg(not(feature = "async-writer"))]
    let writer = Writer::new(&m, reader);
    #[cfg(feature = "async-loader")]
    let mut loader = Loader::new(&m, writer).await;
    #[cfg(not(feature = "async-loader"))]
    let mut loader = Loader::new(&m, writer);

    #[cfg(feature = "async-loader")]
    loader.load().await;
    #[cfg(not(feature = "async-loader"))]
    loader.load()

    /*
    #[cfg(any(
        feature = "async-extracter",
        feature = "async-reader",
        feature = "async-writer",
        feature = "async-loader"
    ))]
    extracter.forward_batches(reader, writer, loader).await;
    #[cfg(not(any(
        feature = "async-extracter",
        feature = "async-reader",
        feature = "async-writer",
        feature = "async-loader"
    )))]
    extracter.forward_batches(reader, writer, loader);
    */
}
