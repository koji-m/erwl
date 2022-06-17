mod cli;
mod error;
mod extractor;
mod loader;
mod reader;
mod util;
mod writer;

use cli::{arg_parse, command};
use error::LoadError;
use extractor::Extractor;
use loader::Loader;
use reader::Reader;
use writer::Writer;

#[tokio::main]
async fn main() -> Result<(), LoadError> {
    let extractor_args = Extractor::cmd_args();
    let reader_args = Reader::cmd_args();
    let writer_args = Writer::cmd_args();
    let loader_args = Loader::cmd_args();

    let mut cmd = command();
    cmd = arg_parse(&extractor_args, cmd);
    cmd = arg_parse(&reader_args, cmd);
    cmd = arg_parse(&writer_args, cmd);
    cmd = arg_parse(&loader_args, cmd);

    let m = cmd.get_matches();

    #[cfg(feature = "async-reader")]
    let reader = Reader::new(&m).await;
    #[cfg(not(feature = "async-reader"))]
    let reader = Reader::new(&m);
    #[cfg(feature = "async-extractor")]
    let extractor = Extractor::new(&m, reader).await;
    #[cfg(not(feature = "async-extractor"))]
    let extractor = Extractor::new(&m, reader);
    
    #[cfg(feature = "async-writer")]
    let writer = Writer::new(&m).await;
    #[cfg(not(feature = "async-writer"))]
    let writer = Writer::new(&m);
    #[cfg(feature = "async-loader")]
    let mut loader = Loader::new(&m, writer, extractor).await;
    #[cfg(not(feature = "async-loader"))]
    let mut loader = Loader::new(&m, writer, extractor);

    #[cfg(feature = "async-loader")]
    loader.load().await?;
    #[cfg(not(feature = "async-loader"))]
    loader.load().unwrap();

    Ok(())
}
