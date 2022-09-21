mod cli;
mod error;
mod extractor;
mod loader;
mod reader;
mod util;
mod writer;

use cli::{arg_parse, command};

#[tokio::main]
async fn main() {
    let extractor_args = extractor::Extractor::cmd_args();
    let loader_args = loader::Loader::cmd_args();

    let mut cmd = command();
    cmd = arg_parse(&extractor_args, cmd);
    cmd = arg_parse(&loader_args, cmd);

    let m = cmd.get_matches();

    let extractor = extractor::Extractor::new(&m);
    let loader = loader::Loader::new(&m).await;

    let (tx, rx) = futures::channel::mpsc::unbounded();
    let extract = extractor.extract(tx);
    let load = loader.load(rx);

    tokio::spawn(extract);
    let loader_handle = tokio::spawn(load);
    loader_handle.await.unwrap().unwrap();
}
