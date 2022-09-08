use crate::cli::{ArgRequired, CmdArg, CmdArgEntry};
use crate::error::GenericError;
use crate::reader;
use arrow::record_batch::RecordBatch;
use clap::ArgMatches;
use futures::channel::mpsc;
use futures::stream::{self, StreamExt};
use futures::SinkExt;
use std::{fs::File, io, io::Read};
use std::future::Future;

pub struct Extractor {
    file_paths: Vec<String>,
    reader: reader::Reader,
}

impl Extractor {
    pub async fn new(matches: &ArgMatches) -> Self {
        let paths = vec![String::from(matches.value_of("input-files").unwrap())];
        let rdr = reader::Reader::new(&matches);
        Self {
            file_paths: paths,
            reader: rdr,
        }
    }

    pub fn cmd_args() -> CmdArg {
        let mut arg_entries = vec![];
        arg_entries.push(CmdArgEntry::new(
            "input-files",
            "input file paths or '-' for stdin",
            "input-files",
            true,
            ArgRequired::True,
        ));
        arg_entries.extend_from_slice(&reader::Reader::cmd_args().entries());
        CmdArg::new(arg_entries)
    }

    pub fn file_paths(&self) -> Vec<String> {
        self.file_paths.clone()
    }

    pub fn reader(&self) -> reader::Reader {
        self.reader.clone()
    }

    fn get_file(path: &str) -> Box<dyn Read + Send> {
        if path == "-" {
            Box::new(io::stdin())
        } else {
            Box::new(File::open(path).unwrap())
        }
    }

    pub fn extract(&self, mut tx: mpsc::UnboundedSender<RecordBatch>) -> impl Future<Output = Result<(), GenericError>> {
        let rdr = self.reader();
        let file_paths = self.file_paths();
        // ToDo: check if len(file_paths) > 1 and file_paths not contain '-'
        let mut file_paths_stream = stream::iter(file_paths);

        async move {
            while let Some(path) = file_paths_stream.next().await {
                let file = Self::get_file(&path);
                let mut reader_stream = rdr.stream(file);
                while let Some(res) = reader_stream.next().await {
                    if let Ok(rec) = res {
                        if let Err(_) = tx.send(rec).await {
                            return Err(GenericError { message: String::from("mpsc send error") })
                        }
                    } else {
                        return Err(GenericError { message: String::from("read stream error") })
                    }
                }
            }
            Ok(())
        }
    }
}
