use crate::cli::{ArgRequired::True, ArgType, CmdArg, CmdArgEntry};
use crate::loader::Loader;
use crate::reader::Reader;
use crate::util::process_batch;
use crate::writer::Writer;
use clap::ArgMatches;
use std::{fs::File, io};

pub struct Extracter {
    input_file_path: String,
}

impl Extracter {
    pub fn new(matches: &ArgMatches) -> Self {
        Self {
            input_file_path: String::from(matches.value_of("input-file").unwrap()),
        }
    }

    pub fn cmd_args() -> CmdArg {
        CmdArg::new(vec![CmdArgEntry::new(
            "input-file",
            "input file path or '-' for stdin",
            "input-file",
            true,
            True,
            ArgType::String,
        )])
    }

    #[cfg(any(
        feature = "async-extracter",
        feature = "async-reader",
        feature = "async-writer",
        feature = "async-loader"
    ))]
    pub async fn forward_batches(&self, reader: Reader, writer: Writer, loader: Loader) {
        if self.input_file_path == "-" {
            let stdin = io::stdin();
            process_batch(stdin, reader, writer, loader).await;
        } else {
            let file = File::open(self.input_file_path.as_str()).unwrap();
            process_batch(file, reader, writer, loader).await;
        };
    }

    #[cfg(not(any(
        feature = "async-extracter",
        feature = "async-reader",
        feature = "async-writer",
        feature = "async-loader"
    )))]
    pub fn forward_batches(&self, reader: Reader, writer: Writer, loader: Loader) {
        if self.input_file_path == "-" {
            let stdin = io::stdin();
            process_batch(stdin, reader, writer, loader);
        } else {
            let file = File::open(self.input_file_path.as_str()).unwrap();
            process_batch(file, reader, writer, loader);
        };
    }
}
