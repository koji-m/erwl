use crate::cli::{ArgRequired::True, ArgType, CmdArg, CmdArgEntry};
use crate::loader::Loader;
use crate::reader::Reader;
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

    pub async fn forward_batches(&self, reader: Reader, writer: Writer, loader: Loader) {
        if self.input_file_path == "-" {
            let stdin = io::stdin();
            let batch_reader = reader.batch_reader(stdin.lock());
            for (i, batch_) in batch_reader.enumerate() {
                let cursor = writer.write(batch_.unwrap());
                loader.load(cursor.into_inner().unwrap(), i).await;
            }
        } else {
            let file = File::open(self.input_file_path.as_str()).unwrap();
            let batch_reader = reader.batch_reader(file);
            for (i, batch_) in batch_reader.enumerate() {
                let cursor = writer.write(batch_.unwrap());
                loader.load(cursor.into_inner().unwrap(), i).await;
            }
        };
    }
}
