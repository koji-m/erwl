use crate::cli::{ArgRequired::True, CmdArg, CmdArgEntry};
use clap::ArgMatches;
use std::{fs::File, io, io::Read};

pub struct Extractor {
    input_file_path: String,
}

impl Extractor {
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
        )])
    }

    pub fn batch_extractor(&self) -> Box<dyn Read> {
        if self.input_file_path == "-" {
            Box::new(io::stdin())
        } else {
            Box::new(File::open(self.input_file_path.as_str()).unwrap())
        }
    }
}
