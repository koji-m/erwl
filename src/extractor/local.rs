use arrow::record_batch::RecordBatch;
use crate::cli::{ArgRequired::True, CmdArg, CmdArgEntry};
use crate::reader::Reader;
use clap::ArgMatches;
use std::{fs::File, io, io::Read};

pub struct Extractor {
    input_file_paths: Vec<String>,
    reader: Reader,
}

impl Extractor {
    pub fn new(matches: &ArgMatches, mut reader: Reader) -> Self {
        let mut input_file_paths = vec![String::from(matches.value_of("input-files").unwrap())];
        let extractor = Self::next_extractor(&mut input_file_paths).unwrap();
        reader.init(extractor);
        Self {
            input_file_paths,
            reader,
        }
    }

    pub fn cmd_args() -> CmdArg {
        CmdArg::new(vec![CmdArgEntry::new(
            "input-files",
            "input file paths or '-' for stdin",
            "input-files",
            true,
            True,
        )])
    }

    fn next_extractor(paths: &mut Vec<String>) -> Option<Box<dyn Read>> {
        if let Some(next_file) = paths.pop() {
            if next_file == "-" {
                Some(Box::new(io::stdin()))
            } else {
                Some(Box::new(File::open(next_file.as_str()).unwrap()))
            }
        } else {
            None
        }
    }

    pub fn batch_extractor(&mut self) -> Option<Box<dyn Read>> {
        Self::next_extractor(&mut self.input_file_paths)
    }
}

impl Iterator for Extractor {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(batch) = self.reader.next() {
            return Some(batch);
        } else {
            loop {
                if let Some(extractor) = self.batch_extractor() {
                    self.reader.init(extractor);
                    if let Some(batch) = self.reader.next() {
                        return Some(batch);
                    } else {
                        continue;
                    }
                } else {
                    return None;
                }
            }
        }
    }
}