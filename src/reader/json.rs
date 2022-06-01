use crate::cli::{
    ArgRequired::{False, True},
    CmdArg, CmdArgEntry,
    DefaultValue,
};
use crate::extractor::Extractor;
use crate::util::get_schema;
use arrow::{
    error::ArrowError,
    json,
    json::reader::DecoderOptions,
    record_batch::RecordBatch,
};
use clap::ArgMatches;
use std::sync::Arc;

pub struct Reader {
    batch_reader: Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>>>,
}

impl Reader {
    pub fn new(matches: &ArgMatches, extractor: Extractor) -> Self {
        let batch_size: usize = matches.value_of_t("batch-size").unwrap();
        let batch_extractor = extractor.batch_extractor();
        let schema_file_path = String::from(matches.value_of("schema-file").unwrap());
        let schema = get_schema(schema_file_path).unwrap();
        let decoder_options = DecoderOptions::new().with_batch_size(batch_size);
        let batch_reader =
            json::reader::Reader::new(batch_extractor, Arc::new(schema), decoder_options);
        Self {
            batch_reader: Box::new(batch_reader),
        }
    }

    pub fn cmd_args() -> CmdArg {
        CmdArg::new(vec![
            CmdArgEntry::new(
                "schema-file",
                "Schema file (BigQuery schema)",
                "schema",
                true,
                True,
            ),
            CmdArgEntry::new(
                "batch-size",
                "number of records in each files",
                "batch-size",
                true,
                False(DefaultValue::String(String::from("10000"))),
            ),
        ])
    }
}

impl Iterator for Reader {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(batch_res) = self.batch_reader.next() {
            Some(batch_res.unwrap())
        } else {
            None
        }
    }
}
