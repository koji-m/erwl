use crate::cli::{
    ArgRequired::{False, True},
    CmdArg, CmdArgEntry, DefaultValue,
};
use crate::util::get_schema;
use arrow::{
    datatypes::Schema,
    error::ArrowError,
    json,
    json::reader::DecoderOptions,
    record_batch::RecordBatch,
};
use clap::ArgMatches;
use std::sync::Arc;
use std::io::Read;

pub struct Reader {
    batch_reader: Option<Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>>>>,
    schema: Schema,
    decoder_options: DecoderOptions,
}

impl Reader {
    pub fn new(matches: &ArgMatches) -> Self {
        let batch_size: usize = matches.value_of_t("batch-size").unwrap();
        let schema_file_path = String::from(matches.value_of("schema-file").unwrap());
        Self {
            batch_reader: None,
            schema: get_schema(schema_file_path).unwrap(),
            decoder_options: DecoderOptions::new().with_batch_size(batch_size),
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

    pub fn init(&mut self, extractor: Box<dyn Read>) {
        self.batch_reader = Some(Box::new(json::reader::Reader::new(extractor, Arc::new(self.schema.clone()), self.decoder_options.clone())));
    }
}

impl Iterator for Reader {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        self.batch_reader.as_mut().unwrap().next().map(|batch_res| batch_res.unwrap())
    }
}
