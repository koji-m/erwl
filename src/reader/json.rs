use crate::cli::{
    ArgRequired::{False, True},
    CmdArg, CmdArgEntry, DefaultValue,
};
use crate::util::get_schema;
use arrow::{
    datatypes::Schema, error::ArrowError, json, json::reader::DecoderOptions,
    record_batch::RecordBatch,
};
use clap::ArgMatches;
use futures::stream::{iter, BoxStream};
use std::io::Read;
use std::sync::Arc;


#[derive(Clone)]
pub struct Reader {
    schema: Schema,
    decoder_options: DecoderOptions,
}

impl Reader {
    pub fn new(matches: &ArgMatches) -> Self {
        let batch_size: usize = matches.value_of_t("batch-size").unwrap();
        let schema_file_path = String::from(matches.value_of("schema-file").unwrap());
        Self {
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

    pub fn stream(&self, file: Box<dyn Read + Send>) -> BoxStream<'static, Result<RecordBatch, ArrowError>> {
        // async json reader not supported yet: https://github.com/apache/arrow-rs/issues/78
        Box::pin(iter(json::reader::Reader::new(
            file,
            Arc::new(self.schema.clone()),
            self.decoder_options.clone(),
        )))
    }
}
