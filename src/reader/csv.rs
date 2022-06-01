use crate::cli::{
    ArgRequired::{False, True},
    CmdArg, CmdArgEntry, DefaultValue,
};
use crate::extractor::Extractor;
use crate::util::get_schema;
use arrow::{error::ArrowError, record_batch::RecordBatch};
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
        let header = matches.is_present("input-csv-header");
        let delimiter = if let Some(d) = matches.value_of("input-csv-delimiter") {
            if d.len() == 1 {
                Some(d.as_bytes()[0])
            } else {
                panic!("delimiter must be one character")
            }
        } else {
            None
        };
        let bounds = None;
        let projection = None;
        let datetime_format = None;
        let batch_reader = arrow::csv::reader::Reader::new(
            batch_extractor,
            Arc::new(schema),
            header,
            delimiter,
            batch_size,
            bounds,
            projection,
            datetime_format,
        );
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
            CmdArgEntry::new(
                "input-csv-header",
                "Input CSV has header",
                "input-csv-header",
                false,
                False(DefaultValue::Bool(false)),
            ),
            CmdArgEntry::new(
                "input-csv-delimiter",
                "Delimiter of input CSV",
                "input-csv-delimiter",
                true,
                False(DefaultValue::String(String::from(","))),
            ),
        ])
    }
}

impl Iterator for Reader {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        self.batch_reader.next().map(|batch_res| batch_res.unwrap())
    }
}
