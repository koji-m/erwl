use crate::cli::{
    ArgRequired, CmdArg, CmdArgEntry, DefaultValue,
};
use crate::util::get_schema;
use arrow::{error::ArrowError, record_batch::RecordBatch};
use arrow::csv;
use arrow::datatypes::Schema;
use clap::ArgMatches;
use futures::stream::{iter, BoxStream};
use std::io::Read;
use std::sync::Arc;

#[derive(Clone)]
pub struct Reader {
    batch_size: usize,
    schema: Schema,
    has_header: bool,
    delimiter: Option<u8>,
    bounds: Option<(usize, usize)>,
    projection: Option<Vec<usize>>,
    datetime_format: Option<String>,
}

impl Reader {
    pub fn new(matches: &ArgMatches) -> Self {
        let batch_size: usize = matches.value_of_t("batch-size").unwrap();
        let schema_file_path = String::from(matches.value_of("schema-file").unwrap());
        let schema = get_schema(schema_file_path).unwrap();
        let has_header = matches.is_present("input-csv-header");
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

        Self {
            batch_size,
            schema,
            has_header,
            delimiter,
            bounds,
            projection,
            datetime_format,
        }
    }

    pub fn cmd_args() -> CmdArg {
        CmdArg::new(vec![
            CmdArgEntry::new(
                "schema-file",
                "Schema file (BigQuery schema)",
                "schema",
                true,
                ArgRequired::True,
            ),
            CmdArgEntry::new(
                "batch-size",
                "number of records in each files",
                "batch-size",
                true,
                ArgRequired::False(DefaultValue::String(String::from("10000"))),
            ),
            CmdArgEntry::new(
                "input-csv-header",
                "Input CSV has header",
                "input-csv-header",
                false,
                ArgRequired::False(DefaultValue::Bool(false)),
            ),
            CmdArgEntry::new(
                "input-csv-delimiter",
                "Delimiter of input CSV",
                "input-csv-delimiter",
                true,
                ArgRequired::False(DefaultValue::String(String::from(","))),
            ),
        ])
    }

    pub fn stream(&self, file: Box<dyn Read + Send>) -> BoxStream<'static, Result<RecordBatch, ArrowError>> {
        Box::pin(iter(csv::reader::Reader::new(
            file,
            Arc::new(self.schema.clone()),
            self.has_header,
            self.delimiter.clone(),
            self.batch_size.clone(),
            self.bounds.clone(),
            self.projection.clone(),
            self.datetime_format.clone(),
        )))
    }
}
