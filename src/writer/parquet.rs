use crate::cli::{ArgRequired::False, CmdArg, CmdArgEntry, DefaultValue};
use crate::util::WriteableCursor;
use arrow::record_batch::RecordBatch;
use clap::ArgMatches;

use parquet::{
    arrow::arrow_writer::ArrowWriter, basic::Compression, file::properties::WriterProperties,
};

#[derive(Clone)]
pub struct Writer {
    properties: WriterProperties,
    file_extension: String,
}

impl Writer {
    pub fn new(matches: &ArgMatches) -> Self {
        let compression = match matches.value_of("compression").unwrap() {
            "snappy" => Compression::SNAPPY,
            _ => Compression::SNAPPY,
        };
        Self {
            properties: WriterProperties::builder()
                .set_compression(compression)
                .build(),
            file_extension: String::from("parquet"),
        }
    }

    pub fn cmd_args() -> CmdArg {
        CmdArg::new(vec![CmdArgEntry::new(
            "compression",
            "Compression type",
            "compression",
            true,
            False(DefaultValue::String(String::from("snappy"))),
        )])
    }

    pub fn file_extension(&self) -> &String {
        &self.file_extension
    }

    pub fn write(&self, cursor: &WriteableCursor, batch: RecordBatch) {
        // async writer not supported yet: https://github.com/apache/arrow-rs/issues/1269
        let mut writer = ArrowWriter::try_new(
            cursor.try_clone().unwrap(),
            batch.schema(),
            Some(self.properties.clone()),
        )
        .unwrap();
        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();
    }
}
