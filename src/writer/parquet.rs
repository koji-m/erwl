use crate::cli::{ArgRequired::False, ArgType, CmdArg, CmdArgEntry};
use clap::ArgMatches;

use arrow::record_batch::RecordBatch;
use parquet::{
    arrow::arrow_writer::ArrowWriter,
    basic::Compression,
    file::{
        properties::WriterProperties,
        writer::{InMemoryWriteableCursor, TryClone},
    },
};

pub struct Writer {
    properties: WriterProperties,
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
        }
    }

    pub fn cmd_args() -> CmdArg {
        CmdArg::new(vec![CmdArgEntry::new(
            "compression",
            "Compression type",
            "compression",
            true,
            False(String::from("snappy")),
            ArgType::String,
        )])
    }

    pub fn write(&self, batch: RecordBatch) -> InMemoryWriteableCursor {
        let cursor = InMemoryWriteableCursor::default();
        let mut writer = ArrowWriter::try_new(
            cursor.try_clone().unwrap(),
            batch.schema(),
            Some(self.properties.clone()),
        )
        .unwrap();
        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();
        cursor
    }
}
