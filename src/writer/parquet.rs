use crate::cli::{ArgRequired::False, ArgType, CmdArg, CmdArgEntry};
use crate::reader::Reader;
use clap::ArgMatches;

use arrow::error::ArrowError;
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
    batch_reader: Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>>>,
    file_extension: String,
}

impl Writer {
    #[cfg(not(feature = "async-reader"))]
    pub fn new(matches: &ArgMatches, reader: Reader) -> Self {
        let compression = match matches.value_of("compression").unwrap() {
            "snappy" => Compression::SNAPPY,
            _ => Compression::SNAPPY,
        };
        let batch_reader = Box::new(reader.batch_reader());
        Self {
            properties: WriterProperties::builder()
                .set_compression(compression)
                .build(),
            batch_reader,
            file_extension: String::from("parquet"),
        }
    }

    #[cfg(feature = "async-reader")]
    pub async fn new(matches: &ArgMatches, reader: Reader) -> Self {
        let compression = match matches.value_of("compression").unwrap() {
            "snappy" => Compression::SNAPPY,
            _ => Compression::SNAPPY,
        };
        let batch_reader = Box::new(reader.batch_reader()).await;
        Self {
            properties: WriterProperties::builder()
                .set_compression(compression)
                .build(),
            batch_reader,
            file_extension: String::from("parquet"),
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

    pub fn file_extension(&self) -> &String {
        &self.file_extension
    }
}

impl Iterator for Writer {
    type Item = InMemoryWriteableCursor;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(res) = self.batch_reader.next() {
            let batch = res.unwrap();
            let cursor = InMemoryWriteableCursor::default();
            let mut writer = ArrowWriter::try_new(
                cursor.try_clone().unwrap(),
                batch.schema(),
                Some(self.properties.clone()),
            )
            .unwrap();
            writer.write(&batch).expect("Writing batch");
            writer.close().unwrap();
            Some(cursor)
        } else {
            None
        }
    }
}
