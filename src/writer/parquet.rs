use crate::cli::{ArgRequired::False, CmdArg, CmdArgEntry, DefaultValue};
use crate::reader::Reader;
use clap::ArgMatches;

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
    reader: Reader,
    file_extension: String,
}

impl Writer {
    #[cfg(not(feature = "async-reader"))]
    pub fn new(matches: &ArgMatches, reader: Reader) -> Self {
        let compression = match matches.value_of("compression").unwrap() {
            "snappy" => Compression::SNAPPY,
            _ => Compression::SNAPPY,
        };
        Self {
            properties: WriterProperties::builder()
                .set_compression(compression)
                .build(),
            reader,
            file_extension: String::from("parquet"),
        }
    }

    #[cfg(feature = "async-reader")]
    pub async fn new(matches: &ArgMatches, reader: Reader) -> Self {
        let compression = match matches.value_of("compression").unwrap() {
            "snappy" => Compression::SNAPPY,
            _ => Compression::SNAPPY,
        };
        Self {
            properties: WriterProperties::builder()
                .set_compression(compression)
                .build(),
            reader,
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
}

impl Iterator for Writer {
    type Item = InMemoryWriteableCursor;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(res) = self.reader.next() {
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
