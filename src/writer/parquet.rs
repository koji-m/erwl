use crate::cli::{ArgRequired::False, CmdArg, CmdArgEntry, DefaultValue};
use crate::reader::Reader;
use crate::util::{WriteBatch, WriteableCursor};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use clap::ArgMatches;

use parquet::{
    arrow::arrow_writer::ArrowWriter, basic::Compression, file::properties::WriterProperties,
};

pub struct Writer {
    properties: WriterProperties,
    reader: Reader,
    file_extension: String,
    current_batch: Option<RecordBatch>,
    current_offset: usize,
    schema: Option<SchemaRef>,
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
            current_batch: None,
            current_offset: 0,
            schema: None,
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
            current_batch: None,
            current_offset: 0,
            schema: None,
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
}

impl WriteBatch for Writer {
    fn current_batch(&self) -> &Option<RecordBatch> {
        &self.current_batch
    }

    fn current_batch_mut(&mut self) -> &mut Option<RecordBatch> {
        &mut self.current_batch
    }

    fn current_offset(&self) -> usize {
        self.current_offset
    }

    fn current_offset_mut(&mut self) -> &mut usize {
        &mut self.current_offset
    }

    fn reader_mut(&mut self) -> &mut Reader {
        &mut self.reader
    }

    fn schema(&self) -> &Option<SchemaRef> {
        &self.schema
    }

    fn schema_mut(&mut self) -> &mut Option<SchemaRef> {
        &mut self.schema
    }

    fn file_extension(&self) -> &String {
        &self.file_extension
    }

    fn write_batch(&self, batch: RecordBatch, cursor: &WriteableCursor) {
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
