use arrow::{
    record_batch::RecordBatch,
    datatypes::SchemaRef
};
use crate::cli::{ArgRequired::False, CmdArg, CmdArgEntry, DefaultValue};
use crate::reader::Reader;
use crate::util::WriteableCursor;
use clap::ArgMatches;

use parquet::{
    arrow::arrow_writer::ArrowWriter,
    basic::Compression,
    file::{
        properties::WriterProperties,
    },
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
            schema:None,
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
            schema:None,
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

    pub fn write(&mut self, cursor: &WriteableCursor, size: usize) -> usize {
        let mut rows_need = size;
        let mut batches = Vec::new();
        while rows_need > 0 {
            if let Some(batch) = &self.current_batch {
                let residue = batch.num_rows() - self.current_offset;
                if residue > rows_need {
                    let sliced = batch.slice(self.current_offset, rows_need);
                    batches.push(sliced);
                    self.current_offset += rows_need;
                    rows_need = 0;
                } else {
                    let sliced = batch.slice(self.current_offset, residue);
                    batches.push(sliced);
                    rows_need -= residue;
                    self.current_offset = 0;
                    self.current_batch = self.reader.next();
                }
            } else if let Some(batch) = self.reader.next() {
                self.schema = Some(batch.schema());
                self.current_batch = Some(batch);
                self.current_offset = 0;
            } else {
                break;
            }
        }
        if batches.is_empty() {
            size - rows_need
        } else {
            let batch = RecordBatch::concat(&self.schema.as_ref().unwrap(), &batches).unwrap();
            let mut writer = ArrowWriter::try_new(
                cursor.try_clone().unwrap(),
                batch.schema(),
                Some(self.properties.clone()),
            )
            .unwrap();
            writer.write(&batch).expect("Writing batch");
            writer.close().unwrap();
            size - rows_need
        }
    }
}
