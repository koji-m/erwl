use crate::cli::{ArgRequired, CmdArg, CmdArgEntry, DefaultValue};
use crate::util::WriteableCursor;
use arrow::csv::writer::WriterBuilder;
use arrow::record_batch::RecordBatch;
use clap::ArgMatches;

#[derive(Clone)]
pub struct Writer {
    file_extension: String,
    has_header: bool,
}

impl Writer {
    pub fn new(matches: &ArgMatches) -> Self {
        let has_header = matches.is_present("output-csv-header");
        Self {
            file_extension: String::from("csv"),
            has_header,
        }
    }

    pub fn cmd_args() -> CmdArg {
        CmdArg::new(vec![CmdArgEntry::new(
            "output-csv-header",
            "Output CSV has header",
            "output-csv-header",
            false,
            ArgRequired::False(DefaultValue::Bool(false)),
        )])
    }

    pub fn file_extension(&self) -> &String {
        &self.file_extension
    }

    pub fn write(&self, cursor: &WriteableCursor, batch: RecordBatch) {
        let builder = WriterBuilder::new().has_headers(self.has_header);
        let mut writer = builder.build(cursor.try_clone().unwrap());
        writer.write(&batch).expect("Writing batch");
    }
}

/*
impl Iterator for Writer {
    type Item = WriteableCursor;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(batch) = self.reader.next() {
            let cursor = WriteableCursor::default();
            let builder = WriterBuilder::new().has_headers(self.headers);
            let mut writer = builder.build(cursor.try_clone().unwrap());
            writer.write(&batch).expect("Writing batch");
            Some(cursor)
        } else {
            None
        }
    }
}
*/