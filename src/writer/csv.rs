use crate::cli::{ArgRequired::False, CmdArg, CmdArgEntry, DefaultValue};
use crate::reader::Reader;
use crate::util::WriteableCursor;
use arrow::csv::writer::WriterBuilder;
use clap::ArgMatches;

pub struct Writer {
    reader: Reader,
    file_extension: String,
    headers: bool,
}

impl Writer {
    #[cfg(not(feature = "async-reader"))]
    pub fn new(matches: &ArgMatches, reader: Reader) -> Self {
        let headers = matches.is_present("headers");
        Self {
            reader,
            file_extension: String::from("csv"),
            headers,
        }
    }

    #[cfg(feature = "async-reader")]
    pub async fn new(matches: &ArgMatches, reader: Reader) -> Self {
        let headers = matches.is_present("headers");
        Self {
            reader,
            file_extension: String::from("csv"),
            headers,
        }
    }

    pub fn cmd_args() -> CmdArg {
        CmdArg::new(vec![CmdArgEntry::new(
            "headers",
            "Write headers",
            "headers",
            false,
            False(DefaultValue::Bool(false)),
        )])
    }

    pub fn file_extension(&self) -> &String {
        &self.file_extension
    }
}

impl Iterator for Writer {
    type Item = WriteableCursor;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(batch) = self.reader.next() {
            let cursor = WriteableCursor::default();
            let builder = WriterBuilder::new()
                .has_headers(self.headers);
            let mut writer = builder.build(
                cursor.try_clone().unwrap(),
            );
            writer.write(&batch).expect("Writing batch");
            Some(cursor)
        } else {
            None
        }
    }
}
