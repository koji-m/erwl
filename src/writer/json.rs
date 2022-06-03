use crate::cli::CmdArg;
use crate::reader::Reader;
use crate::util::WriteableCursor;
use arrow::json::writer::LineDelimitedWriter;
use clap::ArgMatches;

pub struct Writer {
    reader: Reader,
    file_extension: String,
}

impl Writer {
    #[cfg(not(feature = "async-reader"))]
    pub fn new(_matches: &ArgMatches, reader: Reader) -> Self {
        Self {
            reader,
            file_extension: String::from("json"),
        }
    }

    #[cfg(feature = "async-reader")]
    pub async fn new(_matches: &ArgMatches, reader: Reader) -> Self {
        Self {
            reader,
            file_extension: String::from("json"),
        }
    }

    pub fn cmd_args() -> CmdArg {
        CmdArg::new(vec![])
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
            let mut writer = LineDelimitedWriter::new(cursor.try_clone().unwrap());
            writer.write(batch).expect("Writing batch");
            writer.finish().unwrap();
            Some(cursor)
        } else {
            None
        }
    }
}
