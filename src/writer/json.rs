use crate::cli::CmdArg;
use crate::util::WriteableCursor;
use arrow::json::writer::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;
use clap::ArgMatches;

#[derive(Clone)]
pub struct Writer {
    file_extension: String,
}

impl Writer {
    pub fn new(_matches: &ArgMatches) -> Self {
        Self {
            file_extension: String::from("json"),
        }
    }

    pub fn cmd_args() -> CmdArg {
        CmdArg::new(vec![])
    }

    pub fn file_extension(&self) -> &String {
        &self.file_extension
    }

    pub fn write(&self, cursor: &WriteableCursor, batch: RecordBatch) {
        let mut writer = LineDelimitedWriter::new(
            cursor.try_clone().unwrap(),
        );
        writer.write(batch).expect("Writing batch");
        writer.finish().unwrap();
    }
}

/*impl Iterator for Writer {
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
*/