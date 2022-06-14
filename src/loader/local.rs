use crate::cli::{ArgRequired::True, CmdArg, CmdArgEntry};
use crate::error::LoadError;
use crate::util::{WriteBatch, WriteableCursor};
use crate::writer::Writer;
use clap::ArgMatches;
use std::{
    fs::File,
    io,
    io::Write,
    path::Path,
};

pub struct Loader {
    path: String,
    file_prefix: String,
    writer: Writer,
    load_size: usize,
}

impl From<io::Error> for LoadError {
    fn from(_err: io::Error) -> LoadError {
        LoadError
    }
}

impl Loader {
    pub fn new(matches: &ArgMatches, writer: Writer) -> Self {
        Self {
            path: String::from(matches.value_of("path").unwrap()),
            file_prefix: String::from(matches.value_of("file-prefix").unwrap()),
            writer,
            load_size: matches.value_of_t("load-size").unwrap(),
        }
    }

    pub fn load(&mut self) -> Result<(), LoadError> {
        let path = Path::new(&self.path);
        let file_prefix = self.file_prefix.clone();
        let file_extension = self.writer.file_extension().clone();

        for i in 0.. {
            let cursor = WriteableCursor::default();
            let wrote = self.writer.write(&cursor, self.load_size);
            if wrote < 1 {
                break;
            }
            let full_path = path.join(format!("{}{}.{}", file_prefix, i, file_extension));
            let mut file = File::create(full_path)?;
            file.write_all(&cursor.into_inner().unwrap())?;
        }
        Ok(())
    }

    pub fn cmd_args() -> CmdArg {
        CmdArg::new(vec![
            CmdArgEntry::new(
                "path",
                "Local directory path",
                "path",
                true,
                True,
            ),
            CmdArgEntry::new("file-prefix", "File name prefix", "file-prefix", true, True),
            CmdArgEntry::new(
                "load-size",
                "number of records in a batch",
                "load-size",
                true,
                True,
            ),
        ])
    }
}
