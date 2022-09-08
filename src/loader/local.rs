use arrow::record_batch::RecordBatch;
use crate::cli::{ArgRequired, CmdArg, CmdArgEntry};
//use crate::error::LoadError;
use crate::util::{BatchReceiver, WriteableCursor};
use crate::writer;
use clap::ArgMatches;
use futures::channel::mpsc;
use std::future::Future;
//use std::fs::File;
//use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub struct Loader {
    path: String,
    file_prefix: String,
    writer: writer::Writer,
    load_size: usize,
}

impl Loader {
    pub async fn new(matches: &ArgMatches) -> Self {
        let writer = writer::Writer::new(&matches);
        Self {
            path: String::from(matches.value_of("path").unwrap()),
            file_prefix: String::from(matches.value_of("file-prefix").unwrap()),
            writer,
            load_size: matches.value_of_t("load-size").unwrap(),
        }
    }

    pub fn cmd_args() -> CmdArg {
        let mut arg_entries = vec![];
        arg_entries.push(
            CmdArgEntry::new("path", "Local directory path", "path", true, ArgRequired::True)
        );
        arg_entries.push(
            CmdArgEntry::new("file-prefix", "File name prefix", "file-prefix", true, ArgRequired::True)
        );
        arg_entries.push(
            CmdArgEntry::new(
                "load-size",
                "number of records in a batch",
                "load-size",
                true,
                ArgRequired::True,
            )
        );
        arg_entries.extend_from_slice(&writer::Writer::cmd_args().entries());
        CmdArg::new(arg_entries)
    }

    fn writer(&self) -> writer::Writer {
        self.writer.clone()
    }

    fn output_path(index: usize, path: &Path, file_prefix: String, file_extension: String) -> PathBuf {
        path.join(format!("{}{}.{}", file_prefix, index, file_extension))
    }

    async fn write(cursor: WriteableCursor, path: PathBuf) {
        let mut file = File::create(path).await.unwrap();
        file.write_all(&cursor.into_inner().unwrap()).await.unwrap();
    }

    pub fn load(&self, rx: mpsc::UnboundedReceiver<RecordBatch>) -> impl Future<Output = Result<(), mpsc::TryRecvError>> {
        let load_size = self.load_size;
        let mut rcvr = BatchReceiver::new(rx);
        let writer = self.writer();
        let path_str = self.path.clone();
        let file_prefix = self.file_prefix.clone();
        let file_extension = self.writer.file_extension().clone();
        async move {
            let path = Path::new(&path_str);
            for i in 0.. {
                // receive RecordBatches from rx and aggregate them to single RecordBatch
                if let Some(rec) = rcvr.receive(load_size).await {
                    // create WriteableCursor
                    let cursor = WriteableCursor::default();
                    // write aggregated RecordBatch to WriteableCursor
                    writer.write(&cursor, rec);
                    // load contents of WriteableCursor to target destination
                    let path = Self::output_path(i, path.clone(), file_prefix.clone(), file_extension.clone());
                    Self::write(cursor, path).await;
                } else {
                    break;
                }
            }
            Ok(())
        }
    }
}

/*
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
            CmdArgEntry::new("path", "Local directory path", "path", true, True),
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
*/