use crate::cli::{ArgRequired, CmdArg, CmdArgEntry};
//use crate::error::LoadError;
use crate::util::{BatchReceiver, WriteableCursor};
use crate::writer;
use arrow::record_batch::RecordBatch;
use clap::ArgMatches;
use futures::channel::mpsc;
use google_cloud_auth::{create_token_source, Config};
use std::future::Future;

pub struct Loader {
    bucket: String,
    key_prefix: String,
    writer: writer::Writer,
    load_size: usize,
}

/*
impl From<google_cloud_auth::error::Error> for LoadError {
    fn from(_err: google_cloud_auth::error::Error) -> LoadError {
        LoadError
    }
}

impl From<reqwest::Error> for LoadError {
    fn from(_err: reqwest::Error) -> LoadError {
        LoadError
    }
}
*/

impl Loader {
    pub async fn new(matches: &ArgMatches) -> Self {
        let writer = writer::Writer::new(&matches);
        Self {
            bucket: String::from(matches.value_of("gcs-bucket").unwrap()),
            key_prefix: String::from(matches.value_of("key-prefix").unwrap()),
            writer,
            load_size: matches.value_of_t("load-size").unwrap(),
        }
    }

    pub fn cmd_args() -> CmdArg {
        let mut arg_entries = vec![];
        arg_entries.push(
            CmdArgEntry::new(
                "gcs-bucket",
                "Google Cloud Storage bucket name",
                "gcs-bucket",
                true,
                ArgRequired::True,
            ),
        );
        arg_entries.push(
            CmdArgEntry::new(
                "key-prefix",
                "Object key prefix",
                "key-prefix",
                true,
                ArgRequired::True
            ),
        );
        arg_entries.push(
            CmdArgEntry::new(
                "load-size",
                "number of records in a batch",
                "load-size",
                true,
                ArgRequired::True,
            ),
        );
        arg_entries.extend_from_slice(&writer::Writer::cmd_args().entries());
        CmdArg::new(arg_entries)
    }

    fn writer(&self) -> writer::Writer {
        self.writer.clone()
    }

    pub fn load(&self, rx: mpsc::UnboundedReceiver<RecordBatch>) -> impl Future<Output = Result<(), mpsc::TryRecvError>> {
        let load_size = self.load_size;
        let bucket = self.bucket.clone();
        let file_extension = self.writer.file_extension().clone();
        let mut rcvr = BatchReceiver::new(rx);
        let writer = self.writer();
        let key_prefix = self.key_prefix.clone();
        let client = reqwest::Client::new();

        async move {
            let scopes = ["https://www.googleapis.com/auth/devstorage.read_write"];
            let config = Config {
                audience: None,
                scopes: Some(&scopes),
            };
            let ts = create_token_source(config).await.unwrap();
            let token = ts.token().await.unwrap();
            for i in 0.. {
                // receive RecordBatches from rx and aggregate them to single RecordBatch
                if let Some(rec) = rcvr.receive(load_size).await {
                    // create WriteableCursor
                    let cursor = WriteableCursor::default();
                    // write aggregated RecordBatch to WriteableCursor
                    writer.write(&cursor, rec);
                    // load contents of WriteableCursor to target destination
                    client.post(format!(
                        "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}{}.{}",
                        bucket,
                        key_prefix,
                        i,
                        file_extension,
                    ))
                    .bearer_auth(&token.access_token)
                    .header("Content-Type", "application/octet-stream")
                    .body(cursor.into_inner().unwrap())
                    .send()
                    .await.unwrap();
                } else {
                    break;
                }
            }
            Ok(())
        }
    }
    
    /*
    pub async fn load(&mut self) -> Result<(), LoadError> {
        let bucket = self.bucket.clone();
        let key_prefix = self.key_prefix.clone();
        let file_extension = self.writer.file_extension().clone();
        let client = reqwest::Client::new();
        let scopes = ["https://www.googleapis.com/auth/devstorage.read_write"];
        let config = Config {
            audience: None,
            scopes: Some(&scopes),
        };
        let ts = create_token_source(config).await?;
        let token = ts.token().await?;

        for i in 0.. {
            let cursor = WriteableCursor::default();
            let wrote = self.writer.write(&cursor, self.load_size);
            if wrote < 1 {
                break;
            }
            client.post(format!(
                    "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}{}.{}",
                    bucket,
                    key_prefix,
                    i,
                    file_extension,
                ))
                .bearer_auth(&token.access_token)
                .header("Content-Type", "application/octet-stream")
                .body(cursor.into_inner().unwrap())
                .send()
                .await?;
        }
        Ok(())
    }
    */
}
