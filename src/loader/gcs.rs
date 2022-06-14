use crate::cli::{ArgRequired::True, CmdArg, CmdArgEntry};
use crate::error::LoadError;
use crate::util::{WriteBatch, WriteableCursor};
use crate::writer::Writer;
use clap::ArgMatches;
use google_cloud_auth::{create_token_source, Config};

pub struct Loader {
    bucket: String,
    key_prefix: String,
    writer: Writer,
    load_size: usize,
}

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

impl Loader {
    pub async fn new(matches: &ArgMatches, writer: Writer) -> Self {
        Self {
            bucket: String::from(matches.value_of("gcs-bucket").unwrap()),
            key_prefix: String::from(matches.value_of("key-prefix").unwrap()),
            writer,
            load_size: matches.value_of_t("load-size").unwrap(),
        }
    }

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

    pub fn cmd_args() -> CmdArg {
        CmdArg::new(vec![
            CmdArgEntry::new(
                "gcs-bucket",
                "Google Cloud Storage bucket name",
                "gcs-bucket",
                true,
                True,
            ),
            CmdArgEntry::new("key-prefix", "Object key prefix", "key-prefix", true, True),
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
