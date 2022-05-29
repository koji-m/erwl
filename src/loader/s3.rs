use crate::cli::{ArgRequired::True, ArgType, CmdArg, CmdArgEntry};
use crate::error::LoadError;
use crate::writer::Writer;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{
    error::PutObjectError,
    types::{ByteStream, SdkError},
    {Client, Region},
};
use clap::ArgMatches;

pub struct Loader {
    config: aws_types::sdk_config::SdkConfig,
    bucket: String,
    key_prefix: String,
    writer: Writer,
}

impl From<SdkError<PutObjectError>> for LoadError {
    fn from(_err: SdkError<PutObjectError>) -> LoadError {
        LoadError
    }
}

impl Loader {
    pub async fn new(matches: &ArgMatches, writer: Writer) -> Self {
        let region_provider =
            RegionProviderChain::default_provider().or_else(Region::new("us-east-1"));
        Self {
            config: aws_config::from_env().region(region_provider).load().await,
            bucket: String::from(matches.value_of("s3-bucket").unwrap()),
            key_prefix: String::from(matches.value_of("key-prefix").unwrap()),
            writer,
        }
    }

    pub async fn load_batch(
        bytes: Vec<u8>,
        key_prefix: &String,
        suffix: usize,
        client: &Client,
        bucket: &String,
        file_extension: &String,
    ) -> Result<(), LoadError> {
        let stream = ByteStream::from(bytes);
        let file = format!("{}{}.{}", key_prefix, suffix, file_extension);
        client
            .put_object()
            .bucket(bucket)
            .key(&file)
            .body(stream)
            .send()
            .await?;
        Ok(())
    }

    pub async fn load(&mut self) -> Result<(), LoadError> {
        let key_prefix = self.key_prefix.clone();
        let client = Client::new(&self.config);
        let bucket = self.bucket.clone();
        let file_extension = self.writer.file_extension().clone();
        for (i, cursor) in self.writer.by_ref().enumerate() {
            Self::load_batch(
                cursor.into_inner().unwrap(),
                &key_prefix,
                i,
                &client,
                &bucket,
                &file_extension,
            )
            .await?;
        }
        Ok(())
    }

    pub fn cmd_args() -> CmdArg {
        CmdArg::new(vec![
            CmdArgEntry::new(
                "s3-bucket",
                "S3 bucket name",
                "s3-bucket",
                true,
                True,
                ArgType::String,
            ),
            CmdArgEntry::new(
                "key-prefix",
                "S3 key prefix",
                "key-prefix",
                true,
                True,
                ArgType::String,
            ),
        ])
    }
}
