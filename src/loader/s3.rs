use arrow::{
    record_batch::RecordBatch,
    datatypes::SchemaRef,
};
use crate::cli::{ArgRequired::True, CmdArg, CmdArgEntry};
use crate::error::LoadError;
use crate::extractor::Extractor;
use crate::util::{WriteBatch, WriteableCursor};
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
    extractor: Extractor,
    load_size: usize,
    current_batch: Option<RecordBatch>,
    current_offset: usize,
    schema: Option<SchemaRef>,
}

impl From<SdkError<PutObjectError>> for LoadError {
    fn from(_err: SdkError<PutObjectError>) -> LoadError {
        LoadError
    }
}

impl Loader {
    pub async fn new(matches: &ArgMatches, writer: Writer, extractor: Extractor) -> Self {
        let region_provider =
            RegionProviderChain::default_provider().or_else(Region::new("us-east-1"));
        Self {
            config: aws_config::from_env().region(region_provider).load().await,
            bucket: String::from(matches.value_of("s3-bucket").unwrap()),
            key_prefix: String::from(matches.value_of("key-prefix").unwrap()),
            writer,
            extractor,
            load_size: matches.value_of_t("load-size").unwrap(),
            current_batch: None,
            current_offset: 0,
            schema: None,
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
        for i in 0.. {
            let cursor = WriteableCursor::default();
            let wrote = self.write(&cursor, self.load_size);
            if wrote < 1 {
                break;
            }
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
            CmdArgEntry::new("s3-bucket", "S3 bucket name", "s3-bucket", true, True),
            CmdArgEntry::new("key-prefix", "S3 key prefix", "key-prefix", true, True),
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

impl WriteBatch for Loader {
    fn current_batch(&self) -> &Option<RecordBatch> {
        &self.current_batch
    }

    fn current_batch_mut(&mut self) -> &mut Option<RecordBatch> {
        &mut self.current_batch
    }

    fn current_offset(&self) -> usize {
        self.current_offset
    }

    fn current_offset_mut(&mut self) -> &mut usize {
        &mut self.current_offset
    }

    fn extractor_mut(&mut self) -> &mut Extractor {
        &mut self.extractor
    }

    fn writer(&self) -> &Writer {
        &self.writer
    }

    fn schema(&self) -> &Option<SchemaRef> {
        &self.schema
    }

    fn schema_mut(&mut self) -> &mut Option<SchemaRef> {
        &mut self.schema
    }

    fn file_extension(&self) -> &String {
        self.writer.file_extension()
    }
}
