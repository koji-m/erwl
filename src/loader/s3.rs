use crate::cli::{ArgRequired::True, CmdArg, CmdArgEntry};
use crate::error::LoadError;
use crate::util::{BatchReceiver, WriteableCursor};
use crate::writer;
use arrow::record_batch::RecordBatch;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{
    error::PutObjectError,
    types::{ByteStream, SdkError},
    {Client, Region},
};
use clap::ArgMatches;
use futures::channel::mpsc;
use std::future::Future;
use tokio::runtime::Runtime;

pub struct Loader {
    config: aws_types::sdk_config::SdkConfig,
    bucket: String,
    key_prefix: String,
    writer: writer::Writer,
    load_size: usize,
}

impl From<SdkError<PutObjectError>> for LoadError {
    fn from(_err: SdkError<PutObjectError>) -> LoadError {
        LoadError
    }
}

impl Loader {
    pub async fn new(matches: &ArgMatches) -> Self {
        let writer = writer::Writer::new(&matches);
        let region_provider =
            RegionProviderChain::default_provider().or_else(Region::new("us-east-1"));
        Self {
            config: aws_config::from_env().region(region_provider).load().await,
            bucket: String::from(matches.value_of("s3-bucket").unwrap()),
            key_prefix: String::from(matches.value_of("key-prefix").unwrap()),
            writer,
            load_size: matches.value_of_t("load-size").unwrap(),
        }
    }

    pub fn cmd_args() -> CmdArg {
        let mut arg_entries = vec![];
        arg_entries.push(
            CmdArgEntry::new("s3-bucket", "S3 bucket name", "s3-bucket", true, True),
        );
        arg_entries.push(
            CmdArgEntry::new("key-prefix", "S3 key prefix", "key-prefix", true, True),
        );
        arg_entries.push(
            CmdArgEntry::new(
                "load-size",
                "number of records in a batch",
                "load-size",
                true,
                True,
            ),
        );
        arg_entries.extend_from_slice(&writer::Writer::cmd_args().entries());
        CmdArg::new(arg_entries)
    }

    fn writer(&self) -> writer::Writer {
        self.writer.clone()
    }

    fn key<'a>(suffix: usize, key_prefix: String, file_extension: String) -> String {
        format!("{}{}.{}", key_prefix, suffix, file_extension)
    }

    pub async fn upload(
        bytes: Vec<u8>,
        key: &str,
        client: &Client,
        bucket: &String,
    ) -> Result<(), LoadError> {
        let stream = ByteStream::from(bytes);
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(stream)
            .send()
            .await?;
        Ok(())
    }

    pub fn load(&self, rx: mpsc::UnboundedReceiver<RecordBatch>) -> impl Future<Output = Result<(), mpsc::TryRecvError>> {
        let load_size = self.load_size;
        let mut rcvr = BatchReceiver::new(rx);
        let writer = self.writer();
        let key_prefix = self.key_prefix.clone();
        let client = Client::new(&self.config);
        let bucket = self.bucket.clone();
        let file_extension = self.writer.file_extension().clone();

        async move {
            for i in 0.. {
                // receive RecordBatches from rx and aggregate them to single RecordBatch
                if let Some(rec) = rcvr.receive(load_size).await {
                    // create WriteableCursor
                    let cursor = WriteableCursor::default();
                    // write aggregated RecordBatch to WriteableCursor
                    writer.write(&cursor, rec);
                    // load contents of WriteableCursor to target destination
                    let key = Self::key(i, key_prefix.clone(), file_extension.clone());
                    Self::upload(
                        cursor.into_inner().unwrap(),
                        &key,
                        &client,
                        &bucket,
                    ).await.unwrap();
                } else {
                    break;
                }
            }
            Ok(())
        }
    }

    /*
    pub fn load(&mut self) -> Result<(), LoadError> {
        let key_prefix = self.key_prefix.clone();
        let client = Client::new(&self.config);
        let bucket = self.bucket.clone();
        let file_extension = self.writer.file_extension().clone();
        let rt  = Runtime::new().unwrap();
        rt.block_on(async {
            for i in 0.. {
                let cursor = WriteableCursor::default();
                let wrote = self.write(&cursor, self.load_size).await;
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
        });
        Ok(())
    }
    */



    /*
    async fn write(&mut self, cursor: &WriteableCursor, size: usize) -> usize {
        let mut rows_need = size;
        let mut batches = Vec::new();
        while rows_need > 0 {
            if let Some(batch) = self.current_batch() {
                let residue = batch.num_rows() - self.current_offset();
                if residue > rows_need {
                    let sliced = batch.slice(self.current_offset(), rows_need);
                    batches.push(sliced);
                    *self.current_offset_mut() += rows_need;
                    rows_need = 0;
                } else {
                    let sliced = batch.slice(self.current_offset(), residue);
                    batches.push(sliced);
                    rows_need -= residue;
                    *self.current_offset_mut() = 0;
                    *self.current_batch_mut() = self.stream_mut().next().await;
                }
            } else if let Some(batch) = self.stream_mut().next().await {
                *self.schema_mut() = Some(batch.schema());
                *self.current_batch_mut() = Some(batch);
                *self.current_offset_mut() = 0;
            } else {
                break;
            }
        }
        let batch = RecordBatch::concat(self.schema().as_ref().unwrap(), &batches).unwrap();
        self.writer().write_batch(batch, cursor);
        size - rows_need
    }

    async fn write_all(&mut self, cursor: &WriteableCursor) -> usize {
        let mut batches = Vec::new();
        let mut num_rows_wrote = 0;
        while let Some(batch) = self.stream_mut().next().await {
            *self.schema_mut() = Some(batch.schema());
            num_rows_wrote += batch.num_rows();
            batches.push(batch);
        }
        let batch = RecordBatch::concat(self.schema().as_ref().unwrap(), &batches).unwrap();
        self.writer().write_batch(batch, cursor);
        num_rows_wrote
    }

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

    fn stream_mut(&mut self) -> &mut BoxStream<'static, RecordBatch> {
        &mut self.stream
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
    */
}
