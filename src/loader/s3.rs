use crate::cli::{ArgRequired::True, ArgType, CmdArg, CmdArgEntry};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{
    types::ByteStream,
    {Client, Region},
};
use clap::ArgMatches;

pub struct Loader {
    client: Client,
    bucket: String,
    key_prefix: String,
}

impl Loader {
    pub async fn new(matches: &ArgMatches) -> Self {
        let region_provider =
            RegionProviderChain::default_provider().or_else(Region::new("us-east-1"));
        let config = aws_config::from_env().region(region_provider).load().await;
        Self {
            client: Client::new(&config),
            bucket: String::from(matches.value_of("s3-bucket").unwrap()),
            key_prefix: String::from(matches.value_of("key-prefix").unwrap()),
        }
    }

    pub async fn load(&self, bytes: Vec<u8>, suffix: usize) {
        let stream = ByteStream::from(bytes);
        let file = format!("{}{}.parquet", self.key_prefix, suffix);
        let resp = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&file)
            .body(stream)
            .send()
            .await;
        match resp {
            Ok(_) => println!("Wrote s3://{}/{}", self.bucket, file),
            Err(_) => println!("Error write s3://{}/{}", self.bucket, file),
        }
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
