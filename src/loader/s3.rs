use crate::cli::{ArgRequired::True, ArgType, Cmd, CmdArg, CmdArgEntry, ParsedArgs};
use crate::config::Conf;

pub struct Loader {
    cmd_arg: CmdArg,
    bucket: Option<String>,
    key_prefix: Option<String>,
}

impl Loader {
    pub fn new() -> Self {
        let cmd_arg_entries = vec![
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
        ];
        Self {
            cmd_arg: CmdArg::new(cmd_arg_entries),
            bucket: None,
            key_prefix: None,
        }
    }

    pub fn bucket(&self) -> &Option<String> {
        &self.bucket
    }

    pub fn key_prefix(&self) -> &Option<String> {
        &self.key_prefix
    }
}

impl Cmd for Loader {
    fn cmd_arg(&self) -> &CmdArg {
        &self.cmd_arg
    }
}

impl Conf for Loader {
    fn configure(&mut self, args: &ParsedArgs) {
        self.bucket = Some(String::from(args.value_of("s3-bucket").unwrap()));
        self.key_prefix = Some(String::from(args.value_of("key-prefix").unwrap()));
    }
}
