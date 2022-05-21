use crate::cli::{ArgRequired::False, ArgType, Cmd, CmdArg, CmdArgEntry, ParsedArgs};
use crate::config::Conf;

pub struct Writer {
    cmd_arg: CmdArg,
    compression: Option<String>,
}

impl Writer {
    pub fn new() -> Self {
        let cmd_arg_entries = vec![CmdArgEntry::new(
            "compression",
            "Compression type",
            "compression",
            true,
            False(String::from("snappy")),
            ArgType::String,
        )];
        Self {
            cmd_arg: CmdArg::new(cmd_arg_entries),
            compression: None,
        }
    }

    pub fn compression(&self) -> &Option<String> {
        &self.compression
    }
}

impl Cmd for Writer {
    fn cmd_arg(&self) -> &CmdArg {
        &self.cmd_arg
    }
}

impl Conf for Writer {
    fn configure(&mut self, args: &ParsedArgs) {
        self.compression = Some(String::from(args.value_of("compression").unwrap()));
    }
}
