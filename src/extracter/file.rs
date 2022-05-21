use crate::cli::{ArgRequired::True, ArgType, Cmd, CmdArg, CmdArgEntry, ParsedArgs};
use crate::config::Conf;

pub struct Extracter {
    cmd_arg: CmdArg,
    input_file: Option<String>,
}

impl Extracter {
    pub fn new() -> Self {
        let cmd_arg_entries = vec![CmdArgEntry::new(
            "input-file",
            "input file path or '-' for stdin",
            "input-file",
            true,
            True,
            ArgType::String,
        )];
        Self {
            cmd_arg: CmdArg::new(cmd_arg_entries),
            input_file: None,
        }
    }

    pub fn input_file(&self) -> &Option<String> {
        &self.input_file
    }
}

impl Cmd for Extracter {
    fn cmd_arg(&self) -> &CmdArg {
        &self.cmd_arg
    }
}

impl Conf for Extracter {
    fn configure(&mut self, args: &ParsedArgs) {
        self.input_file = Some(String::from(args.value_of("input-file").unwrap()));
    }
}
