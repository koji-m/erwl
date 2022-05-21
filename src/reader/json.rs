use crate::cli::{
    ArgRequired::{False, True},
    ArgType, Cmd, CmdArg, CmdArgEntry, ParsedArgs,
};
use crate::config::Conf;

pub struct Reader {
    cmd_arg: CmdArg,
    schema_file_path: Option<String>,
    batch_size: Option<usize>,
}

impl Reader {
    pub fn new() -> Self {
        let cmd_arg_entries = vec![
            CmdArgEntry::new(
                "schema-file",
                "Schema file (BigQuery schema)",
                "schema",
                true,
                True,
                ArgType::String,
            ),
            CmdArgEntry::new(
                "batch-size",
                "number of records in each files",
                "batch-size",
                true,
                False(String::from("10000")),
                ArgType::Number,
            ),
        ];
        Self {
            cmd_arg: CmdArg::new(cmd_arg_entries),
            schema_file_path: None,
            batch_size: None,
        }
    }

    pub fn schema_file_path(&self) -> &Option<String> {
        &self.schema_file_path
    }

    pub fn batch_size(&self) -> &Option<usize> {
        &self.batch_size
    }
}

impl Cmd for Reader {
    fn cmd_arg(&self) -> &CmdArg {
        &self.cmd_arg
    }
}

impl Conf for Reader {
    fn configure(&mut self, args: &ParsedArgs) {
        self.schema_file_path = Some(String::from(args.value_of("schema-file").unwrap()));
        self.batch_size = Some(args.value_of_t("batch-size").unwrap());
    }
}
