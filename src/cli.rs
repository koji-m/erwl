use clap::{Arg, Command};

#[derive(Clone)]
pub enum DefaultValue {
    String(String),
    Bool(bool),
}

#[derive(Clone)]
pub enum ArgRequired {
    True,
    False(DefaultValue),
}

impl ArgRequired {
    fn as_bool(&self) -> bool {
        match self {
            Self::True => true,
            Self::False(_) => false,
        }
    }
}

#[derive(Clone)]
pub struct CmdArgEntry {
    pub name: String,
    pub help: String,
    pub long: String,
    pub takes_value: bool,
    pub required: ArgRequired,
}

impl CmdArgEntry {
    pub fn new(
        name: &str,
        help: &str,
        long: &str,
        takes_value: bool,
        required: ArgRequired,
    ) -> Self {
        Self {
            name: String::from(name),
            help: String::from(help),
            long: String::from(long),
            takes_value,
            required,
        }
    }
}

#[derive(Clone)]
pub struct CmdArg {
    entries: Vec<CmdArgEntry>,
}

impl CmdArg {
    pub fn new(entries: Vec<CmdArgEntry>) -> Self {
        Self { entries }
    }

    pub fn entries(&self) -> &Vec<CmdArgEntry> {
        &self.entries
    }
}

pub trait Cmd {
    fn cmd_arg(&self) -> &CmdArg;
}

pub fn command<'a>() -> Command<'a> {
    Command::new("erwl")
        .version("0.0.1")
        .about("Extract and Load data")
}

pub fn arg_parse<'a>(cmd_args: &'a CmdArg, mut cmd: Command<'a>) -> Command<'a> {
    let args: Vec<Arg> = cmd_args
        .entries()
        .iter()
        .map(|e| {
            let a = Arg::new(e.name.as_str())
                .help(e.help.as_str())
                .long(e.long.as_str())
                .takes_value(e.takes_value)
                .required(e.required.as_bool());
            match e.required {
                ArgRequired::True => a,
                ArgRequired::False(ref val) => match val {
                    DefaultValue::String(s) => a.default_value(s.as_str()),
                    DefaultValue::Bool(b) => if *b {
                        a.default_value("true")
                    } else {
                        a
                    }
                }
            }
        })
        .collect();
    for arg in args {
        cmd = cmd.arg(arg);
    }
    cmd
}
