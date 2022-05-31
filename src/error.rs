use std::error;
use std::fmt;

#[derive(Debug, Clone)]
pub struct LoadError;

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Load error")
    }
}

impl error::Error for LoadError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

#[derive(Debug, Clone)]
pub struct UnknownTypeError {
    pub type_name: String,
}

impl fmt::Display for UnknownTypeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "unknown BigQuery type: {}", self.type_name)
    }
}

impl error::Error for UnknownTypeError {
    fn description(&self) -> &str {
        "unknown BigQuery types"
    }
}