use crate::cli::ParsedArgs;

pub trait Conf {
    fn configure(&mut self, args: &ParsedArgs);
}
