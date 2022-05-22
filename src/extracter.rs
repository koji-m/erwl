#[cfg(feature = "extracter-file")]
mod file;
#[cfg(feature = "extracter-file")]
pub use file::Extracter;

#[cfg(not(any(feature = "sync-extracter", feature = "async-extracter")))]
compile_error!("feature extracter-* not enabled.");
