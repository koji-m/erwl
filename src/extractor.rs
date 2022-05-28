#[cfg(feature = "extractor-file")]
mod file;
#[cfg(feature = "extractor-file")]
pub use file::Extractor;

#[cfg(not(any(feature = "sync-extractor", feature = "async-extractor")))]
compile_error!("feature extractor-* not enabled.");
