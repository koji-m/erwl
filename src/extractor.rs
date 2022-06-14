#[cfg(feature = "extractor-local")]
mod local;
#[cfg(feature = "extractor-local")]
pub use local::Extractor;

#[cfg(not(any(feature = "sync-extractor", feature = "async-extractor")))]
compile_error!("feature extractor-* not enabled.");
