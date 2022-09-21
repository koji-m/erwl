#[cfg(feature = "extractor-local")]
mod local;
#[cfg(feature = "extractor-local")]
pub use local::Extractor;
#[cfg(feature = "extractor-postgresql")]
mod postgresql;
#[cfg(feature = "extractor-postgresql")]
pub use postgresql::Extractor;

#[cfg(not(feature = "extractor"))]
compile_error!("feature extractor-* not enabled.");
