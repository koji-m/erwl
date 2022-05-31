#[cfg(feature = "reader-json")]
mod json;
#[cfg(feature = "reader-json")]
pub use json::Reader;
#[cfg(feature = "reader-csv")]
mod csv;
#[cfg(feature = "reader-csv")]
pub use csv::Reader;

#[cfg(not(any(feature = "sync-reader", feature = "async-reader")))]
compile_error!("feature reader-* not enabled.");
