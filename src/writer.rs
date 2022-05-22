#[cfg(feature = "writer-parquet")]
mod parquet;
#[cfg(feature = "writer-parquet")]
pub use crate::writer::parquet::Writer;

#[cfg(not(any(feature = "sync-writer", feature = "async-writer")))]
compile_error!("feature writer-* not enabled.");
