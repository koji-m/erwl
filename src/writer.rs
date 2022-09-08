#[cfg(feature = "writer-parquet")]
mod parquet;
#[cfg(feature = "writer-parquet")]
pub use crate::writer::parquet::Writer;
#[cfg(feature = "writer-csv")]
mod csv;
#[cfg(feature = "writer-csv")]
pub use crate::writer::csv::Writer;
#[cfg(feature = "writer-json")]
mod json;
#[cfg(feature = "writer-json")]
pub use crate::writer::json::Writer;

#[cfg(not(feature = "writer"))]
compile_error!("feature writer-* not enabled.");
