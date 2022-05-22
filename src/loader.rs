#[cfg(feature = "loader-s3")]
mod s3;
#[cfg(feature = "loader-s3")]
pub use s3::Loader;

#[cfg(not(any(feature = "sync-loader", feature = "async-loader")))]
compile_error!("feature loader-* not enabled.");
