#[cfg(feature = "loader-s3")]
mod s3;
#[cfg(feature = "loader-s3")]
pub use s3::Loader;
#[cfg(feature = "loader-gcs")]
mod gcs;
#[cfg(feature = "loader-gcs")]
pub use gcs::Loader;
#[cfg(feature = "loader-local")]
mod local;
#[cfg(feature = "loader-local")]
pub use local::Loader;

#[cfg(not(feature = "loader"))]
compile_error!("feature loader-* not enabled.");
