#[cfg(feature = "extractor-local")]
mod local;
#[cfg(feature = "extractor-local")]
pub use local::Extractor;
#[cfg(feature = "extractor-js")]
mod js;
#[cfg(feature = "extractor-js")]
pub use js::{cmd_args, get_stream};
#[cfg(feature = "extractor-mysql")]
mod mysql;
#[cfg(feature = "extractor-mysql")]
pub use crate::extractor::mysql::{cmd_args, get_stream};

#[cfg(feature = "extractor-dummy")]
mod dummy;
#[cfg(feature = "extractor-dummy")]
pub use dummy::{cmd_args, get_future};

#[cfg(not(feature = "extractor"))]
compile_error!("feature extractor-* not enabled.");
