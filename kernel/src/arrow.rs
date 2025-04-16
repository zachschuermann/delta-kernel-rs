//! This module exists to help re-export the version of arrow used by default-engine and other
//! parts of kernel that need arrow

#[cfg(feature = "arrow_54")]
pub use arrow_54::*;

#[cfg(all(feature = "arrow_55", not(feature = "arrow_54")))]
pub use arrow_55::*;

// if nothing is enabled but we need arrow because of some other feature flag, default to lowest
// supported version
#[cfg(all(
    feature = "need_arrow",
    not(feature = "arrow_54"),
    not(feature = "arrow_55")
))]
compile_error!("Requested a feature that needs arrow without enabling arrow. Please enable the `arrow_54` or `arrow_55` feature");
