mod proto;

// ---------------------------------------
//   DAFT IR ORGANIZATION VIA RE-EXPORTS
// ---------------------------------------

#[rustfmt::skip]
pub mod rex {
    pub use daft_dsl::*;
}

#[rustfmt::skip]
pub mod functions {
    // pub use daft_functions::*;
    pub use daft_dsl::functions::*;
}

#[rustfmt::skip]
pub mod rel {
    pub use daft_logical_plan::*;
}

/// Flatten the daft_schema package, consider the prelude.
#[rustfmt::skip]
pub mod schema {
    pub use daft_schema::schema::*;
    pub use daft_schema::dtype::*;
    pub use daft_schema::field::*;
    pub use daft_schema::time_unit::TimeUnit;
    pub use daft_schema::image_format::ImageFormat;
    pub use daft_schema::image_mode::ImageMode;
}
