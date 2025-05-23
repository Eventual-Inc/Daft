//! Exports for ScalarUDF trait.
//!
//! This is so we don't have to manually import all each time we implement a funciton.

pub use common_error::{ensure, DaftResult};
pub use daft_core::{
    prelude::{Field, Schema, SchemaRef, Utf8Array},
    series::Series,
};
pub use serde::{Deserialize, Serialize};

pub use crate::{
    functions::{FunctionArgs, ScalarUDF},
    ExprRef,
};
