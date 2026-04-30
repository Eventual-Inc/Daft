//! Exports for ScalarUDF trait.
//!
//! This is so we don't have to manually import all each time we implement a function.

pub use daft_common::error::DaftResult;
pub use daft_common::ensure;
pub use daft_core::{
    prelude::{DataType, Field, Schema, SchemaRef},
    series::Series,
};
pub use serde::{Deserialize, Serialize};

pub use crate::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF},
};
