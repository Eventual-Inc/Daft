use common_error::DaftResult;
use daft_dsl::Expr;
use snafu::ResultExt;

use crate::{
    column_stats::TruthValue,
    micropartition::{MicroPartition, TableState},
    DaftCoreComputeSnafu,
};

impl MicroPartition {
    pub fn concat(mps: &[&mut Self]) {}
}
