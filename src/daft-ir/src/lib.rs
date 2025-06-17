pub(crate) mod proto;

// ---------------------------------------
//   DAFT IR ORGANIZATION VIA RE-EXPORTS
// ---------------------------------------


// todo(conner): consider scan, partition, pushdown mod
pub use common_scan_info::Pushdowns;
pub use common_scan_info::{PartitionField, PartitionTransform};
pub use common_scan_info::{ScanState, ScanTaskLike, ScanTaskLikeRef};

pub use crate::{
    rel::{LogicalPlan, LogicalPlanRef},
    rex::{Expr, ExprRef},
    schema::{DataType, Schema, SchemaRef},
};

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
    use std::sync::Arc;
    use crate::rex::Expr;

    pub use daft_logical_plan::*;
    pub use daft_logical_plan::ops::*;
    use crate::schema::Schema;

    /// Keep scan info together..
    pub use daft_logical_plan::source_info::*;
    pub use common_scan_info::PhysicalScanInfo;

    /// Creates a new source variant.
    pub fn new_source<S, I>(schema: S, info: I) -> Source
    where
        S: Into<Arc<Schema>>,
        I: Into<Arc<SourceInfo>>,
     {
        Source::new(schema.into(), info.into())
    }

    /// Creates a new projection variant.
    pub fn new_project<I, P, E>(input: I, projections: P) -> Project
    where
        I: Into<Arc<LogicalPlan>>,
        P: IntoIterator<Item = E>,
        E: Into<Arc<Expr>>,
    {
        let input: Arc<LogicalPlan> = input.into();
        let projections: Vec<Arc<Expr>> = projections.into_iter().map(|e| e.into()).collect();
        Project::new(input, projections).expect("construction should be infallible.")
    }
}

/// Flatten the daft_schema package, consider the prelude.
#[rustfmt::skip]
pub mod schema {
    pub use common_scan_info::PartitionField;
    pub use common_scan_info::PartitionTransform;
    pub use daft_schema::schema::*;
    pub use daft_schema::dtype::*;
    pub use daft_schema::field::*;
    pub use daft_schema::time_unit::TimeUnit;
    pub use daft_schema::image_format::ImageFormat;
    pub use daft_schema::image_mode::ImageMode;
}

/// Python exports for daft_ir.
#[cfg(feature = "python")]
pub mod python;

/// Export for daft.daft registration.
#[cfg(feature = "python")]
pub use python::register_modules;
