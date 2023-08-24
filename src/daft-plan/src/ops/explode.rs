use std::sync::Arc;

use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::Expr;
use snafu::ResultExt;

use crate::{
    logical_plan::{self, CreationSnafu},
    LogicalPlan,
};

#[derive(Clone, Debug)]
pub struct Explode {
    pub explode_exprs: Vec<Expr>,
    pub exploded_schema: SchemaRef,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Explode {
    pub(crate) fn new(
        explode_exprs: Vec<Expr>,
        input: Arc<LogicalPlan>,
    ) -> logical_plan::Result<Self> {
        let exploded_schema = {
            let upstream_schema = input.schema();
            let explode_schema = {
                let explode_fields = explode_exprs
                    .iter()
                    .map(|e| e.to_field(&upstream_schema))
                    .collect::<common_error::DaftResult<Vec<_>>>()
                    .context(CreationSnafu)?;
                Schema::new(explode_fields).context(CreationSnafu)?
            };
            let fields = upstream_schema
                .fields
                .iter()
                .map(|(name, field)| match explode_schema.fields.get(name) {
                    None => field,
                    Some(explode_field) => explode_field,
                })
                .cloned()
                .collect::<Vec<_>>();
            Schema::new(fields).context(CreationSnafu)?.into()
        };
        Ok(Self {
            explode_exprs,
            exploded_schema,
            input,
        })
    }
}
