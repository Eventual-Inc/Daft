use std::sync::Arc;

use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::Expr;
use snafu::ResultExt;

use crate::{
    logical_plan::{self, CreationSnafu},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Explode {
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    // Expressions to explode. e.g. col("a")
    pub to_explode: Vec<Expr>,
    pub exploded_schema: SchemaRef,
}

impl Explode {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        to_explode: Vec<Expr>,
    ) -> logical_plan::Result<Self> {
        let explode_exprs = to_explode
            .iter()
            .map(daft_dsl::functions::list::explode)
            .collect::<Vec<_>>();
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
                .map(|(name, field)| explode_schema.fields.get(name).unwrap_or(field))
                .cloned()
                .collect::<Vec<_>>();
            Schema::new(fields).context(CreationSnafu)?.into()
        };
        Ok(Self {
            input,
            to_explode,
            exploded_schema,
        })
    }
}
