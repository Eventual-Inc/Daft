use std::{collections::HashSet, sync::Arc};

use daft_core::schema::{Schema, SchemaRef};
use daft_dsl::Expr;
use snafu::ResultExt;

use crate::{
    logical_plan::{self, CreationSnafu},
    JoinType, LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Join {
    // Upstream nodes.
    pub input: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,

    pub left_on: Vec<Expr>,
    pub right_on: Vec<Expr>,
    pub output_schema: SchemaRef,
    pub join_type: JoinType,
}

impl Join {
    pub(crate) fn try_new(
        input: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        join_type: JoinType,
    ) -> logical_plan::Result<Self> {
        // Schema inference ported from existing behaviour for parity,
        // but contains bug https://github.com/Eventual-Inc/Daft/issues/1294
        let output_schema = {
            let left_join_keys = left_on
                .iter()
                .map(|e| e.name())
                .collect::<common_error::DaftResult<HashSet<_>>>()
                .context(CreationSnafu)?;
            let left_schema = &input.schema().fields;
            let fields = left_schema
                .iter()
                .map(|(_, field)| field)
                .cloned()
                .chain(right.schema().fields.iter().filter_map(|(rname, rfield)| {
                    if left_join_keys.contains(rname.as_str()) {
                        None
                    } else if left_schema.contains_key(rname) {
                        let new_name = format!("right.{}", rname);
                        Some(rfield.rename(new_name))
                    } else {
                        Some(rfield.clone())
                    }
                }))
                .collect::<Vec<_>>();
            Schema::new(fields).context(CreationSnafu)?.into()
        };
        Ok(Self {
            input,
            right,
            left_on,
            right_on,
            output_schema,
            join_type,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Join: Type = {}", self.join_type));
        if !self.left_on.is_empty() && !self.right_on.is_empty() && self.left_on == self.right_on {
            res.push(format!(
                "On = {}",
                self.left_on
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        } else {
            if !self.left_on.is_empty() {
                res.push(format!(
                    "Left on = {}",
                    self.left_on
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            if !self.right_on.is_empty() {
                res.push(format!(
                    "Right on = {}",
                    self.right_on
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
        }
        res.push(format!(
            "Output schema = {}",
            self.output_schema.short_string()
        ));
        res
    }
}
