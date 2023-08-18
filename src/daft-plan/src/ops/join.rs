use std::sync::Arc;

use daft_core::schema::SchemaRef;
use daft_dsl::Expr;

use crate::{JoinType, LogicalPlan};

#[derive(Clone, Debug)]
pub struct Join {
    pub right: Arc<LogicalPlan>,
    pub left_on: Vec<Expr>,
    pub right_on: Vec<Expr>,
    pub output_projection: Vec<Expr>,
    pub output_schema: SchemaRef,
    pub join_type: JoinType,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
}

impl Join {
    pub(crate) fn new(
        right: Arc<LogicalPlan>,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        output_projection: Vec<Expr>,
        output_schema: SchemaRef,
        join_type: JoinType,
        input: Arc<LogicalPlan>,
    ) -> Self {
        Self {
            right,
            left_on,
            right_on,
            output_projection,
            output_schema,
            join_type,
            input,
        }
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
