use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use common_treenode::{Transformed, TransformedResult, TreeNode};
use daft_core::{
    join::{JoinStrategy, JoinType},
    schema::{Schema, SchemaRef},
    DataType,
};
use daft_dsl::{
    col,
    join::{get_common_join_keys, infer_join_schema},
    resolve_exprs, Expr, ExprRef,
};
use itertools::Itertools;
use snafu::ResultExt;

use crate::{
    logical_ops::Project,
    logical_plan::{self, CreationSnafu},
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Join {
    // Upstream nodes.
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,

    pub left_on: Vec<ExprRef>,
    pub right_on: Vec<ExprRef>,
    pub join_type: JoinType,
    pub join_strategy: Option<JoinStrategy>,
    pub output_schema: SchemaRef,
}

impl std::hash::Hash for Join {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.left, state);
        std::hash::Hash::hash(&self.right, state);
        std::hash::Hash::hash(&self.left_on, state);
        std::hash::Hash::hash(&self.right_on, state);
        std::hash::Hash::hash(&self.join_type, state);
        std::hash::Hash::hash(&self.join_strategy, state);
        std::hash::Hash::hash(&self.output_schema, state);
    }
}

impl Join {
    pub(crate) fn try_new(
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        left_on: Vec<ExprRef>,
        right_on: Vec<ExprRef>,
        join_type: JoinType,
        join_strategy: Option<JoinStrategy>,
    ) -> logical_plan::Result<Self> {
        let (left_on, left_fields) =
            resolve_exprs(left_on, &left.schema()).context(CreationSnafu)?;
        let (right_on, right_fields) =
            resolve_exprs(right_on, &right.schema()).context(CreationSnafu)?;

        for (on_exprs, on_fields) in [(&left_on, left_fields), (&right_on, right_fields)] {
            let on_schema = Schema::new(on_fields).context(CreationSnafu)?;
            for (field, expr) in on_schema.fields.values().zip(on_exprs.iter()) {
                if matches!(field.dtype, DataType::Null) {
                    return Err(DaftError::ValueError(format!(
                        "Can't join on null type expressions: {expr}"
                    )))
                    .context(CreationSnafu);
                }
            }
        }

        if matches!(join_type, JoinType::Anti | JoinType::Semi) {
            // The output schema is the same as the left input schema for anti and semi joins.

            let output_schema = left.schema().clone();

            Ok(Self {
                left,
                right,
                left_on,
                right_on,
                join_type,
                join_strategy,
                output_schema,
            })
        } else {
            let common_join_keys: HashSet<_> =
                get_common_join_keys(left_on.as_slice(), right_on.as_slice()).collect();

            let left_names = left.schema().names();
            let right_names = right.schema().names();

            let mut new_field_names = left_names
                .iter()
                .cloned()
                .chain(right_names.iter().cloned())
                .collect::<HashSet<_>>();

            // rename right columns that have the same name as left columns and are not join keys
            // old_name -> new_name
            let right_rename_mapping: HashMap<_, _> = right_names
                .iter()
                .filter_map(|name| {
                    if !new_field_names.contains(name) || common_join_keys.contains(name) {
                        None
                    } else {
                        let mut new_name = name.clone();
                        while new_field_names.contains(&new_name) {
                            new_name = format!("right.{}", new_name);
                        }
                        new_field_names.insert(new_name.clone());

                        Some((name.clone(), new_name))
                    }
                })
                .collect();

            let (right, right_on) = if right_rename_mapping.is_empty() {
                (right, right_on)
            } else {
                // projection to update the right side with the new column names
                let new_right_projection: Vec<_> = right_names
                    .iter()
                    .map(|name| {
                        if let Some(new_name) = right_rename_mapping.get(name) {
                            Expr::Alias(col(name.clone()), new_name.clone().into()).into()
                        } else {
                            col(name.clone())
                        }
                    })
                    .collect();

                let new_right: LogicalPlan =
                    Project::try_new(right, new_right_projection, Default::default())?.into();

                // change any column references in the right_on expressions to the new column names
                let new_right_on = right_on
                    .into_iter()
                    .map(|expr| {
                        expr.transform(|e| {
                            if let Expr::Column(name) = e.as_ref()
                                && let Some(new_name) = right_rename_mapping.get(name.as_ref())
                            {
                                Ok(Transformed::yes(col(new_name.clone())))
                            } else {
                                Ok(Transformed::no(e))
                            }
                        })
                        .data()
                    })
                    .collect::<DaftResult<Vec<_>>>()
                    .context(CreationSnafu)?;

                (new_right.into(), new_right_on)
            };

            let output_schema = infer_join_schema(
                &left.schema(),
                &right.schema(),
                &left_on,
                &right_on,
                join_type,
            )
            .context(CreationSnafu)?;

            Ok(Self {
                left,
                right,
                left_on,
                right_on,
                join_type,
                join_strategy,
                output_schema,
            })
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Join: Type = {}", self.join_type));
        res.push(format!(
            "Strategy = {}",
            self.join_strategy
                .map_or_else(|| "Auto".to_string(), |s| s.to_string())
        ));
        if !self.left_on.is_empty() && !self.right_on.is_empty() && self.left_on == self.right_on {
            res.push(format!(
                "On = {}",
                self.left_on.iter().map(|e| e.to_string()).join(", ")
            ));
        } else {
            if !self.left_on.is_empty() {
                res.push(format!(
                    "Left on = {}",
                    self.left_on.iter().map(|e| e.to_string()).join(", ")
                ));
            }
            if !self.right_on.is_empty() {
                res.push(format!(
                    "Right on = {}",
                    self.right_on.iter().map(|e| e.to_string()).join(", ")
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
