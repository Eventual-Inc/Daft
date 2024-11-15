use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    col,
    join::{get_common_join_keys, infer_join_schema},
    optimization::replace_columns_with_expressions,
    Expr, ExprRef, ExprResolver,
};
use itertools::Itertools;
use snafu::ResultExt;
use uuid::Uuid;

use crate::{
    logical_plan::{self, CreationSnafu},
    ops::Project,
    LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Join {
    // Upstream nodes.
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,

    pub left_on: Vec<ExprRef>,
    pub right_on: Vec<ExprRef>,
    pub null_equals_nulls: Option<Vec<bool>>,
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
        std::hash::Hash::hash(&self.null_equals_nulls, state);
        std::hash::Hash::hash(&self.join_type, state);
        std::hash::Hash::hash(&self.join_strategy, state);
        std::hash::Hash::hash(&self.output_schema, state);
    }
}

impl Join {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        left_on: Vec<ExprRef>,
        right_on: Vec<ExprRef>,
        null_equals_nulls: Option<Vec<bool>>,
        join_type: JoinType,
        join_strategy: Option<JoinStrategy>,
        join_suffix: Option<&str>,
        join_prefix: Option<&str>,
        // if true, then duplicate column names will be kept
        // ex: select * from a left join b on a.id = b.id
        // if true, then the resulting schema will have two columns named id (id, and b.id)
        // In SQL the join column is always kept, while in dataframes it is not
        keep_join_keys: bool,
    ) -> logical_plan::Result<Self> {
        let expr_resolver = ExprResolver::default();

        let (left_on, _) = expr_resolver
            .resolve(left_on, &left.schema())
            .context(CreationSnafu)?;
        let (right_on, _) = expr_resolver
            .resolve(right_on, &right.schema())
            .context(CreationSnafu)?;

        let (unique_left_on, unique_right_on) =
            Self::rename_join_keys(left_on.clone(), right_on.clone());

        let left_fields: Vec<Field> = unique_left_on
            .iter()
            .map(|e| e.to_field(&left.schema()))
            .collect::<DaftResult<Vec<Field>>>()
            .context(CreationSnafu)?;

        let right_fields: Vec<Field> = unique_right_on
            .iter()
            .map(|e| e.to_field(&right.schema()))
            .collect::<DaftResult<Vec<Field>>>()
            .context(CreationSnafu)?;

        for (on_exprs, on_fields) in [
            (&unique_left_on, &left_fields),
            (&unique_right_on, &right_fields),
        ] {
            for (field, expr) in on_fields.iter().zip(on_exprs.iter()) {
                // Null type check for both fields and expressions
                if matches!(field.dtype, DataType::Null) {
                    return Err(DaftError::ValueError(format!(
                        "Can't join on null type expressions: {expr}"
                    )))
                    .context(CreationSnafu);
                }
            }
        }

        if let Some(null_equals_null) = &null_equals_nulls {
            if null_equals_null.len() != left_on.len() {
                return Err(DaftError::ValueError(
                    "null_equals_nulls must have the same length as left_on or right_on"
                        .to_string(),
                ))
                .context(CreationSnafu);
            }
        }

        if matches!(join_type, JoinType::Anti | JoinType::Semi) {
            // The output schema is the same as the left input schema for anti and semi joins.

            let output_schema = left.schema();

            Ok(Self {
                left,
                right,
                left_on,
                right_on,
                null_equals_nulls,
                join_type,
                join_strategy,
                output_schema,
            })
        } else {
            let common_join_keys: HashSet<_> =
                get_common_join_keys(left_on.as_slice(), right_on.as_slice())
                    .map(|k| k.to_string())
                    .collect();

            let left_names = left.schema().names();
            let right_names = right.schema().names();

            let mut names_so_far: HashSet<String> = HashSet::from_iter(left_names);

            // rename right columns that have the same name as left columns and are not join keys
            // old_name -> new_name
            let right_rename_mapping: HashMap<_, _> = right_names
                .iter()
                .filter_map(|name| {
                    if !names_so_far.contains(name)
                        || (common_join_keys.contains(name) && !keep_join_keys)
                    {
                        None
                    } else {
                        let mut new_name = name.clone();
                        while names_so_far.contains(&new_name) {
                            new_name = match (join_prefix, join_suffix) {
                                (Some(prefix), Some(suffix)) => {
                                    format!("{}{}{}", prefix, new_name, suffix)
                                }
                                (Some(prefix), None) => {
                                    format!("{}{}", prefix, new_name)
                                }
                                (None, Some(suffix)) => {
                                    format!("{}{}", new_name, suffix)
                                }
                                (None, None) => {
                                    format!("right.{}", new_name)
                                }
                            };
                        }
                        names_so_far.insert(new_name.clone());

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

                let new_right: LogicalPlan = Project::try_new(right, new_right_projection)?.into();

                let right_on_replace_map = right_rename_mapping
                    .iter()
                    .map(|(old_name, new_name)| (old_name.clone(), col(new_name.clone())))
                    .collect::<HashMap<_, _>>();

                // change any column references in the right_on expressions to the new column names
                let new_right_on = right_on
                    .into_iter()
                    .map(|expr| replace_columns_with_expressions(expr, &right_on_replace_map))
                    .collect::<Vec<_>>();

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
                null_equals_nulls,
                join_type,
                join_strategy,
                output_schema,
            })
        }
    }

    /// Renames join keys for the given left and right expressions. This is required to
    /// prevent errors when the join keys on the left and right expressions have the same key
    /// name.
    ///
    /// This function takes two vectors of expressions (`left_exprs` and `right_exprs`) and
    /// checks for pairs of column expressions that differ. If both expressions in a pair
    /// are column expressions and they are not identical, it generates a unique identifier
    /// and renames both expressions by appending this identifier to their original names.
    ///
    /// The function returns two vectors of expressions, where the renamed expressions are
    /// substituted for the original expressions in the cases where renaming occurred.
    ///
    /// # Parameters
    /// - `left_exprs`: A vector of expressions from the left side of a join.
    /// - `right_exprs`: A vector of expressions from the right side of a join.
    ///
    /// # Returns
    /// A tuple containing two vectors of expressions, one for the left side and one for the
    /// right side, where expressions that needed to be renamed have been modified.
    ///
    /// # Example
    /// ```
    /// let (renamed_left, renamed_right) = rename_join_keys(left_expressions, right_expressions);
    /// ```
    ///
    /// For more details, see [issue #2649](https://github.com/Eventual-Inc/Daft/issues/2649).

    fn rename_join_keys(
        left_exprs: Vec<Arc<Expr>>,
        right_exprs: Vec<Arc<Expr>>,
    ) -> (Vec<Arc<Expr>>, Vec<Arc<Expr>>) {
        left_exprs
            .into_iter()
            .zip(right_exprs)
            .map(
                |(left_expr, right_expr)| match (&*left_expr, &*right_expr) {
                    (Expr::Column(left_name), Expr::Column(right_name))
                        if left_name == right_name =>
                    {
                        (left_expr, right_expr)
                    }
                    _ => {
                        let unique_id = Uuid::new_v4().to_string();

                        let renamed_left_expr =
                            left_expr.alias(format!("{}_{}", left_expr.name(), unique_id));
                        let renamed_right_expr =
                            right_expr.alias(format!("{}_{}", right_expr.name(), unique_id));
                        (renamed_left_expr, renamed_right_expr)
                    }
                },
            )
            .unzip()
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
        if let Some(null_equals_nulls) = &self.null_equals_nulls {
            res.push(format!(
                "Null equals Nulls = [{}]",
                null_equals_nulls.iter().map(|b| b.to_string()).join(", ")
            ));
        }
        res.push(format!(
            "Output schema = {}",
            self.output_schema.short_string()
        ));
        res
    }
}
