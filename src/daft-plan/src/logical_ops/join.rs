use std::{collections::HashSet, sync::Arc};

use common_error::DaftError;
use daft_core::{
    join::{JoinStrategy, JoinType},
    schema::{hash_index_map, Schema, SchemaRef},
    DataType,
};
use daft_dsl::{resolve_exprs, ExprRef};
use itertools::Itertools;
use snafu::ResultExt;

use crate::{
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

    // Joins may rename columns from the right input; this struct tracks those renames.
    // Output name -> Original name
    pub right_input_mapping: indexmap::IndexMap<String, String>,
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
        state.write_u64(hash_index_map(&self.right_input_mapping))
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
        let mut right_input_mapping = indexmap::IndexMap::new();
        // Schema inference ported from existing behaviour for parity,
        // but contains bug https://github.com/Eventual-Inc/Daft/issues/1294
        let output_schema = {
            let left_join_keys = left_on.iter().map(|e| e.name()).collect::<HashSet<_>>();
            let right_join_keys = right_on.iter().map(|e| e.name()).collect::<HashSet<_>>();
            let left_schema = &left.schema().fields;
            let fields = left_schema
                .iter()
                .map(|(_, field)| field)
                .cloned()
                .chain(right.schema().fields.iter().filter_map(|(rname, rfield)| {
                    if (left_join_keys.contains(rname.as_str())
                        && right_join_keys.contains(rname.as_str()))
                        || matches!(join_type, JoinType::Anti | JoinType::Semi)
                    {
                        right_input_mapping.insert(rname.clone(), rname.clone());
                        None
                    } else if left_schema.contains_key(rname) {
                        let new_name = format!("right.{}", rname);
                        right_input_mapping.insert(new_name.clone(), rname.clone());
                        Some(rfield.rename(new_name))
                    } else {
                        right_input_mapping.insert(rname.clone(), rname.clone());
                        Some(rfield.clone())
                    }
                }))
                .collect::<Vec<_>>();
            Schema::new(fields).context(CreationSnafu)?.into()
        };
        Ok(Self {
            left,
            right,
            left_on,
            right_on,
            join_type,
            join_strategy,
            output_schema,
            right_input_mapping,
        })
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
