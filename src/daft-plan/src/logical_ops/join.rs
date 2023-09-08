use std::{collections::HashSet, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_core::{
    schema::{hash_index_map, Schema, SchemaRef},
    DataType,
};
use daft_dsl::Expr;
use snafu::ResultExt;

use crate::{
    logical_plan::{self, CreationSnafu},
    JoinType, LogicalPlan,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Join {
    // Upstream nodes.
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,

    pub left_on: Vec<Expr>,
    pub right_on: Vec<Expr>,
    pub join_type: JoinType,
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
        std::hash::Hash::hash(&self.output_schema, state);
        state.write_u64(hash_index_map(&self.right_input_mapping))
    }
}

impl Join {
    pub(crate) fn try_new(
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        join_type: JoinType,
    ) -> logical_plan::Result<Self> {
        for (on_exprs, schema) in [(&left_on, left.schema()), (&right_on, right.schema())] {
            let on_fields = on_exprs
                .iter()
                .map(|e| e.to_field(schema.as_ref()))
                .collect::<DaftResult<Vec<_>>>()
                .context(CreationSnafu)?;
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
            let left_join_keys = left_on
                .iter()
                .map(|e| e.name())
                .collect::<common_error::DaftResult<HashSet<_>>>()
                .context(CreationSnafu)?;
            let left_schema = &left.schema().fields;
            let fields = left_schema
                .iter()
                .map(|(_, field)| field)
                .cloned()
                .chain(right.schema().fields.iter().filter_map(|(rname, rfield)| {
                    if left_join_keys.contains(rname.as_str()) {
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
            output_schema,
            right_input_mapping,
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
