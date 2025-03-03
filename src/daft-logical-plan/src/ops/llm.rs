use std::sync::Arc;

use daft_core::prelude::*;
use daft_schema::{dtype::DataType, field::Field};
use indexmap::IndexSet;

use crate::{stats::StatsState, LogicalPlan};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LLM {
    // Upstream node.
    pub input: Arc<LogicalPlan>,

    // Output column field
    pub output_field_name: String,
    pub output_schema: SchemaRef,

    // A prompt comprised of an ordered list of fragments
    prompt: String,
    required_cols: Vec<String>,

    pub stats_state: StatsState,
}

impl LLM {
    pub(crate) fn new(
        input: Arc<LogicalPlan>,
        prompt: &str,
        output_field_name: &str,
        output_field_dtype: &DataType,
    ) -> Self {
        let output_schema = Schema::new(
            input
                .schema()
                .fields
                .iter()
                .filter_map(|(f_name, f)| {
                    if f_name.as_str() != output_field_name {
                        Some(f.clone())
                    } else {
                        None
                    }
                })
                .chain(std::iter::once(Field::new(
                    output_field_name,
                    output_field_dtype.clone(),
                )))
                .collect(),
        )
        .unwrap();

        // Parse prompt string for template variables, handling escaped braces
        let mut required_cols = IndexSet::new();
        let mut in_var = false;
        let mut escaped = false;
        let mut curr_var = String::new();
        for c in prompt.chars() {
            match (escaped, in_var, c) {
                (false, false, '\\') => escaped = true,
                (false, false, '{') => in_var = true,
                (false, true, '}') => {
                    if !curr_var.is_empty() {
                        required_cols.insert(curr_var.clone());
                        curr_var.clear();
                    }
                    in_var = false;
                }
                (true, _, _c) => escaped = false,
                (false, true, c) => curr_var.push(c),
                _ => {}
            }
        }

        Self {
            input,
            output_field_name: output_field_name.to_string(),
            output_schema: Arc::new(output_schema),
            prompt: prompt.to_string(),
            stats_state: StatsState::NotMaterialized,
            required_cols: required_cols.into_iter().collect(),
        }
    }

    pub fn required_columns(&self) -> Vec<IndexSet<String>> {
        vec![self.required_cols.iter().cloned().collect()]
    }

    pub fn prompt(&self) -> &str {
        self.prompt.as_str()
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        // TODO(desmond): We can do better estimations with the projection schema. For now, reuse the old logic.
        let input_stats = self.input.materialized_stats();
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![format!("LLM")];
        res.push(format!("output_field = {}", self.output_field_name));

        // Reconstruct full prompt from fragments
        res.push(format!("prompt = {}", self.prompt()));

        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
