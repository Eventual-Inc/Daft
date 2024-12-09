use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::{DaftError, DaftResult};
use daft_core::{prelude::SchemaRef, RecordBatch};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InMemoryScan {
    pub record_batches: Vec<RecordBatch>,
    pub schema: SchemaRef,
}
impl InMemoryScan {
    pub fn try_new(record_batches: Vec<RecordBatch>) -> DaftResult<Self> {
        let schema = record_batches[0].schema().clone();
        if record_batches.iter().any(|batch| batch.schema() != &schema) {
            return Err(DaftError::ComputeError(
                "All record batches must have the same schema".to_string(),
            ));
        }

        Ok(Self {
            record_batches,
            schema,
        })
    }
    pub fn multiline_display(&self) -> Vec<String> {
        let size_bytes = self
            .record_batches
            .iter()
            .map(|batch| batch.size_bytes().unwrap_or(0))
            .sum::<usize>();
        let mut res = vec![];
        res.push("InMemoryScan:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res.push(format!("Size bytes = {}", size_bytes));

        res
    }
    pub fn num_rows(&self) -> usize {
        self.record_batches
            .first()
            .map(|batch| batch.num_rows())
            .unwrap_or(0)
    }
    pub fn size_bytes(&self) -> usize {
        self.record_batches
            .iter()
            .map(|batch| batch.size_bytes().unwrap_or(0))
            .sum()
    }
}

impl TreeDisplay for InMemoryScan {
    fn display_as(&self, level: DisplayLevel) -> String {
        let size_bytes = self
            .record_batches
            .iter()
            .map(|batch| batch.size_bytes().unwrap_or(0))
            .sum::<usize>();
        match level {
            DisplayLevel::Compact => self.get_name(),
            DisplayLevel::Default => {
                format!(
                    "InMemoryScan:
Schema = {},
Size bytes = {}",
                    self.schema.short_string(),
                    size_bytes,
                )
            }
            DisplayLevel::Verbose => todo!(),
        }
    }

    fn get_name(&self) -> String {
        "InMemoryScan".to_string()
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![]
    }
}
