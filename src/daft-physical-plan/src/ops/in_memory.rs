use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::{DaftError, DaftResult};
use daft_core::prelude::SchemaRef;
use daft_table::Table;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InMemoryScan {
    pub tables: Vec<Table>,
    pub schema: SchemaRef,
}

impl InMemoryScan {
    pub fn try_new(tables: Vec<Table>) -> DaftResult<Self> {
        let schema = tables[0].schema.clone();
        if tables.iter().any(|batch| batch.schema != schema) {
            return Err(DaftError::ComputeError(
                "All record batches must have the same schema".to_string(),
            ));
        }

        Ok(Self { tables, schema })
    }
    pub fn multiline_display(&self) -> Vec<String> {
        let size_bytes = self
            .tables
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
        self.tables
            .first()
            .map(|batch| batch.num_rows())
            .unwrap_or(0)
    }
    pub fn size_bytes(&self) -> usize {
        self.tables
            .iter()
            .map(|batch| batch.size_bytes().unwrap_or(0))
            .sum()
    }
}

impl TreeDisplay for InMemoryScan {
    fn display_as(&self, level: DisplayLevel) -> String {
        let size_bytes = self
            .tables
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
