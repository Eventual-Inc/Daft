use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_schema::schema::{Schema, SchemaRef};
use serde::{Deserialize, Serialize};

use crate::series::Series;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// A RecordBatch is a collection of columns that have the same length.
pub struct RecordBatch {
    schema: SchemaRef,
    pub columns: Vec<Series>,
}

impl std::hash::Hash for RecordBatch {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        // TODO: can we properly hash the columns?
        self.columns.len().hash(state);
    }
}

impl RecordBatch {
    pub fn try_new(columns: Vec<Series>) -> DaftResult<Self> {
        if columns.is_empty() {
            let schema = Arc::new(Schema::empty());
            Ok(Self { schema, columns })
        } else {
            let len = columns.first().map(|col| col.len()).unwrap();
            if columns.iter().any(|col| col.len() != len) {
                return Err(DaftError::ComputeError(
                    "All columns must have the same length".to_string(),
                ));
            }

            let fields = columns
                .iter()
                .map(|col| col.field().clone())
                .collect::<Vec<_>>();
            let schema = Arc::new(Schema::new(fields)?);

            Ok(Self { schema, columns })
        }
    }

    pub fn size_bytes(&self) -> DaftResult<usize> {
        self.columns
            .iter()
            .try_fold(0, |acc, col| col.size_bytes().map(|size| acc + size))
    }
    pub fn num_rows(&self) -> usize {
        self.columns.first().map(|col| col.len()).unwrap_or(0)
    }
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}
