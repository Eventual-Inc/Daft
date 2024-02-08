use std::sync::Arc;

use daft_core::{datatypes::Field, schema::Schema, DataType};

use crate::LogicalPlan;

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub struct MonotonicallyIncreasingId {
    pub input: Arc<LogicalPlan>,
    pub schema: Arc<Schema>,
    pub column_name: String,
}

impl MonotonicallyIncreasingId {
    pub(crate) fn new(input: Arc<LogicalPlan>, column_name: Option<&str>) -> Self {
        let column_name = column_name.unwrap_or("id");

        let mut schema_with_id_index_map = input.schema().fields.clone();
        schema_with_id_index_map.insert(
            column_name.to_string(),
            Field::new(column_name, DataType::UInt64),
        );
        let schema_with_id = Schema {
            fields: schema_with_id_index_map,
        };

        Self {
            input,
            schema: Arc::new(schema_with_id),
            column_name: column_name.to_string(),
        }
    }
}
