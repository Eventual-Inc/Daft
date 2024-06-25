use std::collections::HashMap;

use daft_core::array::ops::{DaftCompare, DaftLogical};
use daft_dsl::{ExprRef, Literal};
use daft_table::Table;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionSpec {
    pub keys: Table,
}

impl PartitionSpec {
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Keys = {}", self.keys));
        res
    }

    pub fn to_fill_map(&self) -> HashMap<&str, ExprRef> {
        self.keys
            .schema
            .fields
            .iter()
            .map(|(col, _)| {
                (
                    col.as_str(),
                    self.keys.get_column(col).unwrap().clone().lit(),
                )
            })
            .collect()
    }
}

impl PartialEq for PartitionSpec {
    fn eq(&self, other: &Self) -> bool {
        // If the names of fields or types of fields don't match, return False
        if self.keys.schema != other.keys.schema {
            return false;
        }

        // Assuming exact matches in field names and types, now compare each field's values
        for field_name in self.keys.schema.as_ref().fields.keys() {
            let self_column = self.keys.get_column(field_name).unwrap();
            let other_column = other.keys.get_column(field_name).unwrap();
            if let Some(value_eq) = self_column.equal(other_column).unwrap().get(0) {
                if !value_eq {
                    return false;
                }
            } else {
                // For partition spec, we treat null as equal to null, in order to allow for
                // partitioning on columns that may have nulls.
                let self_null = self_column.is_null().unwrap();
                let other_null = other_column.is_null().unwrap();
                if self_null
                    .xor(&other_null)
                    .unwrap()
                    .bool()
                    .unwrap()
                    .get(0)
                    .unwrap()
                {
                    return false;
                }
            }
        }

        true
    }
}

impl Eq for PartitionSpec {}
