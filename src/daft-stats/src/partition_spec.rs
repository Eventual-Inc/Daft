use daft_core::array::ops::DaftCompare;
use daft_table::Table;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionSpec {
    pub keys: Table,
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
            let value_eq = self_column.equal(other_column).unwrap().get(0).unwrap();
            if !value_eq {
                return false;
            }
        }

        true
    }
}

impl Eq for PartitionSpec {}
