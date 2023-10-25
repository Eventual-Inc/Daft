use daft_table::Table;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionSpec {
    keys: Table,
}
