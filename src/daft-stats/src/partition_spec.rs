use daft_table::Table;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionSpec {
    pub keys: Table,
}
