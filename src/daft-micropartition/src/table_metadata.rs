use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct TableMetadata {
    pub length: usize,
}
