use super::super::Repetition;

use serde::{Deserialize, Serialize};

/// Common type information.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct FieldInfo {
    /// The field name
    pub name: String,
    /// The repetition
    pub repetition: Repetition,
    /// the optional id, to select fields by id
    pub id: Option<i32>,
}
