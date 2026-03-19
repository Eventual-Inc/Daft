use derive_more::Display;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Display, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum UnionMode {
    Sparse,
    Dense,
}

impl UnionMode {
    pub fn to_arrow(&self) -> arrow_schema::UnionMode {
        match self {
            Self::Sparse => arrow_schema::UnionMode::Sparse,
            Self::Dense => arrow_schema::UnionMode::Dense,
        }
    }

    pub fn is_dense(&self) -> bool {
        matches!(self, Self::Dense)
    }
}
