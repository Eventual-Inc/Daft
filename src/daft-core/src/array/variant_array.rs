use common_error::DaftResult;

use crate::{
    prelude::{LogicalArray, VariantType},
    series::Series,
};

pub type VariantArray = LogicalArray<VariantType>;

impl VariantArray {
    pub fn values(&self) -> &Series {
        &self.physical.children[0]
    }

    pub fn metadata(&self) -> &Series {
        &self.physical.children[1]
    }

    pub fn metadata_binary(&self) -> DaftResult<&crate::prelude::BinaryArray> {
        self.metadata().binary()
    }

    pub fn values_binary(&self) -> DaftResult<&crate::prelude::BinaryArray> {
        self.values().binary()
    }
}
