use common_error::{DaftError, DaftResult};

use crate::series::{Series, array_impl::IntoSeries};

impl Series {
    pub fn hll_cardinality(&self) -> DaftResult<Self> {
        match self.data_type() {
            crate::prelude::DataType::FixedSizeBinary(_) => {
                Ok(self.fixed_size_binary()?.hll_cardinality()?.into_series())
            }
            other => Err(DaftError::TypeError(format!(
                "hll_cardinality is not implemented for type {}",
                other
            ))),
        }
    }
}
