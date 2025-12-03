use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::{prelude::*, utils::supertype::try_get_supertype};
use daft_dsl::expr::bound_expr::BoundExpr;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn unpivot(
        &self,
        ids: &[BoundExpr],
        values: &[BoundExpr],
        variable_name: &str,
        value_name: &str,
    ) -> DaftResult<Self> {
        match self.concat_or_get()? {
            None => {
                if values.is_empty() {
                    return Err(DaftError::ValueError(
                        "Unpivot requires at least one value column".to_string(),
                    ));
                }

                let values_dtype = values
                    .iter()
                    .map(|e| e.inner().to_field(&self.schema).map(|f| f.dtype))
                    .reduce(|l, r| try_get_supertype(&l?, &r?))
                    .unwrap()?;

                let fields = ids
                    .iter()
                    .map(|e| e.inner().to_field(&self.schema))
                    .chain(vec![
                        Ok(Field::new(variable_name, DataType::Utf8)),
                        Ok(Field::new(value_name, values_dtype)),
                    ])
                    .collect::<DaftResult<Vec<_>>>()?;

                Ok(Self::empty(Some(Arc::new(Schema::new(fields)))))
            }
            Some(t) => {
                let unpivoted = t.unpivot(ids, values, variable_name, value_name)?;
                Ok(Self::new_loaded(
                    unpivoted.schema.clone(),
                    vec![unpivoted].into(),
                    None,
                ))
            }
        }
    }
}
