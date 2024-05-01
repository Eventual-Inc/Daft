use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::{datatypes::Field, schema::Schema, utils::supertype::try_get_supertype, DataType};
use daft_dsl::ExprRef;
use daft_io::IOStatsContext;

use crate::micropartition::MicroPartition;

impl MicroPartition {
    pub fn unpivot(
        &self,
        ids: &[ExprRef],
        values: &[ExprRef],
        variable_name: &str,
        value_name: &str,
    ) -> DaftResult<Self> {
        let io_stats = IOStatsContext::new("MicroPartition::unpivot");

        let tables = self.concat_or_get(io_stats)?;

        match tables.as_slice() {
            [] => {
                if values.is_empty() {
                    return Err(DaftError::ValueError(
                        "Unpivot requires at least one value column".to_string(),
                    ));
                }

                let values_dtype = values
                    .iter()
                    .map(|e| e.to_field(&self.schema).map(|f| f.dtype))
                    .reduce(|l, r| try_get_supertype(&l?, &r?))
                    .unwrap()?;

                let fields = ids
                    .iter()
                    .map(|e| e.to_field(&self.schema))
                    .chain(vec![
                        Ok(Field::new(variable_name, DataType::Utf8)),
                        Ok(Field::new(value_name, values_dtype)),
                    ])
                    .collect::<DaftResult<Vec<_>>>()?;

                Ok(Self::empty(Some(Arc::new(Schema::new(fields)?))))
            }
            [t] => {
                let unpivoted = t.unpivot(ids, values, variable_name, value_name)?;
                Ok(Self::new_loaded(
                    unpivoted.schema.clone(),
                    vec![unpivoted].into(),
                    None,
                ))
            }
            _ => unreachable!(),
        }
    }
}
