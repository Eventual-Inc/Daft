use std::{any::Any, sync::Arc};

use arrow2::{compute::cast::cast, datatypes::DataType as ArrowDataType, offset::OffsetsBuffer};
use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ListArray,
    datatypes::{DataType, Field},
    prelude::Schema,
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListUnique {
    include_nulls: bool,
}

#[typetag::serde]
impl ScalarUDF for ListUnique {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "list_unique"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                match field.dtype {
                    DataType::List(inner_type) => {
                        Ok(Field::new(field.name, DataType::List(inner_type)))
                    }
                    DataType::FixedSizeList(inner_type, _) => {
                        Ok(Field::new(field.name, DataType::List(inner_type)))
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expected list input, got {}",
                        field.dtype
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input] => {
                // Convert fixed size list to regular list if needed
                let input = if let DataType::FixedSizeList(inner_type, _) = input.data_type() {
                    let fixed_list = input.fixed_size_list()?;
                    let arrow_array = fixed_list.to_arrow();
                    let list_type = ArrowDataType::List(Box::new(arrow2::datatypes::Field::new(
                        "item",
                        inner_type.as_ref().to_arrow()?,
                        true,
                    )));
                    let list_array = cast(arrow_array.as_ref(), &list_type, Default::default())?;
                    Series::try_from_field_and_arrow_array(
                        Field::new(input.name(), DataType::List(inner_type.clone())),
                        list_array,
                    )?
                } else {
                    input.clone()
                };

                let list = input.list()?;
                let mut result: Vec<Series> = Vec::new();
                let mut offsets = Vec::new();
                offsets.push(0i64);
                let mut current_offset = 0i64;

                for sub_series in list {
                    if let Some(sub_series) = sub_series {
                        let probe_table = if self.include_nulls {
                            sub_series.build_probe_table_with_nulls()?
                        } else {
                            sub_series.build_probe_table_without_nulls()?
                        };

                        // Get indices in sorted order to preserve first occurrence order
                        let mut indices: Vec<_> = probe_table.keys().map(|k| k.idx).collect();
                        indices.sort_unstable();

                        // Create a new series with just the unique values
                        let mut unique_values = Vec::new();
                        for idx in indices {
                            unique_values.push(sub_series.slice(idx as usize, (idx + 1) as usize)?);
                        }

                        current_offset += unique_values.len() as i64;
                        offsets.push(current_offset);
                        result.extend(unique_values);
                    } else {
                        offsets.push(current_offset);
                    }
                }

                // Concatenate all unique values into a single series
                let mut arrow_arrays = Vec::new();
                for series in &result {
                    arrow_arrays.push(series.to_arrow());
                }
                let arrow_refs: Vec<&dyn arrow2::array::Array> =
                    arrow_arrays.iter().map(|a| &**a).collect();
                let concatenated = arrow2::compute::concatenate::concatenate(&arrow_refs)?;

                let list_array = ListArray::new(
                    Arc::new(Field::new(input.name(), input.data_type().clone())),
                    Series::from_arrow(Arc::new(input.field().to_exploded_field()?), concatenated)?,
                    OffsetsBuffer::try_from(offsets)?,
                    input.validity().cloned(),
                );

                Ok(list_array.into_series())
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

/// Returns a list of unique elements in each list, preserving order of first occurrence.
///
/// When include_nulls is false (default), nulls are excluded from the result.
/// When include_nulls is true, nulls are included in the result.
pub fn list_unique(expr: ExprRef, include_nulls: bool) -> ExprRef {
    ScalarFunction::new(ListUnique { include_nulls }, vec![expr]).into()
}
