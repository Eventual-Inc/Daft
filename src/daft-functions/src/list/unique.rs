use std::{any::Any, sync::Arc};

use arrow2::{compute::cast::cast, datatypes::DataType as ArrowDataType};
use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ListArray,
    datatypes::{DataType, Field, Int64Array},
    prelude::Schema,
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListUnique;

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
                let field = Arc::new(Field::new(input.name(), input.data_type().clone()));
                match input.data_type() {
                    DataType::List(inner_type) | DataType::FixedSizeList(inner_type, _) => {
                        // Convert fixed size list to regular list if needed
                        let input = if let DataType::FixedSizeList(_, _) = input.data_type() {
                            let fixed_list = input.fixed_size_list()?;
                            let arrow_array = fixed_list.to_arrow();
                            let list_type =
                                ArrowDataType::List(Box::new(arrow2::datatypes::Field::new(
                                    "item",
                                    inner_type.as_ref().to_arrow()?,
                                    true,
                                )));
                            let list_array =
                                cast(arrow_array.as_ref(), &list_type, Default::default())?;
                            Series::try_from_field_and_arrow_array(
                                Field::new(field.name.as_str(), DataType::List(inner_type.clone())),
                                list_array,
                            )?
                        } else {
                            input.clone()
                        };

                        let mut result = Vec::new();
                        let mut offsets = Vec::new();
                        let mut validity = Vec::new();
                        offsets.push(0i64);
                        let mut current_offset = 0i64;

                        for sub_series in input.list()? {
                            if let Some(sub_series) = sub_series {
                                let unique_values = sub_series
                                    .build_probe_table_without_nulls()
                                    .expect("Building the probe table should always work");
                                let mut unique_series = Vec::new();
                                let sub_array = sub_series.i64()?;

                                // Collect and sort indices to preserve original order
                                let mut indices: Vec<_> =
                                    unique_values.keys().map(|k| k.idx).collect();
                                indices.sort_unstable();

                                for idx in indices {
                                    if let Some(value) = sub_array.get(idx as usize) {
                                        unique_series.push(value);
                                    }
                                }
                                current_offset += unique_series.len() as i64;
                                offsets.push(current_offset);
                                result.extend(unique_series);
                                validity.push(true);
                            } else {
                                offsets.push(current_offset);
                                validity.push(false);
                            }
                        }

                        let values = Int64Array::from_values("", result.into_iter());

                        let list_array = ListArray::new(
                            Field::new(field.name.as_str(), DataType::List(inner_type.clone())),
                            values.into_series(),
                            arrow2::offset::OffsetsBuffer::try_from(offsets)?,
                            Some(arrow2::bitmap::Bitmap::from_iter(validity)),
                        );

                        Ok(list_array.into_series())
                    }
                    _ => Err(DaftError::TypeError(format!(
                        "Expected list input, got {}",
                        input.data_type()
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn list_unique(expr: ExprRef) -> ExprRef {
    ScalarFunction::new(ListUnique, vec![expr]).into()
}
