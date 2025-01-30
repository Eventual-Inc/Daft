use std::{any::Any, sync::Arc};

use arrow2::{compute::cast::cast, datatypes::DataType as ArrowDataType, offset::OffsetsBuffer};
use common_error::{DaftError, DaftResult};
use daft_core::{
    array::{growable::make_growable, ListArray},
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
    ignore_nulls: bool,
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
                let mut offsets = Vec::new();
                offsets.push(0i64);
                let mut current_offset = 0i64;
                let mut result = Vec::new();

                for sub_series in list {
                    if let Some(sub_series) = sub_series {
                        let probe_table = if self.ignore_nulls {
                            sub_series.build_probe_table_without_nulls()?
                        } else {
                            sub_series.build_probe_table_with_nulls()?
                        };

                        let mut indices: Vec<_> = probe_table.keys().map(|k| k.idx).collect();
                        indices.sort_unstable();

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

                let field = Arc::new(input.field().to_exploded_field()?);
                let child_data_type = if let DataType::List(inner_type) = input.data_type() {
                    inner_type.as_ref().clone()
                } else {
                    return Err(DaftError::TypeError("Expected list type".into()));
                };

                if current_offset == 0 {
                    let empty_array = arrow2::array::new_empty_array(child_data_type.to_arrow()?);
                    let list_array = ListArray::new(
                        Arc::new(Field::new(input.name(), input.data_type().clone())),
                        Series::from_arrow(field, empty_array)?,
                        OffsetsBuffer::try_from(offsets)?,
                        input.validity().cloned(),
                    );
                    return Ok(list_array.into_series());
                }

                let result_refs: Vec<&Series> = result.iter().collect();
                let mut growable = make_growable(
                    &field.name,
                    &child_data_type,
                    result_refs,
                    false,
                    current_offset as usize,
                );

                for (i, series) in result.iter().enumerate() {
                    growable.extend(i, 0, series.len());
                }

                let list_array = ListArray::new(
                    Arc::new(Field::new(input.name(), input.data_type().clone())),
                    growable.build()?,
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
/// When ignore_nulls is true (default), nulls are excluded from the result.
/// When ignore_nulls is false, nulls are included in the result.
pub fn list_unique(expr: ExprRef, ignore_nulls: bool) -> ExprRef {
    ScalarFunction::new(ListUnique { ignore_nulls }, vec![expr]).into()
}
