use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::{DataType, Field},
    prelude::{Schema, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::series::SeriesListExtension;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ListFill;

#[typetag::serde]
impl ScalarUDF for ListFill {
    fn name(&self) -> &'static str {
        "list_fill"
    }

    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        match inputs.as_slice() {
            [num, elem] => {
                let num = num.cast(&DataType::Int64)?;
                let num_array = num.i64()?;
                elem.list_fill(num_array)
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let inputs = inputs.into_inner();
        match inputs.as_slice() {
            [n, elem] => {
                let num_field = n.to_field(schema)?;
                let elem_field = elem.to_field(schema)?;
                if !num_field.dtype.is_integer() {
                    return Err(DaftError::TypeError(format!(
                        "Expected num field to be of numeric type, received: {}",
                        num_field.dtype
                    )));
                }
                elem_field.to_list_field()
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn list_fill(n: ExprRef, elem: ExprRef) -> ExprRef {
    ScalarFunction::new(ListFill {}, vec![n, elem]).into()
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use arrow2::offset::OffsetsBuffer;
    use daft_core::{
        array::ListArray,
        datatypes::{Int8Array, Utf8Array},
        series::IntoSeries,
    };
    use daft_dsl::{lit, null_lit};

    use super::*;

    #[test]
    fn test_to_field() {
        let col0_null = null_lit().alias("c0");
        let col0_num = lit(10).alias("c0");
        let col1_null = null_lit().alias("c1");
        let col1_str = lit("abc").alias("c1");

        let schema = Schema::new(vec![
            Field::new("c0", DataType::Int32),
            Field::new("c1", DataType::Utf8),
        ]);

        let fill = ListFill {};
        let DaftError::SchemaMismatch(e) =
            fill.to_field(&[col0_null.clone()], &schema).unwrap_err()
        else {
            panic!("Expected SchemaMismatch error");
        };
        assert_eq!(e, "Expected 2 input args, got 1");
        let DaftError::TypeError(e) = fill
            .to_field(&[col0_null.clone(), col1_str.clone()], &schema)
            .unwrap_err()
        else {
            panic!("Expected TypeError error");
        };
        assert_eq!(
            e,
            "Expected num field to be of numeric type, received: Null"
        );

        let list_of_null = fill
            .to_field(&[col0_num.clone(), col1_null.clone()], &schema)
            .unwrap();
        let expected = Field::new("c1", DataType::List(Box::new(DataType::Null)));
        assert_eq!(list_of_null, expected);
        let list_of_str = fill
            .to_field(&[col0_num.clone(), col1_str.clone()], &schema)
            .unwrap();
        let expected = Field::new("c1", DataType::List(Box::new(DataType::Utf8)));
        assert_eq!(list_of_str, expected);
    }

    #[test]
    fn test_evaluate_with_invalid_input() {
        let fill = ListFill {};
        let num = Int8Array::from_iter(
            Field::new("s0", DataType::Int8),
            vec![Some(1), Some(0), Some(10)].into_iter(),
        )
        .into_series();

        let error = fill.evaluate_from_series(&[num.clone()]).unwrap_err();
        assert_eq!(
            error.to_string(),
            "DaftError::ValueError Expected 2 input args, got 1"
        );
    }

    #[test]
    fn test_evaluate_mismatched_len() {
        let fill = ListFill {};
        let num = Int8Array::from_iter(
            Field::new("s0", DataType::Int8),
            vec![Some(1), Some(0), Some(10), Some(11), Some(7)].into_iter(),
        )
        .into_series();
        let str = Utf8Array::from_iter("s2", vec![None, Some("hello"), Some("world")].into_iter())
            .into_series();
        let error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fill.evaluate_from_series(&[num.clone(), str.clone()])
                .unwrap()
        }));
        assert!(error.is_err());
    }

    #[test]
    fn test_evaluate() -> DaftResult<()> {
        let fill = ListFill {};
        let num = Int8Array::from_iter(
            Field::new("s0", DataType::Int8),
            vec![Some(1), Some(0), Some(3)].into_iter(),
        )
        .into_series();
        let str = Utf8Array::from_iter("s2", vec![None, Some("hello"), Some("world")].into_iter())
            .into_series();
        let result = fill.evaluate_from_series(&[num.clone(), str.clone()])?;
        // the expected result should be a list of strings: [[None], [], ["world", "world", "world"]]
        let flat_child = Utf8Array::from_iter(
            "s2",
            vec![None, Some("world"), Some("world"), Some("world")].into_iter(),
        )
        .into_series();
        let offsets = vec![0, 1, 1, 4];
        let offsets = OffsetsBuffer::try_from(offsets).unwrap();
        let expected = ListArray::new(
            Field::new("s2", DataType::List(Box::new(DataType::Utf8))),
            flat_child,
            offsets,
            None,
        );
        assert_eq!(result.field(), expected.field.as_ref());
        assert_eq!(result.len(), expected.len());
        let result_list = result.list()?;
        assert_eq!(result_list.offsets(), expected.offsets());
        assert_eq!(result_list.validity(), expected.validity());
        assert_eq!(
            result_list
                .flat_child
                .utf8()?
                .into_iter()
                .collect::<Vec<_>>(),
            expected.flat_child.utf8()?.into_iter().collect::<Vec<_>>()
        );
        Ok(())
    }
}
