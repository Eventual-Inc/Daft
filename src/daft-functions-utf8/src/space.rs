use common_error::{DaftError, DaftResult, ensure};
use daft_core::{
    array::DataArray,
    prelude::{DaftIntegerType, DaftNumericType, DataType, Field, Schema, Utf8Array},
    series::{IntoSeries, Series},
    with_match_integer_daft_types,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use num_traits::NumCast;
use serde::{Deserialize, Serialize};

/// Spark-compatible `space` function.
/// Returns a string consisting of n spaces.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Space;

#[typetag::serde]
impl ScalarUDF for Space {
    fn name(&self) -> &'static str {
        "space"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;

        if input.data_type().is_null() {
            return Ok(Series::full_null(
                input.name(),
                &DataType::Utf8,
                input.len(),
            ));
        }

        if input.data_type().is_integer() {
            with_match_integer_daft_types!(input.data_type(), |$T| {
                Ok(space_impl(input.downcast::<<$T as DaftDataType>::ArrayType>()?)?.into_series())
            })
        } else {
            Err(DaftError::TypeError(format!(
                "space not implemented for type {}",
                input.data_type()
            )))
        }
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input, but received {}", inputs.len());
        let input = inputs.required((0, "input"))?.to_field(schema)?;

        if input.dtype.is_null() {
            Ok(Field::new(input.name, DataType::Null))
        } else {
            ensure!(
                input.dtype.is_integer(),
                TypeError: "Expects input to 'space' to be integer, but received {}", input.dtype
            );
            Ok(Field::new(input.name, DataType::Utf8))
        }
    }

    fn docstring(&self) -> &'static str {
        "Returns a string consisting of n space characters."
    }
}

#[must_use]
pub fn space(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Space {}, vec![input]).into()
}

fn space_impl<I>(arr: &DataArray<I>) -> DaftResult<Utf8Array>
where
    I: DaftIntegerType,
    <I as DaftNumericType>::Native: Ord + std::hash::Hash,
{
    let result: Utf8Array = arr
        .into_iter()
        .map(|val| {
            val.and_then(|v| {
                let n: i64 = NumCast::from(v)?;
                if n < 0 {
                    Some(String::new())
                } else {
                    Some(" ".repeat(n as usize))
                }
            })
        })
        .collect::<Utf8Array>()
        .rename(arr.name());

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::prelude::{Field, Int64Array};

    use super::*;

    #[test]
    fn test_space_basic() {
        let arr = Int64Array::from_slice("a", &[0i64, 1, 3, 5]);
        let result = space_impl(&arr).unwrap();

        assert_eq!(result.get(0), Some(""));
        assert_eq!(result.get(1), Some(" "));
        assert_eq!(result.get(2), Some("   "));
        assert_eq!(result.get(3), Some("     "));
    }

    #[test]
    fn test_space_negative() {
        let arr = Int64Array::from_slice("a", &[-1i64, -5]);
        let result = space_impl(&arr).unwrap();

        assert_eq!(result.get(0), Some(""));
        assert_eq!(result.get(1), Some(""));
    }

    #[test]
    fn test_space_with_nulls() {
        let arr = Int64Array::from_iter(
            Arc::new(Field::new("a", DataType::Int64)),
            vec![Some(2i64), None, Some(1)].into_iter(),
        );
        let result = space_impl(&arr).unwrap();

        assert_eq!(result.get(0), Some("  "));
        assert_eq!(result.get(1), None);
        assert_eq!(result.get(2), Some(" "));
    }
}
