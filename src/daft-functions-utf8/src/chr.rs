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

/// Spark-compatible `chr` function.
/// Converts an ASCII numeric value to a character.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Chr;

#[typetag::serde]
impl ScalarUDF for Chr {
    fn name(&self) -> &'static str {
        "chr"
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
                Ok(chr_impl(input.downcast::<<$T as DaftDataType>::ArrayType>()?)?.into_series())
            })
        } else {
            Err(DaftError::TypeError(format!(
                "chr not implemented for type {}",
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
                TypeError: "Expects input to 'chr' to be integer, but received {}", input.dtype
            );
            Ok(Field::new(input.name, DataType::Utf8))
        }
    }

    fn docstring(&self) -> &'static str {
        "Converts an ASCII numeric value to a character. Returns the character corresponding to the ASCII code."
    }
}

#[must_use]
pub fn chr(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Chr {}, vec![input]).into()
}

fn chr_impl<I>(arr: &DataArray<I>) -> DaftResult<Utf8Array>
where
    I: DaftIntegerType,
    <I as DaftNumericType>::Native: Ord + std::hash::Hash,
{
    let result: Utf8Array = arr
        .into_iter()
        .map(|val| {
            val.and_then(|v| {
                let code: u32 = NumCast::from(v)?;
                char::from_u32(code).map(|c| c.to_string())
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
    fn test_chr_basic() {
        let arr = Int64Array::from_slice("a", &[65i64, 97, 48, 32]);
        let result = chr_impl(&arr).unwrap();

        assert_eq!(result.get(0), Some("A"));
        assert_eq!(result.get(1), Some("a"));
        assert_eq!(result.get(2), Some("0"));
        assert_eq!(result.get(3), Some(" "));
    }

    #[test]
    fn test_chr_with_nulls() {
        let arr = Int64Array::from_iter(
            Arc::new(Field::new("a", DataType::Int64)),
            vec![Some(65i64), None, Some(66)].into_iter(),
        );
        let result = chr_impl(&arr).unwrap();

        assert_eq!(result.get(0), Some("A"));
        assert_eq!(result.get(1), None);
        assert_eq!(result.get(2), Some("B"));
    }

    #[test]
    fn test_chr_invalid_code() {
        // Invalid Unicode code point should return None
        let arr = Int64Array::from_slice("a", &[0xD800i64]); // surrogate, invalid
        let result = chr_impl(&arr).unwrap();
        assert_eq!(result.get(0), None);
    }
}
