use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{BooleanArray, DaftLogical, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{functions::ScalarUDF, ExprRef};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Coalesce {}

#[typetag::serde]
impl ScalarUDF for Coalesce {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "coalesce"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [] => Err(DaftError::SchemaMismatch(
                "Expected at least 1 input args, got 0".to_string(),
            )),
            [input] => {
                let input_field = input.to_field(schema)?;
                Ok(input_field)
            }
            _ => {
                let first_field = inputs[0].to_field(schema)?;

                for input in inputs {
                    if input.to_field(schema)?.dtype != first_field.dtype {
                        return Err(DaftError::SchemaMismatch(
                            "All input fields must have the same data type".to_string(),
                        ));
                    }
                }
                Ok(first_field)
            }
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs.len() {
            0 => Err(DaftError::ComputeError("No inputs provided".to_string())),
            1 => Ok(inputs[0].clone()),
            _ => {
                let name = inputs[0].name();
                let dtype = inputs[0].data_type();
                let len = inputs[0].len();
                // the first input is not null, so no work to do
                if inputs[0].validity().is_none() {
                    return Ok(inputs[0].clone());
                }

                let mut current_value = Series::full_null(name, dtype, len);
                let remainder = BooleanArray::from_values(name, vec![true; len].into_iter());
                let mut remainder = remainder.into_series();

                for input in inputs {
                    let to_apply = remainder.and(&input.not_null()?)?;
                    current_value = input.if_else(&current_value, &to_apply)?;

                    remainder = remainder.and(&input.is_null()?)?;
                }

                Ok(current_value.rename(name))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use daft_core::{
        prelude::{DataType, Field, FullNull, Int8Array, Utf8Array},
        series::{IntoSeries, Series},
    };
    use daft_dsl::functions::ScalarUDF;

    #[test]
    fn test_coalesce_0() {
        let s0 = Int8Array::from_iter(
            Field::new("s0", DataType::Int8),
            vec![None, None, Some(10), Some(11), None].into_iter(),
        )
        .into_series();
        let s1 = Int8Array::from_iter(
            Field::new("s1", DataType::Int8),
            vec![None, Some(2), Some(3), None, None].into_iter(),
        )
        .into_series();
        let s2 = Int8Array::from_iter(
            Field::new("s2", DataType::Int8),
            vec![None, Some(1), Some(4), Some(4), Some(10)].into_iter(),
        )
        .into_series();

        let coalesce = super::Coalesce {};
        let output = coalesce.evaluate(&[s0, s1, s2]).unwrap();
        let actual = output.i8().unwrap();
        let expected = Int8Array::from_iter(
            Field::new("s0", DataType::Int8),
            vec![None, Some(2), Some(10), Some(11), Some(10)].into_iter(),
        );

        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_coalesce_1() {
        let s0 = Int8Array::from_iter(
            Field::new("s0", DataType::Int8),
            vec![None, None, Some(10), Some(11), None].into_iter(),
        )
        .into_series();

        let s1 = Int8Array::from_iter(
            Field::new("s1", DataType::Int8),
            vec![None, Some(2), Some(3), None, None].into_iter(),
        )
        .into_series();

        let coalesce = super::Coalesce {};
        let output = coalesce.evaluate(&[s0, s1]).unwrap();
        let actual = output.i8().unwrap();
        let expected = Int8Array::from_iter(
            Field::new("s0", DataType::Int8),
            vec![None, Some(2), Some(10), Some(11), None].into_iter(),
        );

        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_coalesce_no_args() {
        let coalesce = super::Coalesce {};
        let output = coalesce.evaluate(&[]);

        assert!(output.is_err());
    }

    #[test]
    fn test_coalesce_one_arg() {
        let s0 = Int8Array::from_iter(
            Field::new("s0", DataType::Int8),
            vec![None, None, Some(10), Some(11), None].into_iter(),
        )
        .into_series();

        let coalesce = super::Coalesce {};
        let output = coalesce.evaluate(&[s0.clone()]).unwrap();
        // can't directly compare as null != null
        let output = output.i8().unwrap();
        let s0 = s0.i8().unwrap();
        assert_eq!(output, s0);
    }

    #[test]
    fn test_coalesce_full_nulls() {
        let s0 = Series::full_null("s0", &DataType::Utf8, 100);
        let s1 = Series::full_null("s1", &DataType::Utf8, 100);
        let s2 = Series::full_null("s2", &DataType::Utf8, 100);

        let coalesce = super::Coalesce {};
        let output = coalesce.evaluate(&[s0, s1, s2]).unwrap();
        let actual = output.utf8().unwrap();
        let expected = Utf8Array::full_null("s0", &DataType::Utf8, 100);

        assert_eq!(actual, &expected);
    }
}
