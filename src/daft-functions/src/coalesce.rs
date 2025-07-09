use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{BooleanArray, DaftLogical, DataType, Field, Schema},
    series::{IntoSeries, Series},
    utils::supertype::try_get_collection_supertype,
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Coalesce;

#[typetag::serde]
impl ScalarUDF for Coalesce {
    fn name(&self) -> &'static str {
        "coalesce"
    }
    /// Coalesce is a special case of the case-when expression.
    ///
    /// SQL-2023 - 6.12 General Rules 2.a
    ///  > If the value of the <search condition> of some <searched when clause> in a <case
    ///  > specification> is True, then the value of the <case expression> is the value of
    ///  > the <result> of the first (leftmost) <searched when clause> whose <search
    ///  > condition> evaluates to True, cast as the declared type of the <case
    ///  > specification>.
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        let inputs = inputs.as_slice();
        match inputs.len() {
            0 => Err(DaftError::ComputeError("No inputs provided".to_string())),
            1 => Ok(inputs[0].clone()),
            _ => {
                // need to re-compute the type from the inputs
                let first = &inputs[0];
                let name = first.name();
                let dtypes = inputs.iter().map(|s| s.data_type());
                let dtype = try_get_collection_supertype(dtypes)?;
                let len = inputs.iter().map(|s| s.len()).max().unwrap_or(0);

                // the first input is not null, so no work to do
                if first.validity().is_none() && first.data_type() != &DataType::Null {
                    return inputs[0].cast(&dtype);
                }

                // coalesce output uses the computed type
                let mut current_value = Series::full_null(name, &dtype, len);
                let remainder = BooleanArray::from_values(name, vec![true; len].into_iter());
                let all_false = BooleanArray::from_values(name, vec![false; len].into_iter());
                let mut remainder = remainder.into_series();

                for input in inputs {
                    let to_apply = remainder.and(&input.not_null()?)?;
                    // apply explicit cast
                    current_value = input.if_else(&current_value, &to_apply)?.cast(&dtype)?;
                    remainder = remainder.and(&input.is_null()?)?;
                    // exit early if all values are filled
                    if remainder.bool().unwrap() == &all_false {
                        break;
                    }
                }

                Ok(current_value.rename(name))
            }
        }
    }
    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let inputs = inputs.into_inner();
        let inputs = inputs.as_slice();

        match inputs {
            [] => Err(DaftError::SchemaMismatch(
                "Expected at least 1 input args, got 0".to_string(),
            )),
            [input] => {
                let input_field = input.to_field(schema)?;
                Ok(input_field)
            }
            _ => {
                let field_name = inputs[0].to_field(schema)?.name;
                let field_types = inputs
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<DaftResult<Vec<_>>>()?;
                let field_type = try_get_collection_supertype(&field_types)?;
                Ok(Field::new(field_name, field_type))
            }
        }
    }
}

#[must_use]
/// Coalesce returns the first non-null value in a list of expressions.
/// Returns the first non-null value from a sequence of expressions.
///
/// # Arguments
/// * `inputs` - A vector of expressions to evaluate in order
pub fn coalesce(inputs: Vec<ExprRef>) -> ExprRef {
    ScalarFunction::new(Coalesce {}, inputs).into()
}

#[cfg(test)]
mod tests {
    use common_error::DaftError;
    use daft_core::{
        prelude::{DataType, Field, FullNull, Int8Array, Schema, Utf8Array},
        series::{IntoSeries, Series},
    };
    use daft_dsl::{
        functions::{FunctionArgs, ScalarUDF},
        lit, null_lit, resolved_col,
    };

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
        let args = FunctionArgs::new_unnamed(vec![s0, s1, s2]);

        let output = coalesce.call(args).unwrap();
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
        let args = FunctionArgs::new_unnamed(vec![s0, s1]);
        let output = coalesce.call(args).unwrap();
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
        let args = FunctionArgs::new_unnamed(vec![]);
        let output = coalesce.call(args);

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
        let args = FunctionArgs::new_unnamed(vec![s0.clone()]);
        let output = coalesce.call(args).unwrap();
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
        let args = FunctionArgs::new_unnamed(vec![s0.clone(), s1.clone(), s2.clone()]);
        let output = coalesce.call(args).unwrap();
        let actual = output.utf8().unwrap();
        let expected = Utf8Array::full_null("s0", &DataType::Utf8, 100);

        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_coalesce_with_mismatched_types() {
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
        let s2 = Utf8Array::from_iter(
            "s2",
            vec![
                None,
                Some("hello"),
                Some("world"),
                Some("hello"),
                Some("world"),
            ]
            .into_iter(),
        )
        .into_series();

        let coalesce = super::Coalesce {};
        let args = FunctionArgs::new_unnamed(vec![s0.clone(), s1.clone(), s2.clone()]);
        let output = coalesce.call(args).unwrap();

        let expected = Utf8Array::from_iter(
            "s2",
            vec![None, Some("2"), Some("10"), Some("11"), Some("world")].into_iter(),
        );
        assert_eq!(output.utf8().unwrap(), &expected);
    }

    #[test]
    fn test_to_field() {
        let col_0 = null_lit().alias("s0");
        let fallback = lit(0);

        let schema = Schema::new(vec![
            Field::new("s0", DataType::Int32),
            Field::new("s1", DataType::Int32),
        ]);
        let expected = Field::new("s0", DataType::Int32);

        let coalesce = super::Coalesce {};
        let output = coalesce
            .get_return_field(FunctionArgs::new_unnamed(vec![col_0, fallback]), &schema)
            .unwrap();
        assert_eq!(output, expected);
    }

    #[test]
    fn test_to_field_with_mismatched_types() {
        let col_0 = resolved_col("s0");
        let col_1 = resolved_col("s1");
        let fallback = lit("not found");

        let schema = Schema::new(vec![
            Field::new("s0", DataType::Int8),
            Field::new("s1", DataType::Int8),
            Field::new("s2", DataType::Utf8),
        ]);
        let expected = Field::new("s0", DataType::Utf8);

        let coalesce = super::Coalesce {};
        let output = coalesce
            .get_return_field(
                FunctionArgs::new_unnamed(vec![col_0, col_1, fallback]),
                &schema,
            )
            .unwrap();
        assert_eq!(output, expected);
    }

    #[test]
    fn test_to_field_with_incompatible_types() {
        let col_0 = resolved_col("s0");
        let col_1 = resolved_col("s1");
        let col_2 = lit(1u32);

        let schema = Schema::new(vec![
            Field::new("s0", DataType::Date),
            Field::new("s1", DataType::Boolean),
            Field::new("s2", DataType::UInt32),
        ]);
        let expected = "could not determine supertype of Date and Boolean".to_string();
        let coalesce = super::Coalesce {};
        let DaftError::TypeError(e) = coalesce
            .get_return_field(
                FunctionArgs::new_unnamed(vec![col_0, col_1, col_2]),
                &schema,
            )
            .unwrap_err()
        else {
            panic!("Expected error")
        };

        assert_eq!(e, expected);
    }
}
