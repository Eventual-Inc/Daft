use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::DaftCompare,
    prelude::{DaftLogical, Field, Schema},
    series::{IntoSeries, Series},
    utils::supertype::try_get_collection_supertype,
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Greatest;

#[typetag::serde]
impl ScalarUDF for Greatest {
    fn name(&self) -> &'static str {
        "greatest"
    }

    /// Returns the largest of the input values per row, ignoring NULLs.
    /// Returns NULL only when all inputs in a row are NULL. Mirrors Spark's
    /// `Greatest` semantics (`org.apache.spark.sql.catalyst.expressions.Greatest`).
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        compare_inputs(inputs, /* keep_greater = */ true)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        common_return_field(self.name(), inputs, schema)
    }

    fn docstring(&self) -> &'static str {
        "Returns the largest value among the inputs, skipping NULL values. \
         Returns NULL only if all inputs are NULL. Requires at least one argument."
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Least;

#[typetag::serde]
impl ScalarUDF for Least {
    fn name(&self) -> &'static str {
        "least"
    }

    /// Returns the smallest of the input values per row, ignoring NULLs.
    /// Returns NULL only when all inputs in a row are NULL. Mirrors Spark's
    /// `Least` semantics (`org.apache.spark.sql.catalyst.expressions.Least`).
    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        compare_inputs(inputs, /* keep_greater = */ false)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        common_return_field(self.name(), inputs, schema)
    }

    fn docstring(&self) -> &'static str {
        "Returns the smallest value among the inputs, skipping NULL values. \
         Returns NULL only if all inputs are NULL. Requires at least one argument."
    }
}

fn common_return_field(
    fn_name: &str,
    inputs: FunctionArgs<ExprRef>,
    schema: &Schema,
) -> DaftResult<Field> {
    let inputs = inputs.into_inner();
    if inputs.is_empty() {
        return Err(DaftError::SchemaMismatch(format!(
            "Expected at least 1 input arg to {fn_name}, got 0"
        )));
    }
    let field_name = inputs[0].to_field(schema)?.name;
    let field_types = inputs
        .iter()
        .map(|e| e.get_type(schema))
        .collect::<DaftResult<Vec<_>>>()?;
    let field_type = try_get_collection_supertype(&field_types)?;
    Ok(Field::new(field_name, field_type))
}

fn compare_inputs(inputs: FunctionArgs<Series>, keep_greater: bool) -> DaftResult<Series> {
    let inputs = inputs.into_inner();
    let inputs = inputs.as_slice();
    match inputs.len() {
        0 => Err(DaftError::ComputeError(
            "greatest/least requires at least one argument".to_string(),
        )),
        1 => Ok(inputs[0].clone()),
        _ => {
            let name = inputs[0].name().to_string();
            let dtypes = inputs.iter().map(|s| s.data_type());
            let dtype = try_get_collection_supertype(dtypes)?;
            let len = inputs.iter().map(|s| s.len()).max().unwrap_or(0);

            // Promote everything to the common supertype up-front so element-wise
            // comparisons and `if_else` operate on a single dtype.
            let cast_inputs = inputs
                .iter()
                .map(|s| s.cast(&dtype))
                .collect::<DaftResult<Vec<_>>>()?;

            // Start with NULLs of the target dtype; first non-null seeds `current`.
            let mut current = Series::full_null(&name, &dtype, len);

            for next in &cast_inputs {
                // `is_null` / `not_null` always produce non-null Boolean Series.
                let current_is_null = current.is_null()?;
                let next_not_null = next.not_null()?;
                let current_not_null = current.not_null()?;

                // Comparison may yield NULL when either side is NULL, but that
                // case is masked out below by ANDing with `both_non_null`.
                let cmp_arr = if keep_greater {
                    next.gt(&current)?
                } else {
                    next.lt(&current)?
                };

                let current_is_null_b = current_is_null.bool()?;
                let next_not_null_b = next_not_null.bool()?;
                let current_not_null_b = current_not_null.bool()?;

                // Take `next` when:
                //   (a) current is NULL and next is non-null (seeding step), OR
                //   (b) both are non-null AND the comparison favors next.
                let seed_next = current_is_null_b.and(next_not_null_b)?;
                let both_non_null = current_not_null_b.and(next_not_null_b)?;
                let cmp_when_both = both_non_null.and(&cmp_arr)?;
                let take_next = seed_next.or(&cmp_when_both)?;

                current = next.if_else(&current, &take_next.into_series())?;
            }

            Ok(current.rename(&name))
        }
    }
}

#[must_use]
pub fn greatest(inputs: Vec<ExprRef>) -> ExprRef {
    ScalarFn::builtin(Greatest {}, inputs).into()
}

#[must_use]
pub fn least(inputs: Vec<ExprRef>) -> ExprRef {
    ScalarFn::builtin(Least {}, inputs).into()
}

#[cfg(test)]
mod tests {
    use daft_core::{
        prelude::{DataType, Field, FullNull, Int32Array, Schema, Utf8Array},
        series::{IntoSeries, Series},
    };
    use daft_dsl::{
        functions::{FunctionArgs, ScalarUDF, scalar::EvalContext},
        resolved_col,
    };

    use super::{Greatest, Least};

    fn make_int(name: &str, vals: Vec<Option<i32>>) -> Series {
        Int32Array::from_iter(Field::new(name, DataType::Int32), vals.into_iter()).into_series()
    }

    #[test]
    fn test_greatest_basic() {
        // a = [1, None, 3, None]
        // b = [2, 5,    1, None]
        // c = [None, 4, 6, None]
        // greatest = [2, 5, 6, None]
        let a = make_int("a", vec![Some(1), None, Some(3), None]);
        let b = make_int("b", vec![Some(2), Some(5), Some(1), None]);
        let c = make_int("c", vec![None, Some(4), Some(6), None]);
        let ctx = EvalContext { row_count: a.len() };
        let out = Greatest {}
            .call(FunctionArgs::new_unnamed(vec![a, b, c]), &ctx)
            .unwrap();
        let expected = make_int("a", vec![Some(2), Some(5), Some(6), None]);
        assert_eq!(out.i32().unwrap(), expected.i32().unwrap());
    }

    #[test]
    fn test_least_basic() {
        let a = make_int("a", vec![Some(1), None, Some(3), None]);
        let b = make_int("b", vec![Some(2), Some(5), Some(1), None]);
        let c = make_int("c", vec![None, Some(4), Some(6), None]);
        let ctx = EvalContext { row_count: a.len() };
        let out = Least {}
            .call(FunctionArgs::new_unnamed(vec![a, b, c]), &ctx)
            .unwrap();
        let expected = make_int("a", vec![Some(1), Some(4), Some(1), None]);
        assert_eq!(out.i32().unwrap(), expected.i32().unwrap());
    }

    #[test]
    fn test_greatest_single_arg_passthrough() {
        let a = make_int("a", vec![Some(1), None, Some(3)]);
        let ctx = EvalContext { row_count: a.len() };
        let out = Greatest {}
            .call(FunctionArgs::new_unnamed(vec![a.clone()]), &ctx)
            .unwrap();
        assert_eq!(out.i32().unwrap(), a.i32().unwrap());
    }

    #[test]
    fn test_greatest_no_args_errors() {
        let ctx = EvalContext { row_count: 0 };
        let res = Greatest {}.call(FunctionArgs::new_unnamed(vec![]), &ctx);
        assert!(res.is_err());
    }

    #[test]
    fn test_greatest_all_nulls() {
        let a = Series::full_null("a", &DataType::Utf8, 3);
        let b = Series::full_null("b", &DataType::Utf8, 3);
        let ctx = EvalContext { row_count: 3 };
        let out = Greatest {}
            .call(FunctionArgs::new_unnamed(vec![a, b]), &ctx)
            .unwrap();
        let expected = Utf8Array::full_null("a", &DataType::Utf8, 3);
        assert_eq!(out.utf8().unwrap(), &expected);
    }

    #[test]
    fn test_greatest_mixed_supertype() {
        // int + utf8 -> utf8, and string ordering: "10" < "2"
        let a = make_int("a", vec![Some(10), Some(2), None]);
        let b = Utf8Array::from_iter("b", vec![Some("2"), Some("10"), Some("zzz")].into_iter())
            .into_series();
        let ctx = EvalContext { row_count: 3 };
        let out = Greatest {}
            .call(FunctionArgs::new_unnamed(vec![a, b]), &ctx)
            .unwrap();
        // After cast a -> "10", "2", null; b -> "2", "10", "zzz"
        // greatest by lexicographic order: "2", "2", "zzz"
        let expected =
            Utf8Array::from_iter("a", vec![Some("2"), Some("2"), Some("zzz")].into_iter());
        assert_eq!(out.utf8().unwrap(), &expected);
    }

    #[test]
    fn test_to_field_with_supertype() {
        let col_a = resolved_col("a");
        let col_b = resolved_col("b");
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32),
            Field::new("b", DataType::Int64),
        ]);
        let f = Greatest {}
            .get_return_field(FunctionArgs::new_unnamed(vec![col_a, col_b]), &schema)
            .unwrap();
        assert_eq!(f, Field::new("a", DataType::Int64));
    }
}
