use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct EndsWith;

pub fn endswith(input: ExprRef, pattern: ExprRef) -> ExprRef {
    ScalarFunction::new(EndsWith, vec![input, pattern]).into()
}

#[typetag::serde]
impl ScalarUDF for EndsWith {
    fn name(&self) -> &'static str {
        "endswith"
    }
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let pattern = inputs.required((1, "pattern"))?;
        series_endswith(input, pattern)
    }
    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 2, SchemaMismatch: "contains expects 2 arguments");

        let input = inputs.required((0, "input"))?.to_field(schema)?;

        ensure!(input.dtype == DataType::Utf8, TypeError: "input must be of type Utf8");

        let pattern = inputs.required((1, "pattern"))?.to_field(schema)?;
        ensure!(
            pattern.dtype == DataType::Utf8,
            TypeError: "pattern must be of type Utf8"
        );
        Ok(Field::new(input.name, DataType::Boolean))
    }
    fn docstring(&self) -> &'static str {
        "Returns a boolean indicating whether each string ends with the specified pattern"
    }
}

pub fn series_endswith(s: &Series, pattern: &Series) -> DaftResult<Series> {
    s.with_utf8_array(|arr| {
        pattern.with_utf8_array(|pattern_arr| {
            arr.binary_broadcasted_compare(
                pattern_arr,
                |data: &str, pat: &str| Ok(data.ends_with(pat)),
                "endswith",
            )
            .map(IntoSeries::into_series)
        })
    })
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use daft_core::{
        prelude::{AsArrow, Utf8Array},
        series::IntoSeries,
    };

    #[test]
    fn check_endswith_utf_arrays_broadcast() -> DaftResult<()> {
        let data = Utf8Array::from((
            "data",
            Box::new(arrow2::array::Utf8Array::<i64>::from(vec![
                "x_foo".into(),
                "y_foo".into(),
                "z_bar".into(),
            ])),
        ))
        .into_series();
        let pattern = Utf8Array::from((
            "pattern",
            Box::new(arrow2::array::Utf8Array::<i64>::from(vec!["foo".into()])),
        ))
        .into_series();
        let result = super::series_endswith(&data, &pattern)?;
        let result = result.bool()?;

        assert_eq!(result.len(), 3);
        assert!(result.as_arrow().value(0));
        assert!(result.as_arrow().value(1));
        assert!(!result.as_arrow().value(2));
        Ok(())
    }

    #[test]
    fn check_endswith_utf_arrays() -> DaftResult<()> {
        let data = Utf8Array::from((
            "data",
            Box::new(arrow2::array::Utf8Array::<i64>::from(vec![
                "x_foo".into(),
                "y_foo".into(),
                "z_bar".into(),
            ])),
        ))
        .into_series();
        let pattern = Utf8Array::from((
            "pattern",
            Box::new(arrow2::array::Utf8Array::<i64>::from(vec![
                "foo".into(),
                "wrong".into(),
                "bar".into(),
            ])),
        ))
        .into_series();
        let result = super::series_endswith(&data, &pattern)?;

        let result = result.bool()?;

        assert_eq!(result.len(), 3);
        assert!(result.as_arrow().value(0));
        assert!(!result.as_arrow().value(1));
        assert!(result.as_arrow().value(2));
        Ok(())
    }
}
