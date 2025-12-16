use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

use crate::utils::{Utf8ArrayUtils, binary_utf8_evaluate, binary_utf8_to_field};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct EndsWith;

#[typetag::serde]
impl ScalarUDF for EndsWith {
    fn name(&self) -> &'static str {
        "ends_with"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        binary_utf8_evaluate(inputs, "pattern", endswith_impl)
    }
    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        binary_utf8_to_field(
            inputs,
            schema,
            "pattern",
            DataType::is_string,
            self.name(),
            DataType::Boolean,
        )
    }
    fn docstring(&self) -> &'static str {
        "Returns a boolean indicating whether each string ends with the specified pattern"
    }
}

pub fn endswith(input: ExprRef, pattern: ExprRef) -> ExprRef {
    ScalarFn::builtin(EndsWith, vec![input, pattern]).into()
}

fn endswith_impl(s: &Series, pattern: &Series) -> DaftResult<Series> {
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
    use daft_core::{prelude::Utf8Array, series::IntoSeries};

    #[test]
    fn check_endswith_utf_arrays_broadcast() -> DaftResult<()> {
        let data = Utf8Array::from((
            "data",
            Box::new(daft_arrow::array::Utf8Array::<i64>::from(vec![
                "x_foo".into(),
                "y_foo".into(),
                "z_bar".into(),
            ])),
        ))
        .into_series();
        let pattern = Utf8Array::from((
            "pattern",
            Box::new(daft_arrow::array::Utf8Array::<i64>::from(vec![
                "foo".into(),
            ])),
        ))
        .into_series();
        let result = super::endswith_impl(&data, &pattern)?;
        let result = result.bool()?;

        assert_eq!(result.len(), 3);
        assert!(result.get(0).unwrap());
        assert!(result.get(1).unwrap());
        assert!(!result.get(2).unwrap());
        Ok(())
    }

    #[test]
    fn check_endswith_utf_arrays() -> DaftResult<()> {
        let data = Utf8Array::from((
            "data",
            Box::new(daft_arrow::array::Utf8Array::<i64>::from(vec![
                "x_foo".into(),
                "y_foo".into(),
                "z_bar".into(),
            ])),
        ))
        .into_series();
        let pattern = Utf8Array::from((
            "pattern",
            Box::new(daft_arrow::array::Utf8Array::<i64>::from(vec![
                "foo".into(),
                "wrong".into(),
                "bar".into(),
            ])),
        ))
        .into_series();
        let result = super::endswith_impl(&data, &pattern)?;

        let result = result.bool()?;

        assert_eq!(result.len(), 3);
        assert!(result.get(0).unwrap());
        assert!(!result.get(1).unwrap());
        assert!(result.get(2).unwrap());
        Ok(())
    }
}
