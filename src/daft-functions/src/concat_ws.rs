use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{
        FunctionArgs, ScalarUDF,
        scalar::{EvalContext, ScalarFn},
    },
    lit,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ConcatWs;

#[derive(FunctionArgs)]
struct ConcatWsArgs<T> {
    sep: String,
    #[arg(variadic)]
    exprs: Vec<T>,
}

#[typetag::serde]
impl ScalarUDF for ConcatWs {
    fn name(&self) -> &'static str {
        "concat_ws"
    }
    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let ConcatWsArgs { sep, exprs } = inputs.try_into()?;

        if exprs.is_empty() {
            return Err(DaftError::ValueError(
                "concat_ws requires at least one expression argument".to_string(),
            ));
        }

        let len = exprs.iter().map(|s| s.len()).max().unwrap_or(0);
        let name = exprs[0].name().to_string();

        let string_cols: Vec<Vec<Option<String>>> = exprs
            .iter()
            .map(|s| {
                if s.data_type() == &DataType::Null {
                    Ok(vec![None; s.len()])
                } else {
                    let arr = s.utf8()?;
                    Ok(arr.into_iter().map(|v| v.map(|s| s.to_string())).collect())
                }
            })
            .collect::<DaftResult<_>>()?;

        let result = Utf8Array::from_iter(
            &name,
            (0..len).map(|i| {
                let parts: Vec<&str> = string_cols
                    .iter()
                    .filter_map(|col| {
                        let idx = if col.len() == 1 { 0 } else { i };
                        col[idx].as_deref()
                    })
                    .collect();

                // If every input at this row was null, the result is null.
                if parts.is_empty() {
                    None
                } else {
                    Some(parts.join(&sep))
                }
            }),
        );
        Ok(result.into_series())
    }
    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let ConcatWsArgs { exprs, .. } = inputs.try_into()?;

        if exprs.is_empty() {
            return Err(DaftError::ValueError(
                "concat_ws requires at least one expression argument".to_string(),
            ));
        }
        for expr in &exprs {
            let field = expr.to_field(schema)?;
            match &field.dtype {
                DataType::Utf8 | DataType::Null => {}
                dt => {
                    return Err(DaftError::TypeError(format!(
                        "concat_ws expects string inputs, but received {dt}"
                    )));
                }
            }
        }
        let name = exprs[0].to_field(schema)?.name;
        Ok(Field::new(name, DataType::Utf8))
    }
    fn docstring(&self) -> &'static str {
        "Concatenates strings with a separator, skipping nulls."
    }
}

pub fn concat_ws(sep: &str, exprs: Vec<ExprRef>) -> ExprRef {
    let mut inputs = vec![lit(sep)];
    inputs.extend(exprs);
    ScalarFn::builtin(ConcatWs, inputs).into()
}
