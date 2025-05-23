use daft_dsl::functions::prelude::*;

/// FromJson converts a JSON string to a Daft value of ths.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct FromJson {
    schema: SchemaRef,
}

#[typetag::serde]
impl ScalarUDF for FromJson {
    fn name(&self) -> &'static str {
        "from_json"
    }

    fn evaluate(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let s = inputs.required(0)?;
        let arr = s
            .utf8()
            .expect("type should have been validated already during `function_args_to_field`")
            .into_iter()
            .map(|s_opt| s_opt.map(|s| s.to_uppercase()))
            .collect::<Utf8Array>();
        Ok(arr.into_series())
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(inputs.len() == 1, SchemaMismatch: "Expected 1 input, but received {}", inputs.len());
        let input = inputs.required(0)?.to_field(schema)?;
        ensure!(input.dtype.is_string(), "expected string");
        Ok(input)
    }

    // finally we want a brief docstring for the function. This is used when generating the sql documentation.
    fn docstring(&self) -> &'static str {
        "Converts a string to uppercase."
    }
}
