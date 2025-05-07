use common_error::{ensure, DaftError, DaftResult};
use daft_core::{
    prelude::{AsArrow, DataType, Field, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};
use unicode_normalization::{is_nfd_quick, IsNormalized, UnicodeNormalization};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Normalize;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub struct NormalizeOptions {
    pub remove_punct: bool,
    pub lowercase: bool,
    pub nfd_unicode: bool,
    pub white_space: bool,
}

#[typetag::serde]
impl ScalarUDF for Normalize {
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let s = inputs.required((0, "input"))?;
        let remove_punct = evaluate_helper(&inputs, "remove_punct")?;
        let lowercase = evaluate_helper(&inputs, "lowercase")?;
        let nfd_unicode = evaluate_helper(&inputs, "nfd_unicode")?;
        let white_space = evaluate_helper(&inputs, "white_space")?;

        let options = NormalizeOptions {
            remove_punct,
            lowercase,
            nfd_unicode,
            white_space,
        };

        s.with_utf8_array(|arr| Ok(normalize_impl(arr, options)?.into_series()))
    }

    fn name(&self) -> &'static str {
        "normalize"
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        ensure!(
            !inputs.is_empty() && inputs.len() <=5,
            SchemaMismatch: "valid arguments for normalize are [input, remove_punct, lowercase, nfd_unicode, white_space]"
        );
        let input = inputs.required((0, "input"))?.to_field(schema)?;
        ensure!(input.dtype.is_string(), TypeError: "Expected string type for input argument");
        to_field_helper(&inputs, "remove_punct", schema)?;
        to_field_helper(&inputs, "lowercase", schema)?;
        to_field_helper(&inputs, "nfd_unicode", schema)?;
        to_field_helper(&inputs, "white_space", schema)?;

        Ok(input)
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data] => match data.to_field(schema) {
                Ok(data_field) => match &data_field.dtype {
                    DataType::Utf8 => Ok(Field::new(data_field.name, DataType::Utf8)),
                    _ => Err(DaftError::TypeError(format!(
                        "Expects input to normalize to be utf8, but received {data_field}",
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

fn to_field_helper(
    args: &FunctionArgs<ExprRef>,
    flag_name: &'static str,
    schema: &Schema,
) -> DaftResult<()> {
    if let Some(flag) = args
        .optional(flag_name)?
        .map(|f| f.to_field(schema))
        .transpose()?
    {
        ensure!(flag.dtype.is_boolean(), TypeError: "Expected boolean type for {flag_name} argument");
    };
    Ok(())
}
fn evaluate_helper(args: &FunctionArgs<Series>, arg_name: &'static str) -> DaftResult<bool> {
    args.optional(arg_name)?
        .map(|s| {
            ensure!(s.data_type().is_boolean(), ValueError: "{arg_name} must be a boolean");
            ensure!(s.len() == 1, ValueError: "{arg_name} must be a single value");

            Ok(s.bool().unwrap().get(0).unwrap())
        })
        .transpose()
        .map(|v| v.unwrap_or(false))
}

fn normalize_impl(arr: &Utf8Array, opts: NormalizeOptions) -> DaftResult<Utf8Array> {
    Ok(Utf8Array::from_iter(
        arr.name(),
        arr.as_arrow().iter().map(|maybe_s| {
            if let Some(s) = maybe_s {
                let mut s = if opts.white_space {
                    s.trim().to_string()
                } else {
                    s.to_string()
                };

                let mut prev_white = true;
                s = s
                    .chars()
                    .filter_map(|c| {
                        if !(opts.remove_punct && c.is_ascii_punctuation()
                            || opts.white_space && c.is_whitespace())
                        {
                            prev_white = false;
                            Some(c)
                        } else if prev_white || (opts.remove_punct && c.is_ascii_punctuation()) {
                            None
                        } else {
                            prev_white = true;
                            Some(' ')
                        }
                    })
                    .collect();

                if opts.lowercase {
                    s = s.to_lowercase();
                }

                if opts.nfd_unicode && is_nfd_quick(s.chars()) != IsNormalized::Yes {
                    s = s.nfd().collect();
                }
                Some(s)
            } else {
                None
            }
        }),
    ))
}
