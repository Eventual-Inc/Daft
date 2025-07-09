use common_error::{ensure, DaftResult};
use daft_core::{
    prelude::{AsArrow, Field, Schema, Utf8Array},
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

#[derive(FunctionArgs)]
struct NormalizeArgs<T> {
    input: T,
    #[arg(optional)]
    remove_punct: Option<bool>,
    #[arg(optional)]
    lowercase: Option<bool>,
    #[arg(optional)]
    nfd_unicode: Option<bool>,
    #[arg(optional)]
    white_space: Option<bool>,
}

#[typetag::serde]
impl ScalarUDF for Normalize {
    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let args: NormalizeArgs<Series> = inputs.try_into()?;

        normalize_impl(args)
    }

    fn name(&self) -> &'static str {
        "normalize"
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let NormalizeArgs { input, .. } = inputs.try_into()?;

        let input = input.to_field(schema)?;

        ensure!(input.dtype.is_string(), TypeError: "Expected string type for input argument");

        Ok(input)
    }
}

fn normalize_impl(
    NormalizeArgs {
        input,
        remove_punct,
        lowercase,
        nfd_unicode,
        white_space,
    }: NormalizeArgs<Series>,
) -> DaftResult<Series> {
    let white_space = white_space.unwrap_or(false);
    let lowercase = lowercase.unwrap_or(false);
    let nfd_unicode = nfd_unicode.unwrap_or(false);
    let remove_punct = remove_punct.unwrap_or(false);

    input.with_utf8_array(|arr| {
        Ok(Utf8Array::from_iter(
            arr.name(),
            arr.as_arrow().iter().map(|maybe_s| {
                if let Some(s) = maybe_s {
                    let mut s = if white_space {
                        s.trim().to_string()
                    } else {
                        s.to_string()
                    };

                    let mut prev_white = true;
                    s = s
                        .chars()
                        .filter_map(|c| {
                            if !(remove_punct && c.is_ascii_punctuation()
                                || white_space && c.is_whitespace())
                            {
                                prev_white = false;
                                Some(c)
                            } else if prev_white || (remove_punct && c.is_ascii_punctuation()) {
                                None
                            } else {
                                prev_white = true;
                                Some(' ')
                            }
                        })
                        .collect();

                    if lowercase {
                        s = s.to_lowercase();
                    }

                    if nfd_unicode && is_nfd_quick(s.chars()) != IsNormalized::Yes {
                        s = s.nfd().collect();
                    }
                    Some(s)
                } else {
                    None
                }
            }),
        )
        .into_series())
    })
}
