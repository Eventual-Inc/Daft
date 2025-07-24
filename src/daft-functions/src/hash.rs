use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::functions::{prelude::*, ScalarFunction};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct HashFunction;

#[derive(FunctionArgs)]
struct Args<T> {
    input: T,

    #[arg(optional)]
    seed: Option<T>,
}

#[typetag::serde]
impl ScalarUDF for HashFunction {
    fn name(&self) -> &'static str {
        "hash"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args { input, seed } = inputs.try_into()?;
        if let Some(seed) = seed {
            match seed.len() {
                1 if seed.data_type().is_list() => {
                    let seed = seed.list()?;
                    ensure!(
                        seed.len() == 1,
                        "Seed must be a single value or the same length as the input"
                    );
                    let seed = seed.get(0).unwrap();
                    ensure!(
                        seed.len() == input.len(),
                        ValueError: "Seed must be a single value or the same length as the input"
                    );
                    let seed = seed.cast(&DataType::UInt64)?;
                    let seed = seed.u64().unwrap();
                    input.hash(Some(seed)).map(IntoSeries::into_series)
                }
                1 => {
                    let seed = seed.cast(&DataType::UInt64)?;
                    // There's no way to natively extend the array, so we extract the element and repeat it.
                    let seed = seed.u64().unwrap();
                    let seed = seed.get(0).unwrap();
                    let seed = UInt64Array::from_iter(
                        Field::new("seed", DataType::UInt64),
                        std::iter::repeat_n(Some(seed), input.len()),
                    );
                    input.hash(Some(&seed)).map(IntoSeries::into_series)
                }
                _ if seed.len() == input.len() => {
                    let seed = seed.cast(&DataType::UInt64)?;
                    let seed = seed.u64().unwrap();
                    input.hash(Some(seed)).map(IntoSeries::into_series)
                }
                _ => Err(DaftError::ValueError(
                    "Seed must be a single value or the same length as the input".to_string(),
                )),
            }
        } else {
            input.hash(None).map(|arr| arr.into_series())
        }
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args { input, seed } = inputs.try_into()?;
        let input = input.to_field(schema)?;

        if let Some(seed) = seed {
            let seed = seed.to_field(schema)?;
            let cond = (seed.dtype.is_null_or(DataType::is_numeric)
                || matches!(&seed.dtype, DataType::List(inner) if inner.is_numeric() && !inner.is_floating()))
                && !seed.dtype.is_floating();
            ensure!(
                cond,
                TypeError: "seed must be a numeric type"
            );
        }

        Ok(Field::new(input.name, DataType::UInt64))
    }
}

#[must_use]
pub fn hash(input: ExprRef, seed: Option<ExprRef>) -> ExprRef {
    let inputs = match seed {
        Some(seed) => vec![input, seed],
        None => vec![input],
    };

    ScalarFunction::new(HashFunction, inputs).into()
}
