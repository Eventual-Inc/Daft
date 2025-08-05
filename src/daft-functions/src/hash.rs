use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use daft_hash::HashFunctionKind;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct HashFunction;

#[derive(FunctionArgs)]
struct Args<T> {
    input: T,

    #[arg(optional)]
    seed: Option<T>,

    #[arg(optional)]
    hash_function: Option<String>,
}

#[typetag::serde]
impl ScalarUDF for HashFunction {
    fn name(&self) -> &'static str {
        "hash"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args {
            input,
            seed,
            hash_function,
        } = inputs.try_into()?;

        let hash_function = hash_function
            .map(|s| s.parse::<HashFunctionKind>())
            .transpose()?
            .unwrap_or(HashFunctionKind::XxHash);

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
                    input
                        .hash_with(Some(seed), hash_function)
                        .map(IntoSeries::into_series)
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
                    input
                        .hash_with(Some(&seed), hash_function)
                        .map(IntoSeries::into_series)
                }
                _ if seed.len() == input.len() => {
                    let seed = seed.cast(&DataType::UInt64)?;
                    let seed = seed.u64().unwrap();
                    input
                        .hash_with(Some(seed), hash_function)
                        .map(IntoSeries::into_series)
                }
                _ => Err(DaftError::ValueError(
                    "Seed must be a single value or the same length as the input".to_string(),
                )),
            }
        } else {
            input
                .hash_with(None, hash_function)
                .map(IntoSeries::into_series)
        }
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args { input, seed, .. } = inputs.try_into()?;
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
pub fn hash(input: ExprRef, seed: Option<ExprRef>, hash_function: Option<ExprRef>) -> ExprRef {
    let mut inputs = vec![input];
    if let Some(seed) = seed {
        inputs.push(seed);
    }
    if let Some(hash_function) = hash_function {
        inputs.push(hash_function);
    }

    ScalarFn::builtin(HashFunction, inputs).into()
}
