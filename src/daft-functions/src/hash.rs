use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use daft_hash::HashFunctionKind;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct HashFunction;

#[derive(FunctionArgs)]
struct Args<T> {
    #[arg(variadic)]
    input: Vec<T>,

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
            mut input,
            seed,
            hash_function,
        } = inputs.try_into()?;

        if input.is_empty() {
            return Err(DaftError::ValueError(
                "hash() requires at least one expression".to_string(),
            ));
        }

        let hash_function = hash_function
            .map(|s| s.parse::<HashFunctionKind>())
            .transpose()?
            .unwrap_or(HashFunctionKind::XxHash3_64);

        if input.len() == 1 {
            let series = input.remove(0);
            return hash_single_column(series, seed, hash_function);
        }

        let first_name = input[0].name().to_string();
        let num_rows = input[0].len();

        let input: DaftResult<Vec<Series>> = input
            .into_iter()
            .map(|series| match series.len() {
                len if len == num_rows => Ok(series),
                1 => series.broadcast(num_rows),
                len => Err(DaftError::ValueError(format!(
                    "hash() requires all inputs to be length 1 or match the first input length. Column {} had length {} vs expected {}",
                    series.name(),
                    len,
                    num_rows
                ))),
            })
            .collect();
        let input = input?;

        let seed = match seed {
            Some(seed_series) => {
                let seed_series = match seed_series.len() {
                    len if len == num_rows => seed_series,
                    1 if !seed_series.data_type().is_list() => seed_series.broadcast(num_rows)?,
                    1 => seed_series,
                    _ => {
                        return Err(DaftError::ValueError(
                            "Seed must be a single value or the same length as the input"
                                .to_string(),
                        ));
                    }
                };
                Some(seed_series)
            }
            None => None,
        };

        let mut column_iter = input.into_iter();
        let first_column = column_iter
            .next()
            .expect("validated batch must contain at least one column");

        let mut hash_array = match seed {
            Some(seed_series) => {
                hash_with_seed(first_column, seed_series, hash_function, &first_name)?
                    .u64()
                    .unwrap()
                    .clone()
            }
            None => first_column.hash_with(None, hash_function)?,
        };

        for column in column_iter {
            hash_array = column.hash_with(Some(&hash_array), hash_function)?;
        }
        Ok(hash_array.into_series().rename(&first_name))
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args { input, seed, .. } = inputs.try_into()?;
        let mut args_iter = input.into_iter();
        let first = args_iter.next().ok_or_else(|| {
            DaftError::ValueError("hash() requires at least one expression".to_string())
        })?;
        let input = first.to_field(schema)?;

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

fn hash_single_column(
    series: Series,
    seed: Option<Series>,
    hash_function: HashFunctionKind,
) -> DaftResult<Series> {
    let name = series.name().to_string();
    match seed {
        Some(seed_series) => hash_with_seed(series, seed_series, hash_function, &name),
        None => series
            .hash_with(None, hash_function)
            .map(|arr| arr.into_series().rename(&name)),
    }
}

fn hash_with_seed(
    series: Series,
    seed: Series,
    hash_function: HashFunctionKind,
    name: &str,
) -> DaftResult<Series> {
    match seed.len() {
        1 if seed.data_type().is_list() => {
            let seed = seed.list()?;
            ensure!(
                seed.len() == 1,
                "Seed must be a single value or the same length as the input"
            );
            let seed = seed.get(0).unwrap();
            ensure!(
                seed.len() == series.len(),
                ValueError: "Seed must be a single value or the same length as the input"
            );
            let seed = seed.cast(&DataType::UInt64)?;
            let seed = seed.u64().unwrap();
            series
                .hash_with(Some(seed), hash_function)
                .map(|arr| arr.into_series().rename(name))
        }
        1 => {
            let seed = seed.cast(&DataType::UInt64)?;
            let seed = seed.u64().unwrap();
            let seed = seed.get(0).unwrap();
            let seed = UInt64Array::from_iter(
                Field::new("seed", DataType::UInt64),
                std::iter::repeat_n(Some(seed), series.len()),
            );
            series
                .hash_with(Some(&seed), hash_function)
                .map(|arr| arr.into_series().rename(name))
        }
        _ if seed.len() == series.len() => {
            let seed = seed.cast(&DataType::UInt64)?;
            let seed = seed.u64().unwrap();
            series
                .hash_with(Some(seed), hash_function)
                .map(|arr| arr.into_series().rename(name))
        }
        _ => Err(DaftError::ValueError(
            "Seed must be a single value or the same length as the input".to_string(),
        )),
    }
}
