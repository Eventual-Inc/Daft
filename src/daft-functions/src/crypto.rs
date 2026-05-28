use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::functions::{prelude::*, scalar::ScalarFn};
use serde::{Deserialize, Serialize};

// ============================================================================
// MD5 Function
// ============================================================================

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct Md5Function;

#[derive(FunctionArgs)]
struct Md5Args<T> {
    input: T,
}

#[typetag::serde]
impl ScalarUDF for Md5Function {
    fn name(&self) -> &'static str {
        "md5"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let Md5Args { input } = inputs.try_into()?;
        let input = input_to_bytes(&input)?;
        let name = input.name().to_string();
        Ok(Utf8Array::from_iter(
            &name,
            (0..input.len()).map(|i| input.get(i).map(daft_hash::compute_md5)),
        )
        .into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Md5Args { input } = inputs.try_into()?;
        let input = input.to_field(schema)?;
        validate_input_type(&input.dtype, "md5")?;
        Ok(Field::new(input.name, DataType::Utf8))
    }
}

// ============================================================================
// SHA1 Function
// ============================================================================

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct Sha1Function;

#[derive(FunctionArgs)]
struct Sha1Args<T> {
    input: T,
}

#[typetag::serde]
impl ScalarUDF for Sha1Function {
    fn name(&self) -> &'static str {
        "sha1"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let Sha1Args { input } = inputs.try_into()?;
        let input = input_to_bytes(&input)?;
        let name = input.name().to_string();
        Ok(Utf8Array::from_iter(
            &name,
            (0..input.len()).map(|i| input.get(i).map(daft_hash::compute_sha1)),
        )
        .into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Sha1Args { input } = inputs.try_into()?;
        let input = input.to_field(schema)?;
        validate_input_type(&input.dtype, "sha1")?;
        Ok(Field::new(input.name, DataType::Utf8))
    }
}

// ============================================================================
// SHA2 Function
// ============================================================================

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct Sha2Function;

#[derive(FunctionArgs)]
struct Sha2Args<T> {
    input: T,
    bit_length: u32,
}

#[typetag::serde]
impl ScalarUDF for Sha2Function {
    fn name(&self) -> &'static str {
        "sha2"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let Sha2Args { input, bit_length } = inputs.try_into()?;
        // Validate bit_length
        if !matches!(bit_length, 0 | 224 | 256 | 384 | 512) {
            return Err(DaftError::ValueError(format!(
                "sha2() bit_length must be 0, 224, 256, 384, or 512, got {}",
                bit_length
            )));
        }
        let input = input_to_bytes(&input)?;
        let name = input.name().to_string();
        Ok(Utf8Array::from_iter(
            &name,
            (0..input.len()).map(|i| {
                input
                    .get(i)
                    .map(|bytes| daft_hash::compute_sha2(bytes, bit_length))
            }),
        )
        .into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Sha2Args { input, bit_length } = inputs.try_into()?;
        let input = input.to_field(schema)?;
        validate_input_type(&input.dtype, "sha2")?;
        // Validate bit_length at schema-analysis time to surface errors early
        if !matches!(bit_length, 0 | 224 | 256 | 384 | 512) {
            return Err(DaftError::ValueError(format!(
                "sha2() bit_length must be 0, 224, 256, 384, or 512, got {}",
                bit_length
            )));
        }
        Ok(Field::new(input.name, DataType::Utf8))
    }
}

// ============================================================================
// XxHash64 Function
// ============================================================================

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct XxHash64Function;

#[derive(FunctionArgs)]
struct XxHash64Args<T> {
    #[arg(variadic)]
    input: Vec<T>,

    #[arg(optional)]
    seed: Option<i64>,
}

#[typetag::serde]
impl ScalarUDF for XxHash64Function {
    fn name(&self) -> &'static str {
        "xxhash64"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let XxHash64Args { input, seed } = inputs.try_into()?;

        if input.is_empty() {
            return Err(DaftError::ValueError(
                "xxhash64() requires at least one expression".to_string(),
            ));
        }

        let seed = seed.unwrap_or(42);
        let first_name = input[0].name().to_string();
        let num_rows = input[0].len();

        // Convert all inputs to bytes and combine hashes
        let mut hashes: Vec<i64> = vec![seed; num_rows];

        for series in &input {
            let bytes_series = input_to_bytes(series)?;
            for (i, hash) in hashes.iter_mut().enumerate().take(num_rows) {
                // In Spark, null columns do not update the running hash — the seed is
                // forwarded unchanged. Only hash when data is non-null.
                if let Some(data) = bytes_series.get(i) {
                    *hash = daft_hash::compute_xxhash64_seeded(data, *hash);
                }
            }
        }

        Ok(Int64Array::from_iter(
            Field::new(first_name, DataType::Int64),
            hashes.into_iter().map(Some),
        )
        .into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        _schema: &Schema,
    ) -> DaftResult<Field> {
        let XxHash64Args { input, .. } = inputs.try_into()?;
        let first = input.first().ok_or_else(|| {
            DaftError::ValueError("xxhash64() requires at least one expression".to_string())
        })?;
        let input = first.to_field(_schema)?;
        Ok(Field::new(input.name, DataType::Int64))
    }
}

// ============================================================================
// CRC32 Function
// ============================================================================

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) struct Crc32Function;

#[derive(FunctionArgs)]
struct Crc32Args<T> {
    input: T,
}

#[typetag::serde]
impl ScalarUDF for Crc32Function {
    fn name(&self) -> &'static str {
        "crc32"
    }

    fn call(
        &self,
        inputs: daft_dsl::functions::FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let Crc32Args { input } = inputs.try_into()?;
        let input = input_to_bytes(&input)?;
        let name = input.name().to_string();
        Ok(Int64Array::from_iter(
            Field::new(name, DataType::Int64),
            (0..input.len()).map(|i| input.get(i).map(daft_hash::compute_crc32)),
        )
        .into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Crc32Args { input } = inputs.try_into()?;
        let input = input.to_field(schema)?;
        validate_input_type(&input.dtype, "crc32")?;
        Ok(Field::new(input.name, DataType::Int64))
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Validates whether the input type supports hash operations
fn validate_input_type(dtype: &DataType, func_name: &str) -> DaftResult<()> {
    match dtype {
        DataType::Utf8 | DataType::Binary | DataType::Null => Ok(()),
        _ => Err(DaftError::TypeError(format!(
            "{}() expects String or Binary input, got {:?}",
            func_name, dtype
        ))),
    }
}

/// Helper struct providing a unified byte-array access interface for Series
struct BytesAccessor {
    data: BytesData,
    len: usize,
    name: String,
}

enum BytesData {
    Utf8(Vec<Option<Vec<u8>>>),
    Binary(Vec<Option<Vec<u8>>>),
    Null,
}

impl BytesAccessor {
    fn len(&self) -> usize {
        self.len
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, idx: usize) -> Option<&[u8]> {
        match &self.data {
            BytesData::Utf8(data) => data[idx].as_deref(),
            BytesData::Binary(data) => data[idx].as_deref(),
            BytesData::Null => None,
        }
    }
}

/// Converts a Series into a BytesAccessor
fn input_to_bytes(series: &Series) -> DaftResult<BytesAccessor> {
    let name = series.name().to_string();
    let len = series.len();
    match series.data_type() {
        DataType::Utf8 => {
            let utf8 = series.utf8()?;
            let data: Vec<Option<Vec<u8>>> = (0..len)
                .map(|i| utf8.get(i).map(|s| s.as_bytes().to_vec()))
                .collect();
            Ok(BytesAccessor {
                data: BytesData::Utf8(data),
                len,
                name,
            })
        }
        DataType::Binary => {
            let binary = series.binary()?;
            let data: Vec<Option<Vec<u8>>> = (0..len)
                .map(|i| binary.get(i).map(|b| b.to_vec()))
                .collect();
            Ok(BytesAccessor {
                data: BytesData::Binary(data),
                len,
                name,
            })
        }
        DataType::Null => Ok(BytesAccessor {
            data: BytesData::Null,
            len,
            name,
        }),
        _dt => {
            // For other types, cast to Utf8 first then process
            let utf8_series = series.cast(&DataType::Utf8)?;
            let utf8 = utf8_series.utf8()?;
            let data: Vec<Option<Vec<u8>>> = (0..len)
                .map(|i| utf8.get(i).map(|s| s.as_bytes().to_vec()))
                .collect();
            Ok(BytesAccessor {
                data: BytesData::Utf8(data),
                len,
                name,
            })
        }
    }
}

// ============================================================================
// Public API Functions
// ============================================================================

#[must_use]
pub fn md5(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Md5Function, vec![input]).into()
}

#[must_use]
pub fn sha1(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Sha1Function, vec![input]).into()
}

#[must_use]
pub fn sha2(input: ExprRef, bit_length: ExprRef) -> ExprRef {
    ScalarFn::builtin(Sha2Function, vec![input, bit_length]).into()
}

#[must_use]
pub fn xxhash64(inputs: Vec<ExprRef>, seed: Option<ExprRef>) -> ExprRef {
    let mut all_inputs = inputs;
    if let Some(seed) = seed {
        all_inputs.push(seed);
    }
    ScalarFn::builtin(XxHash64Function, all_inputs).into()
}

#[must_use]
pub fn crc32(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Crc32Function, vec![input]).into()
}
