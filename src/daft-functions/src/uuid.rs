use std::{fmt::Display, str::FromStr, sync::Arc};

use arrow_array::builder::FixedSizeBinaryBuilder;
use common_error::{DaftError, DaftResult};
use daft_core::{
    lit::{FromLiteral, Literal},
    prelude::{DataType, Field, FromArrow, Schema, UuidArray},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{
        FunctionArgs, ScalarUDF,
        scalar::{EvalContext, ScalarFn},
    },
};
use serde::{Deserialize, Serialize};
use uuid::Uuid as RustUuid;

const UUID_LEN: i32 = 16;

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
enum UuidVersion {
    V4,
    V7,
}

impl UuidVersion {
    fn generate(self) -> RustUuid {
        match self {
            Self::V4 => RustUuid::new_v4(),
            Self::V7 => RustUuid::now_v7(),
        }
    }
}

impl Default for UuidVersion {
    fn default() -> Self {
        Self::V4
    }
}

impl std::fmt::Debug for UuidVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::V4 => "v4",
            Self::V7 => "v7",
        })
    }
}

impl Display for UuidVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<UuidVersion> for Literal {
    fn from(value: UuidVersion) -> Self {
        Self::Utf8(value.to_string())
    }
}

impl FromLiteral for UuidVersion {
    fn try_from_literal(lit: &Literal) -> DaftResult<Self> {
        if let Literal::Utf8(s) = lit {
            s.parse()
        } else {
            Err(DaftError::ValueError(format!(
                "Expected a string literal, got {:?}",
                lit
            )))
        }
    }
}

impl FromStr for UuidVersion {
    type Err = DaftError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "4" | "v4" => Ok(Self::V4),
            "7" | "v7" => Ok(Self::V7),
            _ => Err(DaftError::ValueError(format!(
                "`version` must be 'v4' or 'v7', got {s:?}"
            ))),
        }
    }
}

#[derive(FunctionArgs)]
struct UuidArgs {
    #[arg(optional)]
    version: Option<UuidVersion>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Uuid;

#[typetag::serde]
impl ScalarUDF for Uuid {
    fn name(&self) -> &'static str {
        "uuid"
    }

    fn call(&self, inputs: FunctionArgs<Series>, ctx: &EvalContext) -> DaftResult<Series> {
        let UuidArgs { version } = inputs.try_into()?;
        let array = uuid_kernel(ctx.row_count, version.unwrap_or_default())?;
        uuid_series_from_builder(array)
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        _schema: &Schema,
    ) -> DaftResult<Field> {
        let UuidArgs { .. } = inputs.try_into()?;
        Ok(Field::new("", DataType::Uuid))
    }

    fn docstring(&self) -> &'static str {
        "Generates a column of UUID values."
    }
}

#[must_use]
pub fn uuid() -> ExprRef {
    ScalarFn::builtin(Uuid, vec![]).into()
}

fn uuid_series_from_builder(mut builder: FixedSizeBinaryBuilder) -> DaftResult<Series> {
    Ok(
        UuidArray::from_arrow(Field::new("", DataType::Uuid), Arc::new(builder.finish()))?
            .into_series(),
    )
}

fn uuid_kernel(len: usize, version: UuidVersion) -> DaftResult<FixedSizeBinaryBuilder> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(len, UUID_LEN);

    for _ in 0..len {
        builder.append_value(version.generate())?;
    }

    Ok(builder)
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;

    use super::*;

    #[test]
    fn uuid_kernel_sets_version_and_variant_bits() {
        let array = uuid_kernel(128, UuidVersion::V7).unwrap().finish();

        assert_eq!(array.len(), 128);
        for idx in 0..array.len() {
            let bytes = array.value(idx);
            assert_eq!(bytes[6] >> 4, 0x7);
            assert_eq!(bytes[8] >> 6, 0b10);
        }
    }

    #[test]
    fn uuid_v7_kernel_outputs_lexicographically_ordered_values() {
        let array = uuid_kernel(128, UuidVersion::V7).unwrap().finish();

        for idx in 1..array.len() {
            assert!(array.value(idx - 1) < array.value(idx));
        }
    }

    #[test]
    fn uuid_kernel_generates_requested_version() {
        let v4 = uuid_kernel(1, UuidVersion::V4).unwrap().finish();
        let v7 = uuid_kernel(1, UuidVersion::V7).unwrap().finish();

        assert_eq!(v4.value(0)[6] >> 4, 0x4);
        assert_eq!(v7.value(0)[6] >> 4, 0x7);
    }
}
