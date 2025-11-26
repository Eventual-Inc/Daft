use common_error::DaftError;
use daft_core::{
    lit::{FromLiteral, Literal},
    prelude::TimeUnit,
};
use daft_dsl::functions::prelude::*;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UnixTimestamp;

/// simple wrapper to provide custom `fromliteral` impl
struct WrappedTimeUnit(TimeUnit);

impl FromLiteral for WrappedTimeUnit {
    fn try_from_literal(lit: &daft_core::lit::Literal) -> DaftResult<Self> {
        if let Ok(tu) = TimeUnit::try_from_literal(lit) {
            Ok(Self(tu))
        } else if let Literal::Utf8(s) = lit {
            Ok(Self(s.parse()?))
        } else {
            Err(DaftError::type_error(
                "Expected string literal for time unit",
            ))
        }
    }
}
#[derive(FunctionArgs)]
struct Args<T> {
    input: T,
    #[arg(optional)]
    time_unit: Option<WrappedTimeUnit>,
}

#[typetag::serde]
impl ScalarUDF for UnixTimestamp {
    fn name(&self) -> &'static str {
        "to_unix_epoch"
    }

    fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let Args { input, time_unit } = inputs.try_into()?;
        let tu = time_unit.map(|tu| tu.0).unwrap_or(TimeUnit::Seconds);
        input
            .cast(&DataType::Timestamp(tu, None))
            .and_then(|s| s.cast(&DataType::Int64))
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Args { input, .. } = inputs.try_into()?;
        let field = input.to_field(schema)?;

        ensure!(
            matches!(field.dtype, DataType::Timestamp(..) | DataType::Date),
            TypeError: "Expected input to be date or timestamp, got {}",
            field.dtype
        );

        Ok(Field::new(field.name, DataType::Int64))
    }
}
