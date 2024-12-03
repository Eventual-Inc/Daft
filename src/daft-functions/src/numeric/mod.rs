pub mod abs;
pub mod cbrt;
pub mod ceil;
pub mod clip;
pub mod exp;
pub mod floor;
pub mod log;
pub mod round;
pub mod sign;
pub mod sqrt;
pub mod trigonometry;

use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{functions::ScalarUDF, ExprRef};

fn to_field_single_numeric(
    f: &dyn ScalarUDF,
    inputs: &[ExprRef],
    schema: &Schema,
) -> DaftResult<Field> {
    if inputs.len() != 1 {
        return Err(DaftError::SchemaMismatch(format!(
            "Expected 1 input arg, got {}",
            inputs.len()
        )));
    }
    let field = inputs.first().unwrap().to_field(schema)?;
    if !field.dtype.is_numeric() {
        return Err(DaftError::TypeError(format!(
            "Expected input to {} to be numeric, got {}",
            f.name(),
            field.dtype
        )));
    }
    Ok(field)
}

fn to_field_single_floating(
    f: &dyn ScalarUDF,
    inputs: &[ExprRef],
    schema: &Schema,
) -> DaftResult<Field> {
    match inputs {
        [first] => {
            let field = first.to_field(schema)?;
            let dtype = field.dtype.to_floating_representation()?;
            Ok(Field::new(field.name, dtype))
        }
        _ => Err(DaftError::SchemaMismatch(format!(
            "Expected 1 input arg for {}, got {}",
            f.name(),
            inputs.len()
        ))),
    }
}

fn evaluate_single_numeric<F: Fn(&Series) -> DaftResult<Series>>(
    inputs: &[Series],
    func: F,
) -> DaftResult<Series> {
    if inputs.len() != 1 {
        return Err(DaftError::ValueError(format!(
            "Expected 1 input arg, got {}",
            inputs.len()
        )));
    }
    func(inputs.first().unwrap())
}
