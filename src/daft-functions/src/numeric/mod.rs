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

use abs::Abs;
use cbrt::Cbrt;
use ceil::Ceil;
use clip::Clip;
use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{FunctionModule, FunctionRegistry, ScalarUDF},
    ExprRef,
};
use exp::{Exp, Expm1};
use floor::Floor;
use log::{Ln, Log, Log10, Log1p, Log2};
use round::Round;
use sign::{Negative, Sign};
use sqrt::Sqrt;

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

pub struct NumericFunctions;
impl FunctionModule for NumericFunctions {
    fn register(parent: &mut FunctionRegistry) {
        parent.add_fn(Abs);
        parent.add_fn(Cbrt);
        parent.add_fn(Ceil);
        parent.add_fn(Clip);
        parent.add_fn(Exp);
        parent.add_fn(Expm1);
        parent.add_fn(Log);
        parent.add_fn(Log2);
        parent.add_fn(Log10);
        parent.add_fn(Ln);
        parent.add_fn(Log1p);
        parent.add_fn(Floor);
        parent.add_fn(Round);
        parent.add_fn(Sign);
        parent.add_fn(Negative);
        parent.add_fn(Sqrt);

        // trig functions
        use trigonometry::*;
        parent.add_fn(Sin);
        parent.add_fn(Cos);
        parent.add_fn(Tan);
        parent.add_fn(Csc);
        parent.add_fn(Sec);
        parent.add_fn(Cot);
        parent.add_fn(Sinh);
        parent.add_fn(Cosh);
        parent.add_fn(Tanh);
        parent.add_fn(ArcSin);
        parent.add_fn(ArcCos);
        parent.add_fn(ArcTan);
        parent.add_fn(Radians);
        parent.add_fn(Degrees);
        parent.add_fn(ArcTanh);
        parent.add_fn(ArcCosh);
        parent.add_fn(ArcSinh);
        parent.add_fn(Atan2);
    }
}
