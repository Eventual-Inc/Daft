use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::{DaftAtan2, trigonometry::TrigonometricFunction},
    prelude::{DataType, Field, Schema},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, UnaryArg, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};

// super annoying, but using an enum with typetag::serde doesn't work with bincode because it uses Deserializer::deserialize_identifier
macro_rules! trigonometry {
    ($name:ident, $variant:ident, $docstring:literal) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
        pub struct $variant;

        #[typetag::serde]
        impl ScalarUDF for $variant {
            fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
                let UnaryArg { input } = inputs.try_into()?;

                trigonometry(input, &TrigonometricFunction::$variant)
            }

            fn name(&self) -> &'static str {
                TrigonometricFunction::$variant.fn_name()
            }

            fn get_return_field(
                &self,
                inputs: FunctionArgs<ExprRef>,
                schema: &Schema,
            ) -> DaftResult<Field> {
                let UnaryArg { input } = inputs.try_into()?;
                let field = input.to_field(schema)?;

                let dtype = match field.dtype {
                    DataType::Float32 => DataType::Float32,
                    dt if dt.is_numeric() => DataType::Float64,
                    _ => {
                        return Err(DaftError::TypeError(format!(
                            "Expected input to trigonometry to be numeric, got {}",
                            field.dtype
                        )));
                    }
                };
                Ok(Field::new(field.name, dtype))
            }

            fn docstring(&self) -> &'static str {
                $docstring
            }
        }

        #[must_use]
        pub fn $name(input: ExprRef) -> ExprRef {
            ScalarFn::builtin($variant, vec![input]).into()
        }
    };
}

trigonometry!(sin, Sin, "Calculates the sine of an angle in radians.");
trigonometry!(cos, Cos, "Calculates the cosine of an angle in radians.");
trigonometry!(tan, Tan, "Calculates the tangent of an angle in radians.");
trigonometry!(csc, Csc, "Calculates the cosecant of an angle in radians.");
trigonometry!(sec, Sec, "Calculates the secant of an angle in radians.");
trigonometry!(cot, Cot, "Calculates the cotangent of an angle in radians.");
trigonometry!(
    sinh,
    Sinh,
    "Calculates the hyperbolic sine of an angle in radians."
);
trigonometry!(
    cosh,
    Cosh,
    "Calculates the hyperbolic cosine of an angle in radians."
);
trigonometry!(
    tanh,
    Tanh,
    "Calculates the hyperbolic tangent of an angle in radians."
);
trigonometry!(
    arcsin,
    ArcSin,
    "Calculates the inverse sine (arc sine) of a number."
);
trigonometry!(
    arccos,
    ArcCos,
    "Calculates the inverse cosine (arc cosine) of a number."
);
trigonometry!(
    arctan,
    ArcTan,
    "Calculates the inverse tangent (arc tangent) of a number."
);
trigonometry!(
    radians,
    Radians,
    "Converts an angle from degrees to radians."
);
trigonometry!(
    degrees,
    Degrees,
    "Converts an angle from radians to degrees."
);
trigonometry!(
    arctanh,
    ArcTanh,
    "Calculates the inverse hyperbolic tangent of a number."
);
trigonometry!(
    arccosh,
    ArcCosh,
    "Calculates the inverse hyperbolic cosine of a number."
);
trigonometry!(
    arcsinh,
    ArcSinh,
    "Calculates the inverse hyperbolic sine of a number."
);

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Atan2;

#[derive(FunctionArgs)]
struct Atan2Args<T> {
    x: T,
    y: T,
}

#[typetag::serde]
impl ScalarUDF for Atan2 {
    fn call(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let Atan2Args { x, y } = inputs.try_into()?;

        atan2_impl(x, y)
    }

    fn name(&self) -> &'static str {
        "atan2"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["arctan2"]
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let Atan2Args { x, y } = inputs.try_into()?;

        let x_field = x.to_field(schema)?;
        let y_field = y.to_field(schema)?;
        let dtype = match (x_field.dtype, y_field.dtype) {
            (DataType::Float32, DataType::Float32) => DataType::Float32,
            (dt1, dt2) if dt1.is_numeric() && dt2.is_numeric() => DataType::Float64,
            (dt1, dt2) => {
                return Err(DaftError::TypeError(format!(
                    "Expected inputs to atan2 to be numeric, got {dt1} and {dt2}"
                )));
            }
        };
        Ok(Field::new(x_field.name, dtype))
    }

    fn docstring(&self) -> &'static str {
        "Calculates the angle between the positive x-axis and the ray from (0,0) to (x,y)."
    }
}

#[must_use]
pub fn atan2(x: ExprRef, y: ExprRef) -> ExprRef {
    ScalarFn::builtin(Atan2 {}, vec![x, y]).into()
}

fn trigonometry(s: Series, trig_function: &TrigonometricFunction) -> DaftResult<Series> {
    match s.data_type() {
        DataType::Float32 => {
            let ca = s.f32().unwrap();
            Ok(ca.trigonometry(trig_function)?.into_series())
        }
        DataType::Float64 => {
            let ca = s.f64().unwrap();
            Ok(ca.trigonometry(trig_function)?.into_series())
        }
        dt if dt.is_numeric() => {
            let s = s.cast(&DataType::Float64)?;
            let ca = s.f64().unwrap();
            Ok(ca.trigonometry(trig_function)?.into_series())
        }
        dt => Err(DaftError::TypeError(format!(
            "Expected input to trigonometry to be numeric, got {}",
            dt
        ))),
    }
}
fn atan2_impl(s: Series, rhs: Series) -> DaftResult<Series> {
    match (s.data_type(), rhs.data_type()) {
        (DataType::Float32, DataType::Float32) => {
            let lhs_ca = s.f32().unwrap();
            let rhs_ca = rhs.f32().unwrap();
            Ok(lhs_ca.atan2(rhs_ca)?.into_series())
        }
        (DataType::Float64, DataType::Float64) => {
            let lhs_ca = s.f64().unwrap();
            let rhs_ca = rhs.f64().unwrap();
            Ok(lhs_ca.atan2(rhs_ca)?.into_series())
        }
        // avoid extra casting if one side is already f64
        (DataType::Float64, rhs_dt) if rhs_dt.is_numeric() => {
            let rhs_s = rhs.cast(&DataType::Float64)?;
            atan2_impl(s, rhs_s)
        }
        (lhs_dt, DataType::Float64) if lhs_dt.is_numeric() => {
            let lhs_s = s.cast(&DataType::Float64)?;
            atan2_impl(lhs_s, rhs)
        }
        (lhs_dt, rhs_dt) if lhs_dt.is_numeric() && rhs_dt.is_numeric() => {
            let lhs_s = s.cast(&DataType::Float64)?;
            let rhs_s = rhs.cast(&DataType::Float64)?;
            atan2_impl(lhs_s, rhs_s)
        }
        (lhs_dt, rhs_dt) => Err(DaftError::TypeError(format!(
            "Expected inputs to trigonometry to be numeric, got {} and {}",
            lhs_dt, rhs_dt
        ))),
    }
}
