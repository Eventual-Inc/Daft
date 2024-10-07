use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::trigonometry::TrigonometricFunction,
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use super::evaluate_single_numeric;

// super annoying, but using an enum with typetag::serde doesn't work with bincode because it uses Deserializer::deserialize_identifier
macro_rules! trigonometry {
    ($name:ident, $variant:ident) => {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
        pub struct $variant;

        #[typetag::serde]
        impl ScalarUDF for $variant {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn name(&self) -> &'static str {
                TrigonometricFunction::$variant.fn_name()
            }

            fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
                if inputs.len() != 1 {
                    return Err(DaftError::SchemaMismatch(format!(
                        "Expected 1 input arg, got {}",
                        inputs.len()
                    )));
                };
                let field = inputs.first().unwrap().to_field(schema)?;
                let dtype = match field.dtype {
                    DataType::Float32 => DataType::Float32,
                    dt if dt.is_numeric() => DataType::Float64,
                    _ => {
                        return Err(DaftError::TypeError(format!(
                            "Expected input to trigonometry to be numeric, got {}",
                            field.dtype
                        )))
                    }
                };
                Ok(Field::new(field.name, dtype))
            }

            fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
                evaluate_single_numeric(inputs, |s| {
                    s.trigonometry(&TrigonometricFunction::$variant)
                })
            }
        }

        #[must_use]
        pub fn $name(input: ExprRef) -> ExprRef {
            ScalarFunction::new($variant, vec![input]).into()
        }
    };
}

trigonometry!(sin, Sin);
trigonometry!(cos, Cos);
trigonometry!(tan, Tan);
trigonometry!(cot, Cot);
trigonometry!(arcsin, ArcSin);
trigonometry!(arccos, ArcCos);
trigonometry!(arctan, ArcTan);
trigonometry!(radians, Radians);
trigonometry!(degrees, Degrees);
trigonometry!(arctanh, ArcTanh);
trigonometry!(arccosh, ArcCosh);
trigonometry!(arcsinh, ArcSinh);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Atan2 {}

#[typetag::serde]
impl ScalarUDF for Atan2 {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "atan2"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        if inputs.len() != 2 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            )));
        }
        let field1 = inputs.first().unwrap().to_field(schema)?;
        let field2 = inputs.get(1).unwrap().to_field(schema)?;
        let dtype = match (field1.dtype, field2.dtype) {
            (DataType::Float32, DataType::Float32) => DataType::Float32,
            (dt1, dt2) if dt1.is_numeric() && dt2.is_numeric() => DataType::Float64,
            (dt1, dt2) => {
                return Err(DaftError::TypeError(format!(
                    "Expected inputs to atan2 to be numeric, got {dt1} and {dt2}"
                )))
            }
        };
        Ok(Field::new(field1.name, dtype))
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [x, y] => x.atan2(y),
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

#[must_use]
pub fn atan2(x: ExprRef, y: ExprRef) -> ExprRef {
    ScalarFunction::new(Atan2 {}, vec![x, y]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "sin")]
pub fn py_sin(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(sin(expr.into()).into())
}
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "cos")]
pub fn py_cos(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(cos(expr.into()).into())
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "tan")]
pub fn py_tan(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(tan(expr.into()).into())
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "cot")]
pub fn py_cot(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(cot(expr.into()).into())
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "arcsin")]
pub fn py_arcsin(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(arcsin(expr.into()).into())
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "arccos")]
pub fn py_arccos(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(arccos(expr.into()).into())
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "arctan")]
pub fn py_arctan(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(arctan(expr.into()).into())
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "radians")]
pub fn py_radians(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(radians(expr.into()).into())
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "degrees")]
pub fn py_degrees(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(degrees(expr.into()).into())
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "arctanh")]
pub fn py_arctanh(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(arctanh(expr.into()).into())
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "arccosh")]
pub fn py_arccosh(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(arccosh(expr.into()).into())
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "arcsinh")]
pub fn py_arcsinh(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(arcsinh(expr.into()).into())
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "arctan2")]
pub fn py_arctan2(x: PyExpr, y: PyExpr) -> PyResult<PyExpr> {
    Ok(atan2(x.into(), y.into()).into())
}
