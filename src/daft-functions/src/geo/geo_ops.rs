use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

use crate::geo::utils;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GeoOp {
    pub op: GeoOperation,
}

#[typetag::serde]
impl ScalarUDF for GeoOp {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "geo_op"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;
                match field.dtype {
                    DataType::Geometry => match self.op {
                        GeoOperation::Area => Ok(Field::new(field.name, DataType::Float64)),
                        GeoOperation::ConvexHull | GeoOperation::Centroid => {
                            Ok(Field::new(field.name, DataType::Geometry))
                        }
                        _ => Err(DaftError::ValueError(format!(
                            "unsupported op {:?}",
                            self.op
                        ))),
                    },
                    _ => Err(DaftError::TypeError(format!(
                        "GeoOps can only operate on Geometry arrays, got {}",
                        field
                    ))),
                }
            }
            [lhs, rhs] => {
                let lhs_field = lhs.to_field(schema)?;
                let rhs_field = rhs.to_field(schema)?;
                match (lhs_field.dtype, rhs_field.dtype) {
                    (DataType::Geometry, DataType::Geometry) => match self.op {
                        GeoOperation::Distance => Ok(Field::new(lhs_field.name, DataType::Float64)),
                        GeoOperation::Intersects => {
                            Ok(Field::new(lhs_field.name, DataType::Boolean))
                        }
                        GeoOperation::Intersection => {
                            Ok(Field::new(lhs_field.name, DataType::Geometry))
                        }
                        _ => Err(DaftError::ValueError(format!(
                            "unsupported op {:?}",
                            self.op
                        ))),
                    },
                    (lhs, rhs) => Err(DaftError::TypeError(format!(
                        "GeoOps can only operate on Geometry arrays, got {} and {}",
                        lhs, rhs
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [input] => match input.data_type() {
                DataType::Geometry => match self.op {
                    GeoOperation::Area => utils::geo_unary_dispatch(input, GeoOperation::Area),
                    GeoOperation::ConvexHull => {
                        utils::geo_unary_dispatch(input, GeoOperation::ConvexHull)
                    }
                    GeoOperation::Centroid => {
                        utils::geo_unary_dispatch(input, GeoOperation::Centroid)
                    }
                    _ => Err(DaftError::ValueError(format!(
                        "unsupported op {:?}",
                        self.op
                    ))),
                },
                other => Err(DaftError::TypeError(format!(
                    "GeoOps can operate on Geometry arrays, got {}",
                    other
                ))),
            },
            [lhs, rhs] => match (lhs.data_type(), rhs.data_type()) {
                (DataType::Geometry, DataType::Geometry) => match self.op {
                    GeoOperation::Distance => {
                        utils::geo_binary_dispatch(lhs, rhs, GeoOperation::Distance)
                    }
                    GeoOperation::Intersects => {
                        utils::geo_binary_dispatch(lhs, rhs, GeoOperation::Intersects)
                    }
                    GeoOperation::Intersection => {
                        utils::geo_binary_dispatch(lhs, rhs, GeoOperation::Intersection)
                    }
                    _ => Err(DaftError::ValueError(format!(
                        "unsupported op {:?}",
                        self.op
                    ))),
                },
                (lhs, rhs) => Err(DaftError::TypeError(format!(
                    "GeoOps can only operate on Geometry arrays, got {} and {}",
                    lhs, rhs
                ))),
            },
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

pub fn geo_op(input: ExprRef, args: Option<GeoOp>) -> ExprRef {
    ScalarFunction::new(args.unwrap(), vec![input]).into()
}

pub fn geo_op_binary(lhs: ExprRef, rhs: ExprRef, args: Option<GeoOp>) -> ExprRef {
    ScalarFunction::new(args.unwrap(), vec![lhs, rhs]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

use super::utils::GeoOperation;

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "geo_op")]
pub fn py_geo_op(expr: PyExpr, op: GeoOperation) -> PyResult<PyExpr> {
    let op = GeoOp { op };

    Ok(geo_op(expr.into(), Some(op)).into())
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "geo_op_binary")]
pub fn py_geo_op_binary(lhs: PyExpr, rhs: PyExpr, op: GeoOperation) -> PyResult<PyExpr> {
    let op = GeoOp { op };

    Ok(geo_op_binary(lhs.into(), rhs.into(), Some(op)).into())
}
