use common_error::{DaftError, DaftResult};
use daft_core::prelude::{DataType, Field, Schema, Series};
#[cfg(feature = "python")]
use daft_dsl::python::PyExpr;
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
#[cfg(feature = "python")]
use pyo3::{pyfunction, PyResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct ListValueCountsFunction;

#[typetag::serde]
impl ScalarUDF for ListValueCountsFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "list_value_counts"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        let [data] = inputs else {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        };

        let data_field = data.to_field(schema)?;

        let DataType::List(inner_type) = &data_field.dtype else {
            return Err(DaftError::TypeError(format!(
                "Expected list, got {}",
                data_field.dtype
            )));
        };

        let map_type = DataType::Map {
            key: inner_type.clone(),
            value: Box::new(DataType::UInt64),
        };

        Ok(Field::new(data_field.name, map_type))
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        let [data] = inputs else {
            return Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            )));
        };

        data.list_value_counts()
    }
}

pub fn list_value_counts(expr: ExprRef) -> ExprRef {
    ScalarFunction::new(ListValueCountsFunction, vec![expr]).into()
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "list_value_counts")]
pub fn py_list_value_counts(expr: PyExpr) -> PyResult<PyExpr> {
    Ok(list_value_counts(expr.into()).into())
}
