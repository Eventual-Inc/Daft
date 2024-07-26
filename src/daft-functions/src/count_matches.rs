use common_error::{DaftError, DaftResult};

use daft_core::{datatypes::Field, schema::Schema, DataType, Series};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct CountMatchesFunction {
    pub(super) whole_words: bool,
    pub(super) case_sensitive: bool,
}

#[typetag::serde]
impl ScalarUDF for CountMatchesFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "count_matches"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, _] => match data.to_field(schema) {
                Ok(field) => match &field.dtype {
                    DataType::Utf8 => Ok(Field::new(field.name, DataType::UInt64)),
                    a => Err(DaftError::TypeError(format!(
                        "Expects inputs to count_matches to be utf8, but received {a}",
                    ))),
                },
                Err(e) => Err(e),
            },
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        match inputs {
            [data, patterns] => {
                data.utf8_count_matches(patterns, self.whole_words, self.case_sensitive)
            }
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

pub fn utf8_count_matches(
    input: ExprRef,
    patterns: ExprRef,
    whole_words: bool,
    case_sensitive: bool,
) -> ExprRef {
    ScalarFunction::new(
        CountMatchesFunction {
            whole_words,
            case_sensitive,
        },
        vec![input, patterns],
    )
    .into()
}

#[cfg(feature = "python")]
pub mod python {
    use daft_dsl::python::PyExpr;
    use pyo3::{pyfunction, PyResult};

    #[pyfunction]
    pub fn utf8_count_matches(
        expr: PyExpr,
        patterns: PyExpr,
        whole_words: bool,
        case_sensitive: bool,
    ) -> PyResult<PyExpr> {
        let expr =
            super::utf8_count_matches(expr.into(), patterns.into(), whole_words, case_sensitive);
        Ok(expr.into())
    }
}
