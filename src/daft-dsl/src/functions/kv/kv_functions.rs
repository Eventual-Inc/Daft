use common_error::{DaftError, DaftResult};
use daft_core::series::Series;
use daft_schema::schema::Schema;
#[cfg(feature = "python")]
use pyo3::Python;
#[cfg(feature = "python")]
use pyo3::types::PyAnyMethods;
use serde::{Deserialize, Serialize};

use crate::functions::{ExprRef, FunctionArgs, ScalarUDF};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct KVGetWithStoreName;

#[typetag::serde]
impl ScalarUDF for KVGetWithStoreName {
    fn name(&self) -> &'static str {
        "kv_get_with_name"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = args.into_inner();
        if inputs.len() != 2 && inputs.len() != 3 {
            return Err(DaftError::ValueError(
                "kv_get_with_name expects 2 or 3 arguments".to_string(),
            ));
        }
        let keys = &inputs[0];
        let store_name_series = &inputs[1];
        let columns_series_opt = if inputs.len() == 3 {
            Some(&inputs[2])
        } else {
            None
        };
        if store_name_series.len() != 1 {
            return Err(DaftError::ValueError(
                "Store name series must contain exactly one element".to_string(),
            ));
        }
        let name = store_name_series
            .utf8()?
            .get(0)
            .ok_or_else(|| DaftError::ValueError("Missing store name".to_string()))?;

        #[cfg(feature = "python")]
        {
            use daft_core::prelude::*;
            // Parse columns if provided
            let requested: Option<Vec<String>> = if let Some(cols_ser) = columns_series_opt {
                let arr = cols_ser.binary()?;
                let bytes = arr.get(0).ok_or_else(|| {
                    DaftError::ValueError("columns must contain exactly one element".to_string())
                })?;
                let cols: Vec<String> = serde_json::from_slice(bytes).map_err(|e| {
                    DaftError::ValueError(format!("Failed to deserialize columns: {}", e))
                })?;
                Some(cols)
            } else {
                None
            };

            let k_py = keys.cast(&DataType::Python)?;
            let _k_arr = k_py.downcast::<PythonArray>()?;
            let mut result_series_opt: Option<Series> = None;
            Python::attach(|py| {
                let daft_mod = py.import("daft.daft").unwrap();
                let py_keys = pyo3::Py::new(
                    py,
                    daft_core::python::PySeries {
                        series: keys.clone(),
                    },
                )
                .unwrap();
                let func = daft_mod.getattr("kv_get_direct_series").unwrap();
                let py_res = match requested.clone() {
                    Some(cols) => func.call1((name, py_keys, cols)).unwrap(),
                    None => func.call1((name, py_keys, None::<Vec<String>>)).unwrap(),
                };
                let py_series: pyo3::PyRef<daft_core::python::PySeries> = py_res.extract().unwrap();
                let mut s = py_series.series.clone();
                s = s.rename(keys.name());
                result_series_opt = Some(s);
            });
            let s = result_series_opt.ok_or_else(|| {
                DaftError::ValueError("kv_get_direct_series returned None".to_string())
            })?;
            Ok(s)
        }
        #[cfg(not(feature = "python"))]
        {
            let result_series = Series::full_null(
                "result",
                &daft_core::datatypes::DataType::Binary,
                keys.len(),
            );
            Ok(result_series)
        }
    }

    fn get_return_field(
        &self,
        args: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<daft_core::datatypes::Field> {
        let inputs = args.into_inner();
        if inputs.len() != 2 && inputs.len() != 3 {
            return Err(DaftError::ValueError(
                "kv_get_with_name expects 2 or 3 arguments".to_string(),
            ));
        }
        let keys = inputs[0].to_field(schema)?;
        #[cfg(feature = "python")]
        {
            Ok(daft_core::datatypes::Field::new(
                keys.name,
                daft_core::datatypes::DataType::Python,
            ))
        }
        #[cfg(not(feature = "python"))]
        {
            Ok(daft_core::datatypes::Field::new(
                keys.name,
                daft_core::datatypes::DataType::Binary,
            ))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct KVBatchGetWithStoreName;

#[typetag::serde]
impl ScalarUDF for KVBatchGetWithStoreName {
    fn name(&self) -> &'static str {
        "kv_batch_get_with_name"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = args.into_inner();
        if inputs.len() != 3 {
            return Err(DaftError::ValueError(
                "kv_batch_get_with_name expects exactly 3 arguments".to_string(),
            ));
        }
        let keys = &inputs[0];
        let store_name_series = &inputs[1];
        let _batch_size_series = &inputs[2];
        let input_length = keys.len();
        if store_name_series.len() != 1 {
            return Err(DaftError::ValueError(
                "Store name series must contain exactly one element".to_string(),
            ));
        }
        let result_series = Series::full_null(
            "result",
            &daft_core::datatypes::DataType::Binary,
            input_length,
        );
        Ok(result_series)
    }

    fn get_return_field(
        &self,
        args: FunctionArgs<ExprRef>,
        _schema: &Schema,
    ) -> DaftResult<daft_core::datatypes::Field> {
        let inputs = args.into_inner();
        if inputs.len() != 3 {
            return Err(DaftError::ValueError(
                "kv_batch_get_with_name expects exactly 3 arguments".to_string(),
            ));
        }
        Ok(daft_core::datatypes::Field::new(
            "result",
            daft_core::datatypes::DataType::Binary,
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct KVExistsWithStoreName;

#[typetag::serde]
impl ScalarUDF for KVExistsWithStoreName {
    fn name(&self) -> &'static str {
        "kv_exists_with_name"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = args.into_inner();
        if inputs.len() != 2 {
            return Err(DaftError::ValueError(
                "kv_exists_with_name expects exactly 2 arguments".to_string(),
            ));
        }
        let keys = &inputs[0];
        let input_length = keys.len();
        let result_series = Series::full_null(
            "result",
            &daft_core::datatypes::DataType::Binary,
            input_length,
        );
        Ok(result_series)
    }

    fn get_return_field(
        &self,
        args: FunctionArgs<ExprRef>,
        _schema: &Schema,
    ) -> DaftResult<daft_core::datatypes::Field> {
        let inputs = args.into_inner();
        if inputs.len() != 2 {
            return Err(DaftError::ValueError(
                "kv_exists_with_name expects exactly 2 arguments".to_string(),
            ));
        }
        Ok(daft_core::datatypes::Field::new(
            "result",
            daft_core::datatypes::DataType::Binary,
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct KVPutWithStoreName;

#[typetag::serde]
impl ScalarUDF for KVPutWithStoreName {
    fn name(&self) -> &'static str {
        "kv_put_with_name"
    }

    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        let inputs = args.into_inner();
        if inputs.len() != 3 {
            return Err(DaftError::ValueError(
                "kv_put_with_name expects exactly 3 arguments".to_string(),
            ));
        }
        let key = &inputs[0];
        let value = &inputs[1];
        let store_name_series = &inputs[2];
        if store_name_series.len() != 1 {
            return Err(DaftError::ValueError(
                "Store name series must contain exactly one element".to_string(),
            ));
        }
        let name = store_name_series
            .utf8()?
            .get(0)
            .ok_or_else(|| DaftError::ValueError("Missing store name".to_string()))?;
        #[cfg(feature = "python")]
        {
            use daft_core::prelude::*;
            // Build PySeries for key and value
            let mut result_series_opt: Option<Series> = None;
            Python::attach(|py| {
                let daft_mod = py.import("daft.daft").unwrap();
                let py_key = pyo3::Py::new(
                    py,
                    daft_core::python::PySeries {
                        series: key.clone(),
                    },
                )
                .unwrap();
                let py_value = pyo3::Py::new(
                    py,
                    daft_core::python::PySeries {
                        series: value.clone(),
                    },
                )
                .unwrap();
                let func = daft_mod.getattr("kv_put_direct_series").unwrap();
                let py_res = func.call1((name, py_key, py_value)).unwrap();
                let py_series: pyo3::PyRef<daft_core::python::PySeries> = py_res.extract().unwrap();
                let mut s = py_series.series.clone();
                s = s.rename("result");
                result_series_opt = Some(s);
            });
            let s = result_series_opt.ok_or_else(|| {
                DaftError::ValueError("kv_put_direct_series returned None".to_string())
            })?;
            Ok(s)
        }
        #[cfg(not(feature = "python"))]
        {
            let result_series =
                Series::full_null("result", &daft_core::datatypes::DataType::Binary, key.len());
            Ok(result_series)
        }
    }

    fn get_return_field(
        &self,
        _args: FunctionArgs<ExprRef>,
        _schema: &Schema,
    ) -> DaftResult<daft_core::datatypes::Field> {
        let fields = vec![
            daft_core::datatypes::Field::new("ok", daft_core::datatypes::DataType::Boolean),
            daft_core::datatypes::Field::new("key", daft_core::datatypes::DataType::Utf8),
        ];
        Ok(daft_core::datatypes::Field::new(
            "result",
            daft_core::datatypes::DataType::Struct(fields),
        ))
    }
}

pub fn kv_get_with_name(keys: ExprRef, name: ExprRef, columns: Option<ExprRef>) -> ExprRef {
    use crate::functions::scalar::ScalarFn;
    let mut args = vec![keys, name];
    if let Some(c) = columns {
        args.push(c);
    }
    ScalarFn::builtin(KVGetWithStoreName, args).into()
}

pub fn kv_batch_get_with_name(keys: ExprRef, name: ExprRef, batch_size: ExprRef) -> ExprRef {
    use crate::functions::scalar::ScalarFn;
    ScalarFn::builtin(KVBatchGetWithStoreName, vec![keys, name, batch_size]).into()
}

pub fn kv_exists_with_name(keys: ExprRef, name: ExprRef) -> ExprRef {
    use crate::functions::scalar::ScalarFn;
    ScalarFn::builtin(KVExistsWithStoreName, vec![keys, name]).into()
}

pub fn kv_put_with_name(key: ExprRef, value: ExprRef, name: ExprRef) -> ExprRef {
    use crate::functions::scalar::ScalarFn;
    ScalarFn::builtin(KVPutWithStoreName, vec![key, value, name]).into()
}
