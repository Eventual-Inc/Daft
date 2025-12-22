use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

use common_error::{DaftError, DaftResult};
use daft_core::{lit::Literal, prelude::*};
#[cfg(feature = "python")]
use pyo3::{IntoPyObject, types::PyDictMethods};
use serde::{Deserialize, Serialize};

#[cfg(feature = "python")]
type MemoryStore = HashMap<String, std::sync::Arc<pyo3::Py<pyo3::PyAny>>>;
#[cfg(feature = "python")]
type MemoryRegistry = HashMap<String, MemoryStore>;

#[cfg(not(feature = "python"))]
type MemoryStore = HashMap<String, ()>; // Placeholder type for non-Python builds
#[cfg(not(feature = "python"))]
type MemoryRegistry = HashMap<String, MemoryStore>;

use crate::{
    ExprRef,
    functions::{
        function_args::{FunctionArgs, FunctionArgs as FunctionArgsDerive},
        scalar::ScalarUDF,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreDesc {
    pub kind: String,
    pub name: String,
    #[serde(default)]
    pub alias: Option<String>,
}

static MEM_REGISTRY: OnceLock<Mutex<MemoryRegistry>> = OnceLock::new();

#[cfg(feature = "python")]
fn registry() -> &'static Mutex<MemoryRegistry> {
    MEM_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(feature = "python")]
fn parse_store_descs(series: &Series) -> DaftResult<Vec<StoreDesc>> {
    let arr = series.binary()?;
    let bytes = arr.get(0).ok_or_else(|| {
        DaftError::ValueError("store_descs must contain exactly one element".to_string())
    })?;
    let descs: Vec<StoreDesc> = serde_json::from_slice(bytes).map_err(|e| {
        DaftError::ValueError(format!("Failed to deserialize StoreDesc list: {}", e))
    })?;
    Ok(descs)
}

#[cfg(feature = "python")]
fn parse_columns_opt(series_opt: Option<&Series>) -> DaftResult<Option<Vec<String>>> {
    if let Some(series) = series_opt {
        let arr = series.binary()?;
        let bytes = arr.get(0).ok_or_else(|| {
            DaftError::ValueError("columns must contain exactly one element".to_string())
        })?;
        let cols: Vec<String> = serde_json::from_slice(bytes)
            .map_err(|e| DaftError::ValueError(format!("Failed to deserialize columns: {}", e)))?;
        Ok(Some(cols))
    } else {
        Ok(None)
    }
}

#[cfg(feature = "python")]
fn derive_fields_for_store(name: &str) -> Vec<String> {
    let n = name.to_lowercase();
    if n.contains("embedding") {
        vec!["embedding".to_string()]
    } else if n.contains("meta") {
        vec!["metadata".to_string()]
    } else {
        vec!["value".to_string()]
    }
}

#[derive(FunctionArgsDerive)]
struct MemKVPutArgs<T> {
    key: T,
    value: T,
    store_desc: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemKVPut;

#[typetag::serde]
impl ScalarUDF for MemKVPut {
    fn name(&self) -> &'static str {
        "mem_kv_put"
    }

    #[cfg(feature = "python")]
    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        use pyo3::Python;

        let MemKVPutArgs {
            key,
            value,
            store_desc,
        } = args.try_into()?;
        let descs = parse_store_descs(&store_desc)?;
        if descs.len() != 1 {
            return Err(DaftError::ValueError(
                "mem_kv_put expects a single store descriptor".to_string(),
            ));
        }
        let desc = &descs[0];
        if desc.kind.as_str() != "memory" {
            return Err(DaftError::ValueError(format!(
                "mem_kv_put only supports memory backend, got {}",
                desc.kind
            )));
        }

        let k_py = key.cast(&DataType::Python)?;
        let k_arr = k_py.downcast::<PythonArray>()?;
        let v_dtype = value.data_type().clone();
        let len = k_arr.len();

        let mut ack: Vec<Literal> = Vec::with_capacity(k_arr.len());
        let mut guard = registry().lock().unwrap();
        let store = guard.entry(desc.name.clone()).or_default();

        Python::attach(|py| {
            for i in 0..len {
                let key_str = k_arr.str_value(i).unwrap_or_else(|_| String::new());
                let obj_opt: Option<std::sync::Arc<pyo3::Py<pyo3::PyAny>>> = match &v_dtype {
                    DataType::Python => value
                        .downcast::<PythonArray>()
                        .ok()
                        .and_then(|arr| arr.iter().nth(i).flatten())
                        .cloned(),
                    DataType::Utf8 => match value.utf8().ok().and_then(|a| a.get(i)) {
                        Some(s) => {
                            let pystr = pyo3::types::PyString::new(py, s);
                            Some(std::sync::Arc::new(pystr.into_pyobject(py).unwrap().into()))
                        }
                        None => None,
                    },
                    DataType::List(inner) => {
                        if inner.as_ref() == &DataType::Float64 {
                            match value
                                .list()
                                .ok()
                                .and_then(|larr| larr.iter().nth(i).flatten())
                            {
                                Some(sub) => match sub.f64().ok() {
                                    Some(farr) => {
                                        let mut vec: Vec<f64> = Vec::with_capacity(farr.len());
                                        for j in 0..farr.len() {
                                            if let Some(v) = farr.get(j) {
                                                vec.push(v);
                                            }
                                        }
                                        let pylist = pyo3::types::PyList::new(py, &vec).unwrap();
                                        Some(std::sync::Arc::new(
                                            pylist.into_pyobject(py).unwrap().into(),
                                        ))
                                    }
                                    None => None,
                                },
                                None => None,
                            }
                        } else {
                            None
                        }
                    }
                    _ => {
                        // Fallback: try cast to Python
                        {
                            if let Ok(s) = value.cast(&DataType::Python) {
                                if let Ok(arr) = s.downcast::<PythonArray>() {
                                    arr.iter().nth(i).flatten().cloned()
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }
                    }
                };

                if let Some(obj) = obj_opt {
                    store.insert(key_str.clone(), obj);
                    let fields = vec![
                        ("ok".to_string(), Literal::Boolean(true)),
                        ("key".to_string(), Literal::Utf8(key_str)),
                    ];
                    let lit = Literal::Struct(indexmap::IndexMap::from_iter(fields.into_iter()));
                    ack.push(lit);
                } else {
                    let fields = vec![
                        ("ok".to_string(), Literal::Boolean(false)),
                        ("key".to_string(), Literal::Utf8(key_str)),
                    ];
                    let lit = Literal::Struct(indexmap::IndexMap::from_iter(fields.into_iter()));
                    ack.push(lit);
                }
            }
        });

        let mut s = Series::from_literals(ack)?;
        s = s.rename("result");
        Ok(s)
    }

    #[cfg(not(feature = "python"))]
    fn call(&self, _args: FunctionArgs<Series>) -> DaftResult<Series> {
        Err(DaftError::ComputeError(
            "MemKVPut is only supported in Python mode".to_string(),
        ))
    }

    #[cfg(feature = "python")]
    fn get_return_field(&self, args: FunctionArgs<ExprRef>, _schema: &Schema) -> DaftResult<Field> {
        let MemKVPutArgs {
            key: _key,
            value: _value,
            store_desc: _store_desc,
        } = args.try_into()?;
        let fields = vec![
            Field::new("ok", DataType::Boolean),
            Field::new("key", DataType::Utf8),
        ];
        Ok(Field::new("result", DataType::Struct(fields)))
    }

    #[cfg(not(feature = "python"))]
    fn get_return_field(&self, args: FunctionArgs<ExprRef>, _schema: &Schema) -> DaftResult<Field> {
        let MemKVPutArgs {
            key: _key,
            value: _value,
            store_desc: _store_desc,
        } = args.try_into()?;
        let fields = vec![
            Field::new("ok", DataType::Boolean),
            Field::new("key", DataType::Utf8),
        ];
        Ok(Field::new("result", DataType::Struct(fields)))
    }
}

#[derive(FunctionArgsDerive)]
struct MemKVGetArgs<T> {
    keys: T,
    store_descs: T,
    #[arg(optional)]
    columns: Option<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemKVGet;

#[typetag::serde]
impl ScalarUDF for MemKVGet {
    fn name(&self) -> &'static str {
        "mem_kv_get"
    }

    #[cfg(feature = "python")]
    fn call(&self, args: FunctionArgs<Series>) -> DaftResult<Series> {
        use pyo3::Python;

        let MemKVGetArgs {
            keys,
            store_descs,
            columns,
        } = args.try_into()?;
        let descs = parse_store_descs(&store_descs)?;
        let requested = parse_columns_opt(columns.as_ref())?;

        let mut store_field_map: Vec<(String, Vec<String>)> = Vec::new();
        for d in &descs {
            if d.kind.as_str() != "memory" {
                continue;
            }
            let all_fields = derive_fields_for_store(&d.name);
            let sel_fields = if let Some(cols) = &requested {
                all_fields
                    .into_iter()
                    .filter(|f| cols.contains(f))
                    .collect::<Vec<_>>()
            } else {
                all_fields
            };
            if !sel_fields.is_empty() {
                store_field_map.push((d.name.clone(), sel_fields));
            }
        }

        let k_py = keys.cast(&DataType::Python)?;
        let k_arr = k_py.downcast::<PythonArray>()?;
        let mut out: Vec<Literal> = Vec::with_capacity(k_arr.len());

        let guard = registry().lock().unwrap();
        Python::attach(|py| {
            for i in 0..k_arr.len() {
                let key_str = k_arr.str_value(i).unwrap_or_else(|_| String::new());
                let pydict = pyo3::types::PyDict::new(py);
                for (store_name, sel_fields) in &store_field_map {
                    if let Some(store) = guard.get(store_name)
                        && let Some(obj) = store.get(&key_str)
                    {
                        for f in sel_fields {
                            let _ = pydict.set_item(f.clone(), obj.clone().bind(py));
                        }
                    }
                }
                let dict_obj: pyo3::Py<pyo3::PyAny> = pydict.into_pyobject(py).unwrap().into();
                out.push(Literal::Python(common_py_serde::PyObjectWrapper(
                    std::sync::Arc::new(dict_obj),
                )));
            }
        });

        let mut s = Series::from_literals(out)?;
        s = s.rename(keys.name());
        Ok(s)
    }

    #[cfg(not(feature = "python"))]
    fn call(&self, _args: FunctionArgs<Series>) -> DaftResult<Series> {
        Err(DaftError::ComputeError(
            "MemKVGet is only supported in Python mode".to_string(),
        ))
    }

    #[cfg(feature = "python")]
    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let MemKVGetArgs {
            keys,
            store_descs: _,
            columns: _,
        } = args.try_into()?;
        let input_field = keys.to_field(schema)?;
        // Return struct with Python fields; dynamic at runtime.
        Ok(Field::new(input_field.name, DataType::Python))
    }

    #[cfg(not(feature = "python"))]
    fn get_return_field(&self, args: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let MemKVGetArgs {
            keys,
            store_descs: _,
            columns: _,
        } = args.try_into()?;
        let input_field = keys.to_field(schema)?;
        // Return null type when not in Python mode
        Ok(Field::new(input_field.name, DataType::Null))
    }
}

use crate::functions::scalar::ScalarFn;

pub fn mem_kv_put(key: ExprRef, value: ExprRef, store_desc: ExprRef) -> ExprRef {
    ScalarFn::builtin(MemKVPut, vec![key, value, store_desc]).into()
}

pub fn mem_kv_get(keys: ExprRef, store_descs: ExprRef, columns: Option<ExprRef>) -> ExprRef {
    let mut args = vec![keys, store_descs];
    if let Some(c) = columns {
        args.push(c);
    }
    ScalarFn::builtin(MemKVGet, args).into()
}
