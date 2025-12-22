use std::sync::{Arc, OnceLock};

use daft_ai::{provider::ProviderRef, python::PyProviderWrapper};
use daft_catalog::{
    Identifier,
    python::{PyIdentifier, PyTableSource, pyobj_to_catalog, pyobj_to_table},
};
use daft_dsl::functions::python::WrappedUDFClass;
use pyo3::prelude::*;

#[cfg(feature = "python")]
use crate::kv::PyKVStoreWrapper;
use crate::{
    Session,
    kv::{KVStore, KVStoreRef, MemoryKVStore},
};

static CURRENT_SESSION: OnceLock<Session> = OnceLock::new();

#[pyclass]
pub struct PySession(Session);

impl PySession {
    pub fn session(&self) -> &Session {
        &self.0
    }
}

#[pymethods]
impl PySession {
    #[staticmethod]
    pub fn empty() -> Self {
        Self(Session::empty())
    }

    pub fn attach_catalog(&self, catalog: Bound<PyAny>, alias: String) -> PyResult<()> {
        Ok(self.0.attach_catalog(pyobj_to_catalog(catalog)?, alias)?)
    }

    pub fn attach_provider(&self, provider: Bound<PyAny>, alias: String) -> PyResult<()> {
        Ok(self
            .0
            .attach_provider(pyobj_to_provider(provider)?, alias)?)
    }

    pub fn attach_table(&self, table: Bound<PyAny>, alias: String) -> PyResult<()> {
        Ok(self.0.attach_table(pyobj_to_table(table)?, alias)?)
    }

    pub fn detach_catalog(&self, alias: &str) -> PyResult<()> {
        Ok(self.0.detach_catalog(alias)?)
    }

    pub fn detach_provider(&self, alias: &str) -> PyResult<()> {
        Ok(self.0.detach_provider(alias)?)
    }

    pub fn detach_table(&self, alias: &str) -> PyResult<()> {
        Ok(self.0.detach_table(alias)?)
    }

    pub fn create_temp_table(
        &self,
        name: String,
        source: &PyTableSource,
        replace: bool,
        py: Python,
    ) -> PyResult<pyo3::Py<pyo3::PyAny>> {
        self.0
            .create_temp_table(name, source.as_ref(), replace)?
            .to_py(py)
    }

    pub fn current_catalog(&self, py: Python<'_>) -> PyResult<Option<pyo3::Py<pyo3::PyAny>>> {
        self.0.current_catalog()?.map(|c| c.to_py(py)).transpose()
    }

    pub fn current_namespace(&self) -> PyResult<Option<PyIdentifier>> {
        if let Some(namespace) = self.0.current_namespace()? {
            let ident = Identifier::try_new(namespace)?;
            let ident = PyIdentifier::from(ident);
            return Ok(Some(ident));
        }
        Ok(None)
    }

    pub fn current_provider(&self, py: Python<'_>) -> PyResult<Option<pyo3::Py<pyo3::PyAny>>> {
        self.0.current_provider()?.map(|p| p.to_py(py)).transpose()
    }

    pub fn current_model(&self) -> PyResult<Option<String>> {
        Ok(self.0.current_model()?)
    }

    pub fn get_catalog(&self, py: Python<'_>, name: &str) -> PyResult<pyo3::Py<pyo3::PyAny>> {
        self.0.get_catalog(name)?.to_py(py)
    }

    pub fn get_provider(&self, py: Python<'_>, name: &str) -> PyResult<pyo3::Py<pyo3::PyAny>> {
        self.0.get_provider(name)?.to_py(py)
    }

    pub fn get_table(
        &self,
        py: Python<'_>,
        ident: &PyIdentifier,
    ) -> PyResult<pyo3::Py<pyo3::PyAny>> {
        self.0.get_table(ident.as_ref())?.to_py(py)
    }

    pub fn has_catalog(&self, name: &str) -> PyResult<bool> {
        Ok(self.0.has_catalog(name))
    }

    pub fn has_provider(&self, name: &str) -> PyResult<bool> {
        Ok(self.0.has_provider(name))
    }

    pub fn has_table(&self, ident: &PyIdentifier) -> PyResult<bool> {
        Ok(self.0.has_table(ident.as_ref()))
    }

    #[pyo3(signature = (pattern=None))]
    pub fn list_catalogs(&self, pattern: Option<&str>) -> PyResult<Vec<String>> {
        Ok(self.0.list_catalogs(pattern)?)
    }

    #[pyo3(signature = (pattern=None))]
    pub fn list_tables(&self, pattern: Option<&str>) -> PyResult<Vec<String>> {
        Ok(self.0.list_tables(pattern)?)
    }

    #[pyo3(signature = (ident))]
    pub fn set_catalog(&self, ident: Option<&str>) -> PyResult<()> {
        Ok(self.0.set_catalog(ident)?)
    }

    #[pyo3(signature = (ident))]
    pub fn set_namespace(&self, ident: Option<&PyIdentifier>) -> PyResult<()> {
        Ok(self.0.set_namespace(ident.map(|i| i.as_ref()))?)
    }

    #[pyo3(signature = (ident))]
    pub fn set_provider(&self, ident: Option<&str>) -> PyResult<()> {
        Ok(self.0.set_provider(ident)?)
    }

    #[pyo3(signature = (ident))]
    pub fn set_model(&self, ident: Option<&str>) -> PyResult<()> {
        Ok(self.0.set_model(ident)?)
    }

    #[pyo3(signature = (function, alias = None))]
    pub fn attach_function(
        &self,
        function: pyo3::Py<pyo3::PyAny>,
        alias: Option<String>,
    ) -> PyResult<()> {
        let wrapped = WrappedUDFClass {
            inner: Arc::new(function),
        };

        self.0.attach_function(wrapped, alias)?;

        Ok(())
    }

    pub fn detach_function(&self, alias: &str) -> PyResult<()> {
        self.0.detach_function(alias)?;
        Ok(())
    }

    pub fn attach_kv(&self, kv_store: Bound<PyAny>, alias: String) -> PyResult<()> {
        let kv_ref = pyobj_to_kv_store(kv_store)?;
        // Attach to this session first
        self.0.attach_kv(kv_ref.clone(), alias.clone())?;

        // Best-effort: also mirror the attachment into the global CURRENT_SESSION if it
        // refers to a different underlying session. This ensures that Rust-side KV
        // helpers that rely on CURRENT_SESSION (e.g. kv_get_direct_series /
        // kv_put_direct_series) can see KV stores attached via Python Session helpers.
        if let Some(global) = CURRENT_SESSION.get()
            && !self.0.shares_state(global)
        {
            let _ = global.attach_kv(kv_ref, alias);
        }

        Ok(())
    }

    pub fn detach_kv(&self, alias: &str) -> PyResult<()> {
        Ok(self.0.detach_kv(alias)?)
    }

    pub fn get_kv(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        let kv_store = self.0.get_kv(name)?;
        // For PyKVStoreWrapper, we can get the original Python object
        if let Some(wrapper) = kv_store.as_any().downcast_ref::<PyKVStoreWrapper>() {
            Ok(wrapper.inner().clone_ref(py).into())
        } else {
            // Fallback: create a new Python wrapper (this shouldn't happen in normal usage)
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Unable to convert KV store to Python object",
            ))
        }
    }

    pub fn has_kv(&self, name: &str) -> PyResult<bool> {
        Ok(self.0.has_kv(name))
    }

    pub fn set_kv(&self, alias: Option<&str>) -> PyResult<()> {
        Ok(self.0.set_kv(alias)?)
    }

    pub fn current_kv(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        if let Some(kv_store) = self.0.current_kv()? {
            // For PyKVStoreWrapper, we can get the original Python object
            if let Some(wrapper) = kv_store.as_any().downcast_ref::<PyKVStoreWrapper>() {
                Ok(Some(wrapper.inner().clone_ref(py).into()))
            } else {
                // Fallback: create a new Python wrapper (this shouldn't happen in normal usage)
                Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "Unable to convert KV store to Python object",
                ))
            }
        } else {
            Ok(None)
        }
    }

    #[pyo3(signature = (pattern=None))]
    pub fn list_kv(&self, pattern: Option<String>) -> PyResult<Vec<String>> {
        Ok(self.0.list_kv(pattern.as_deref())?)
    }

    /// Attach a Rust memory KV store
    pub fn attach_memory_kv(&self, name: String, alias: String) -> PyResult<()> {
        let store = MemoryKVStore::new(name);
        Ok(self.0.attach_kv(Arc::new(store), alias)?)
    }
}

#[pyfunction]
pub fn set_current_session(sess: Bound<PyAny>) -> PyResult<()> {
    // Expect PySession
    let py_sess: PyRef<PySession> = sess.extract()?;
    let s = py_sess.session().clone_ref();
    let _ = CURRENT_SESSION.set(s);
    Ok(())
}

/// Direct KV get by store name and keys Series (Rust-side)
#[pyfunction(signature = (store_name, keys, on_error, columns=None))]
pub fn kv_get_direct_series(
    py: Python<'_>,
    store_name: &str,
    keys: daft_core::python::PySeries,
    on_error: &str,
    columns: Option<Vec<String>>,
) -> PyResult<daft_core::python::PySeries> {
    // Resolve current session
    let sess = CURRENT_SESSION.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No current session set")
    })?;
    let kv = sess.get_kv(store_name)?;

    // Determine fields: requested or store.schema_fields()
    let fields: Vec<String> = if let Some(cols) = columns {
        cols
    } else {
        kv.schema_fields()
    };

    // Prepare output literals
    use daft_core::prelude::*;
    let k_py = keys.series.cast(&DataType::Python)?;
    let k_arr = k_py.downcast::<PythonArray>()?;
    let mut out: Vec<Literal> = Vec::with_capacity(k_arr.len());

    for i in 0..k_arr.len() {
        let key_str = k_arr.str_value(i).unwrap_or_else(|_| String::new());
        let obj = if let Some(ms) = kv.as_any().downcast_ref::<crate::kv::MemoryKVStore>() {
            ms.get(py, &key_str)?
        } else if let Some(pywrap) = kv.as_any().downcast_ref::<crate::kv::PyKVStoreWrapper>() {
            let py_kv = pywrap.inner().bind(py);
            let obj = py_kv.call_method1("get_one", (key_str.clone(),))?;
            obj.unbind()
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Unsupported KV store for kv_get_direct_series",
            ));
        };

        let pydict = pyo3::types::PyDict::new(py);
        let obj_bound = obj.bind(py);
        if obj_bound.is_none() {
            if on_error == "null" {
                for f in &fields {
                    pydict.set_item(f, pyo3::types::PyNone::get(py))?;
                }
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                    "Missing key: {key_str}"
                )));
            }
        } else if obj_bound.is_instance_of::<pyo3::types::PyDict>() {
            let dict = obj_bound.cast::<pyo3::types::PyDict>()?;
            for f in &fields {
                if let Some(value) = dict.get_item(f)? {
                    pydict.set_item(f, value)?;
                } else {
                    pydict.set_item(f, pyo3::types::PyNone::get(py))?;
                }
            }
        } else if fields.len() == 1 {
            pydict.set_item(fields[0].as_str(), obj.clone_ref(py))?;
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Expected dict result for multi-column KV get",
            ));
        }
        let dict_obj: pyo3::Py<PyAny> = pydict.into_pyobject(py)?.into();
        out.push(Literal::Python(common_py_serde::PyObjectWrapper(
            std::sync::Arc::new(dict_obj),
        )));
    }

    let mut s = Series::from_literals(out)?;
    s = s.rename(keys.series.name());
    Ok(daft_core::python::PySeries { series: s })
}

/// Direct KV put by store name with key/value Series (Rust-side)
#[pyfunction]
pub fn kv_put_direct_series(
    _py: Python<'_>,
    store_name: &str,
    key: daft_core::python::PySeries,
    value: daft_core::python::PySeries,
) -> PyResult<daft_core::python::PySeries> {
    let sess = CURRENT_SESSION.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No current session set")
    })?;
    let kv = sess.get_kv(store_name)?;

    use daft_core::prelude::*;
    let k_py = daft_core::python::PySeries {
        series: key.series.clone(),
    };
    let binding = k_py.series.cast(&DataType::Python)?;
    let k_arr = binding.downcast::<PythonArray>()?;
    let len = k_arr.len();

    // Perform vectorized put where available
    if let Some(ms) = kv.as_any().downcast_ref::<crate::kv::MemoryKVStore>() {
        let _ = ms.put(&key.series, &value.series);
    } else if let Some(pywrap) = kv.as_any().downcast_ref::<crate::kv::PyKVStoreWrapper>() {
        let _ = pywrap.put(&key.series, &value.series);
    } else {
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Unsupported KV store for kv_put_direct_series",
        ));
    }

    // Build ack
    let mut acks: Vec<Literal> = Vec::with_capacity(len);
    for i in 0..len {
        let key_str = k_arr.str_value(i).unwrap_or_else(|_| String::new());
        let fields = vec![
            ("ok".to_string(), Literal::Boolean(true)),
            ("key".to_string(), Literal::Utf8(key_str)),
        ];
        acks.push(Literal::Struct(indexmap::IndexMap::from_iter(
            fields.into_iter(),
        )));
    }

    let mut s = Series::from_literals(acks)?;
    s = s.rename("result");
    Ok(daft_core::python::PySeries { series: s })
}

#[pyfunction(signature = (store_name, keys, batch_size, on_error, columns=None))]
pub fn kv_batch_get_direct_series(
    py: Python<'_>,
    store_name: &str,
    keys: daft_core::python::PySeries,
    batch_size: usize,
    on_error: &str,
    columns: Option<Vec<String>>,
) -> PyResult<daft_core::python::PySeries> {
    let sess = CURRENT_SESSION.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No current session set")
    })?;
    let kv = sess.get_kv(store_name)?;

    let fields: Vec<String> = if let Some(cols) = columns {
        cols
    } else {
        kv.schema_fields()
    };

    use daft_core::prelude::*;
    let k_py = keys.series.cast(&DataType::Python)?;
    let k_arr = k_py.downcast::<PythonArray>()?;
    let total_len = k_arr.len();
    let mut out: Vec<Literal> = Vec::with_capacity(total_len);

    // Process keys in chunks of batch_size
    for chunk_start in (0..total_len).step_by(batch_size) {
        let chunk_end = std::cmp::min(chunk_start + batch_size, total_len);
        let mut batch_keys: Vec<String> = Vec::with_capacity(chunk_end - chunk_start);

        for i in chunk_start..chunk_end {
            batch_keys.push(k_arr.str_value(i).unwrap_or_else(|_| String::new()));
        }

        // Call batch_get on the store
        let batch_results = if let Some(ms) = kv.as_any().downcast_ref::<crate::kv::MemoryKVStore>()
        {
            // MemoryKVStore optimization: use mget-like logic if available, otherwise loop
            let mut results = Vec::with_capacity(batch_keys.len());
            for k in &batch_keys {
                results.push(ms.get(py, k)?);
            }
            results
        } else if let Some(pywrap) = kv.as_any().downcast_ref::<crate::kv::PyKVStoreWrapper>() {
            let py_kv = pywrap.inner().bind(py);
            // Convert Rust Vec<String> to Python List[str]
            let py_keys_list = pyo3::types::PyList::new(py, &batch_keys)?;
            let results_obj = py_kv.call_method1("batch_get", (py_keys_list,))?;
            let results_list = results_obj.cast::<pyo3::types::PyList>()?;
            let mut results = Vec::with_capacity(results_list.len());
            for item in results_list {
                results.push(item.unbind());
            }
            results
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Unsupported KV store for kv_batch_get_direct_series",
            ));
        };

        if batch_results.len() != batch_keys.len() {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "KV store batch_get returned {} items, expected {}",
                batch_results.len(),
                batch_keys.len()
            )));
        }

        // Process results for this batch
        for (i, obj) in batch_results.into_iter().enumerate() {
            let key_str = &batch_keys[i];
            let pydict = pyo3::types::PyDict::new(py);
            let obj_bound = obj.bind(py);

            if obj_bound.is_none() {
                if on_error == "null" {
                    for f in &fields {
                        pydict.set_item(f, pyo3::types::PyNone::get(py))?;
                    }
                } else {
                    return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                        "Missing key: {key_str}"
                    )));
                }
            } else if obj_bound.is_instance_of::<pyo3::types::PyDict>() {
                let dict = obj_bound.cast::<pyo3::types::PyDict>()?;
                for f in &fields {
                    if let Some(value) = dict.get_item(f)? {
                        pydict.set_item(f, value)?;
                    } else {
                        pydict.set_item(f, pyo3::types::PyNone::get(py))?;
                    }
                }
            } else if fields.len() == 1 {
                pydict.set_item(fields[0].as_str(), obj.clone_ref(py))?;
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "Expected dict result for multi-column KV batch_get",
                ));
            }

            let dict_obj: pyo3::Py<PyAny> = pydict.into_pyobject(py)?.into();
            out.push(Literal::Python(common_py_serde::PyObjectWrapper(
                std::sync::Arc::new(dict_obj),
            )));
        }
    }

    let mut s = Series::from_literals(out)?;
    s = s.rename(keys.series.name());
    Ok(daft_core::python::PySeries { series: s })
}

#[pyfunction(signature = (store_name, keys))]
pub fn kv_exists_direct_series(
    py: Python<'_>,
    store_name: &str,
    keys: daft_core::python::PySeries,
) -> PyResult<daft_core::python::PySeries> {
    let sess = CURRENT_SESSION.get().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No current session set")
    })?;
    let kv = sess.get_kv(store_name)?;

    use daft_core::prelude::*;
    let k_py = keys.series.cast(&DataType::Python)?;
    let k_arr = k_py.downcast::<PythonArray>()?;
    let mut out: Vec<Literal> = Vec::with_capacity(k_arr.len());

    for i in 0..k_arr.len() {
        let key_str = k_arr.str_value(i).unwrap_or_else(|_| String::new());
        let obj = if let Some(ms) = kv.as_any().downcast_ref::<crate::kv::MemoryKVStore>() {
            ms.get(py, &key_str)?
        } else if let Some(pywrap) = kv.as_any().downcast_ref::<crate::kv::PyKVStoreWrapper>() {
            let py_kv = pywrap.inner().bind(py);
            py_kv.call_method1("get_one", (key_str.clone(),))?.unbind()
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Unsupported KV store for kv_exists_direct_series",
            ));
        };

        out.push(Literal::Boolean(!obj.is_none(py)));
    }

    let mut s = Series::from_literals(out)?;
    s = s.rename(keys.series.name());
    Ok(daft_core::python::PySeries { series: s })
}

fn pyobj_to_provider(obj: Bound<PyAny>) -> PyResult<ProviderRef> {
    // no current rust-based providers, so just wrap
    Ok(Arc::new(PyProviderWrapper::from(obj.unbind())))
}

fn pyobj_to_kv_store(obj: Bound<PyAny>) -> PyResult<KVStoreRef> {
    // Detect backend type from Python object
    #[cfg(feature = "python")]
    {
        let backend_type: String = obj.getattr("backend_type")?.extract()?;
        if backend_type.as_str() == "memory" {
            // Use Rust MemoryKVStore for session registration
            let name: String = obj.getattr("name")?.extract()?;
            let store = MemoryKVStore::new(name);
            return Ok(Arc::new(store));
        }
    }
    // Fallback: wrap the Python KV Store object
    let wrapper = PyKVStoreWrapper::new(obj.unbind())?;
    Ok(Arc::new(wrapper))
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<PySession>()?;
    parent.add_function(wrap_pyfunction!(set_current_session, parent)?)?;
    parent.add_function(wrap_pyfunction!(kv_get_direct_series, parent)?)?;
    parent.add_function(wrap_pyfunction!(kv_put_direct_series, parent)?)?;
    parent.add_function(wrap_pyfunction!(kv_batch_get_direct_series, parent)?)?;
    parent.add_function(wrap_pyfunction!(kv_exists_direct_series, parent)?)?;
    Ok(())
}
