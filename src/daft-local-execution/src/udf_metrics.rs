use std::{
    collections::{HashMap, hash_map::Entry},
    sync::{LazyLock, Mutex},
};

#[cfg(feature = "python")]
use pyo3::{Py, exceptions::PyValueError, prelude::*, types::PyDict};

#[derive(Default)]
pub(crate) struct MetricsStore {
    pub counters: HashMap<String, u64>,
    pub gauges: HashMap<String, f64>,
}

impl MetricsStore {
    fn increment_counter(&mut self, name: &str, amount: u64) {
        if let Some(count) = self.counters.get_mut(name) {
            *count += amount;
        } else {
            self.counters.insert(name.to_string(), amount);
        }
    }

    fn set_gauge(&mut self, name: &str, value: f64) {
        self.gauges.insert(name.to_string(), value);
    }
}

pub static METRIC_STORES: LazyLock<Mutex<HashMap<String, MetricsStore>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(feature = "python")]
fn with_store_mut<F, R>(udf_id: &str, f: F) -> PyResult<R>
where
    F: FnOnce(&mut MetricsStore) -> R,
{
    if udf_id.is_empty() {
        return Err(PyValueError::new_err("UDF id must be a non-empty string"));
    }

    let mut stores = METRIC_STORES
        .lock()
        .expect("Failed to lock UDF metrics store");
    let store = stores.entry(udf_id.to_string()).or_default();
    Ok(f(store))
}

#[cfg(feature = "python")]
#[pyfunction(signature = (udf_id, name, amount=None))]
fn increment_counter(udf_id: &str, name: &str, amount: Option<u64>) -> PyResult<()> {
    if name.is_empty() {
        return Err(PyValueError::new_err(
            "Metric name must be a non-empty string",
        ));
    }
    let increment = amount.unwrap_or(1);
    with_store_mut(udf_id, |store| store.increment_counter(name, increment))?;
    Ok(())
}

#[cfg(feature = "python")]
#[pyfunction]
fn set_gauge(udf_id: &str, name: &str, value: f64) -> PyResult<()> {
    if name.is_empty() {
        return Err(PyValueError::new_err(
            "Metric name must be a non-empty string",
        ));
    }
    with_store_mut(udf_id, |store| store.set_gauge(name, value))?;
    Ok(())
}

pub fn drain_metrics(udf_id: &str) -> Option<(HashMap<String, u64>, HashMap<String, f64>)> {
    let mut stores = METRIC_STORES
        .lock()
        .expect("Failed to lock UDF metrics store");
    match stores.get_mut(udf_id) {
        Some(store) => {
            let counters = std::mem::take(&mut store.counters);
            let gauges = std::mem::take(&mut store.gauges);
            Some((counters, gauges))
        }
        None => None,
    }
}

#[cfg(feature = "python")]
#[pyfunction]
fn _reset(udf_id: Option<&str>) -> PyResult<()> {
    let mut stores = METRIC_STORES
        .lock()
        .expect("Failed to lock UDF metrics store");

    if let Some(id) = udf_id {
        stores.remove(id);
    } else {
        stores.clear();
    }
    Ok(())
}

#[cfg(feature = "python")]
#[pyfunction]
fn _drain_metrics(py: Python<'_>, udf_id: &str) -> PyResult<Py<PyDict>> {
    let mut stores = METRIC_STORES
        .lock()
        .expect("Failed to lock UDF metrics store");
    let (counters_vec, gauges_vec) = match stores.entry(udf_id.to_string()) {
        Entry::Occupied(mut entry) => {
            let store = entry.get_mut();
            let counters = store.counters.drain().collect::<Vec<_>>();
            let gauges = store.gauges.drain().collect::<Vec<_>>();
            if store.counters.is_empty() && store.gauges.is_empty() {
                entry.remove();
            }
            (counters, gauges)
        }
        Entry::Vacant(_) => (Vec::new(), Vec::new()),
    };

    let counters_dict = PyDict::new(py);
    for (name, value) in counters_vec {
        counters_dict.set_item(name, value)?;
    }

    let gauges_dict = PyDict::new(py);
    for (name, value) in gauges_vec {
        gauges_dict.set_item(name, value)?;
    }

    let result = PyDict::new(py);
    result.set_item("counters", counters_dict)?;
    result.set_item("gauges", gauges_dict)?;
    Ok(result.into())
}

#[cfg(feature = "python")]
pub fn register(parent: &Bound<PyModule>) -> PyResult<()> {
    let module = PyModule::new(parent.py(), "_udf_metrics")?;
    module.add_function(wrap_pyfunction!(increment_counter, &module)?)?;
    module.add_function(wrap_pyfunction!(set_gauge, &module)?)?;
    module.add_function(wrap_pyfunction!(_reset, &module)?)?;
    module.add_function(wrap_pyfunction!(_drain_metrics, &module)?)?;
    parent.add_submodule(&module)?;
    parent.setattr(pyo3::intern!(parent.py(), "_udf_metrics"), &module)?;
    let sys_modules = parent
        .py()
        .import(pyo3::intern!(parent.py(), "sys"))?
        .getattr(pyo3::intern!(parent.py(), "modules"))?;
    sys_modules.set_item("daft._udf_metrics", &module)?;
    Ok(())
}
