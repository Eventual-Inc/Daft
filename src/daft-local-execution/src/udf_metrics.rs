#[cfg(feature = "python")]
use std::{collections::HashMap, sync::Mutex};

#[cfg(feature = "python")]
use once_cell::sync::Lazy;
#[cfg(feature = "python")]
use pyo3::{Py, exceptions::PyValueError, prelude::*, types::PyDict};

#[cfg(feature = "python")]
#[derive(Default)]
pub struct MetricsSnapshot {
    pub counters: HashMap<String, u64>,
    pub gauges: HashMap<String, f64>,
}

#[cfg(feature = "python")]
#[derive(Default)]
struct MetricsStore {
    counters: HashMap<String, u64>,
    gauges: HashMap<String, f64>,
    last_counters: HashMap<String, u64>,
}

#[cfg(feature = "python")]
impl MetricsStore {
    fn increment_counter(&mut self, name: &str, amount: u64) {
        *self.counters.entry(name.to_string()).or_default() += amount;
    }

    fn set_gauge(&mut self, name: &str, value: f64) {
        self.gauges.insert(name.to_string(), value);
    }

    fn snapshot_delta(&mut self) -> MetricsSnapshot {
        let mut delta_counters = HashMap::new();
        for (name, total) in &self.counters {
            let previous = self.last_counters.get(name).copied().unwrap_or(0);
            let delta = total.saturating_sub(previous);
            if delta > 0 {
                delta_counters.insert(name.clone(), delta);
            }
        }
        // Remove entries that no longer exist in totals from last_counters
        self.last_counters
            .retain(|name, _| self.counters.contains_key(name));
        // Update last_counters to current totals
        for (name, total) in &self.counters {
            self.last_counters.insert(name.clone(), *total);
        }

        MetricsSnapshot {
            counters: delta_counters,
            gauges: self.gauges.clone(),
        }
    }
}

#[cfg(feature = "python")]
static METRIC_STORES: Lazy<Mutex<HashMap<String, MetricsStore>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

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
    let store = stores
        .entry(udf_id.to_string())
        .or_insert_with(MetricsStore::default);
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

#[cfg(feature = "python")]
pub fn take_snapshot_for_udf(udf_id: &str) -> MetricsSnapshot {
    let mut stores = METRIC_STORES
        .lock()
        .expect("Failed to lock UDF metrics store");
    let store = stores
        .entry(udf_id.to_string())
        .or_insert_with(MetricsStore::default);
    store.snapshot_delta()
}

#[cfg(feature = "python")]
pub fn take_snapshot_native() -> HashMap<String, MetricsSnapshot> {
    let mut stores = METRIC_STORES
        .lock()
        .expect("Failed to lock UDF metrics store");
    stores
        .iter_mut()
        .map(|(id, store)| (id.clone(), store.snapshot_delta()))
        .collect()
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
fn _snapshot(py: Python<'_>, udf_id: Option<&str>) -> PyResult<Py<PyDict>> {
    let snapshots = if let Some(id) = udf_id {
        let snapshot = take_snapshot_for_udf(id);
        let mut map = HashMap::new();
        map.insert(id.to_string(), snapshot);
        map
    } else {
        take_snapshot_native()
    };

    let mut combined_counters: HashMap<String, u64> = HashMap::new();
    let mut combined_gauges: HashMap<String, f64> = HashMap::new();

    for snapshot in snapshots.into_values() {
        for (name, value) in snapshot.counters {
            *combined_counters.entry(name).or_default() += value;
        }
        for (name, value) in snapshot.gauges {
            combined_gauges.insert(name, value);
        }
    }

    let counters_dict = PyDict::new(py);
    for (name, value) in combined_counters {
        counters_dict.set_item(name, value)?;
    }

    let gauges_dict = PyDict::new(py);
    for (name, value) in combined_gauges {
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
    module.add_function(wrap_pyfunction!(_snapshot, &module)?)?;
    parent.add_submodule(&module)?;
    parent.setattr(pyo3::intern!(parent.py(), "_udf_metrics"), &module)?;
    let sys_modules = parent
        .py()
        .import(pyo3::intern!(parent.py(), "sys"))?
        .getattr(pyo3::intern!(parent.py(), "modules"))?;
    sys_modules.set_item("daft._udf_metrics", &module)?;
    Ok(())
}
