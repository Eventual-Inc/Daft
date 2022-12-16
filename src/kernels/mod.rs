pub mod hashing;
pub mod search_sorted;

use pyo3::prelude::*;

pub fn register_kernels(py: Python, parent: &PyModule) -> PyResult<()> {
    let kernels_mod = PyModule::new(py, "kernels")?;
    kernels_mod.add_function(wrap_pyfunction!(hashing::hash_pyarrow_array, kernels_mod)?)?;
    kernels_mod.add_function(wrap_pyfunction!(
        search_sorted::search_sorted_pyarrow_array,
        kernels_mod
    )?)?;
    kernels_mod.add_function(wrap_pyfunction!(
        search_sorted::search_sorted_multiple_pyarrow_array,
        kernels_mod
    )?)?;
    parent.add_submodule(kernels_mod)?;
    Ok(())
}
