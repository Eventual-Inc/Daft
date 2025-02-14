/// Defines the python daft.
pub fn register_modules<'py>(parent: &Bound<'py, PyModule>) -> PyResult<Bound<'py, PyModule>> {
    let module = PyModule::new(parent.py(), "catalog")?;
    module.add_class::<PyIdentifier>()?;
    module.add_wrapped(wrap_pyfunction!(py_read_table))?;
    module.add_wrapped(wrap_pyfunction!(py_register_table))?;
    module.add_wrapped(wrap_pyfunction!(py_unregister_catalog))?;
    parent.add_submodule(&module)?;
    Ok(module)
}
