use std::sync::Arc;

use common_error::DaftResult;
use daft_context::get_context;
use daft_core::prelude::SchemaRef;
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::{partitioning::{MicroPartitionSet, Partition, PartitionCacheEntry, PartitionSet}, MicroPartition};

/// Create an in-memory scan from arrow arrays.
pub fn from_arrow<S: Into<SchemaRef>>(
    schema: S,
    arrays: Vec<Box<dyn arrow2::array::Array>>,
) -> DaftResult<LogicalPlanBuilder> {
    //
    // create a micropartition
    let schema = schema.into();
    let part = MicroPartition::from_arrow(schema.clone(), arrays)?;
    //
    // add to the runner's partition cache
    let pset = MicroPartitionSet::empty();
    pset.set_partition(0, &Arc::new(part))?;
    let pset_ref = Arc::new(pset);
    let cache_entry = put_partition_set_into_cache(pset_ref.clone())?;
    let cache_entry_key = cache_entry.key();
    let size_bytes = pset_ref.size_bytes()?;
    let num_rows = pset_ref.len();
    let num_partitions = pset_ref.num_partitions();
    //
    // create a scan from the entry
    let builder = LogicalPlanBuilder::in_memory_scan(
        &cache_entry_key,
                        cache_entry, schema, num_partitions, size_bytes, num_rows)?;
    Ok(builder)
}


#[cfg(feature = "python")]
fn put_partition_set_into_cache(pset: Arc<MicroPartitionSet>) -> DaftResult<PartitionCacheEntry> {
    use daft_micropartition::python::PyMicroPartitionSet;
    use pyo3::{types::PyAnyMethods, Python};

    Python::with_gil(|py| {
        // get runner as python object so we can add a partition to the cache
        let py_runner = get_context().get_or_create_runner()?.to_pyobj(py);
        let py_runner = py_runner.bind(py);
        // create a py partition set from our rust partition set.
        let py_pset = PyMicroPartitionSet::from(pset);
        // put in the runner's partition set cache
        let py_cache_entry = py_runner.call_method1("put_partition_set_into_cache", (py_pset,))?;

        todo!()
    })

}

#[cfg(not(feature = "python"))]
fn put_partition_set_into_cache(pset: Arc<MicroPartitionSet>) -> DaftResult<PartitionCacheEntry> {
    Err(DaftError::InternalError(
        "from_arrow requires 'python' feature".to_string(),
    ))
}
