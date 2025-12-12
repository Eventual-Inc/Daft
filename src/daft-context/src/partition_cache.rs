/// This module holds utilities for creating in-memory scans from SQL.
///
/// For now, it lives in daft-sql where it is used, but eventually
/// it may live in daft-micropartition or daft-logical-plan. The
/// current dependency tree makes it difficult to know which is right.
///
use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::{
    MicroPartition,
    partitioning::{MicroPartitionSet, PartitionCacheEntry, PartitionSet},
};

/// Create an in-memory scan from load micropartitions, like `DataFrame._from_micropartitions`
pub fn logical_plan_from_micropartitions(
    parts: Vec<MicroPartition>,
) -> DaftResult<LogicalPlanBuilder> {
    if parts.is_empty() {
        return Err(DaftError::InternalError(
            "from_micropartitions requires at least one partition.".to_string(),
        ));
    }
    // use the schema before taking all the parts
    let schema = parts[0].schema();
    // create a partition set
    let pset = MicroPartitionSet::empty();
    for (i, part) in parts.into_iter().enumerate() {
        pset.set_partition(i, &Arc::new(part))?;
    }
    // add to the runner's partition cache
    let pset_ref = Arc::new(pset);
    let cache_entry = put_partition_set_into_cache(pset_ref.clone())?;
    let partition_key = &cache_entry.key();
    let size_bytes = pset_ref.size_bytes()?;
    let num_rows = pset_ref.len();
    let num_partitions = pset_ref.num_partitions();
    // create a scan from the entry
    let builder = LogicalPlanBuilder::in_memory_scan(
        partition_key,
        cache_entry,
        schema,
        num_partitions,
        size_bytes,
        num_rows,
    )?;
    Ok(builder)
}

#[cfg(feature = "python")]
pub fn put_partition_set_into_cache(
    pset: Arc<MicroPartitionSet>,
) -> DaftResult<PartitionCacheEntry> {
    use daft_micropartition::python::PyMicroPartitionSet;
    use pyo3::{Python, types::PyAnyMethods};

    let runner = daft_runners::get_or_create_runner()?;
    Python::attach(|py| {
        // get runner as python object so we can add a partition to the cache
        let py_runner = runner.to_pyobj(py);
        let py_runner = py_runner.bind(py);
        // TODO chore: replace LocalPartitionSet with MicroPartitionSet
        //   We cannot use PyMicroPartition as a PartitionSet implementation
        //   because it's not possible to make PartitionSet the base class.
        //   This complicated using PyMicroPartition directly, and the easiest
        //   solution would be call DataFrame._from_micropartition directly.
        //   However, I wish to close this gap soon so for now I'm going to
        let py_partitioning = py.import("daft.runners.partitioning")?;
        let py_pset = py_partitioning.getattr("LocalPartitionSet")?;
        let py_pset = py_pset.call_method1(
            "_from_micropartition_set",
            (PyMicroPartitionSet::from(pset),),
        )?;
        // put in the runner's partition set cache
        let py_cache_entry = py_runner.call_method1("put_partition_set_into_cache", (py_pset,))?;
        let py_cache_entry = Arc::new(py_cache_entry.unbind());
        // create an rs cache entry
        Ok(PartitionCacheEntry::Python(py_cache_entry))
    })
}

#[cfg(not(feature = "python"))]
pub fn put_partition_set_into_cache(
    pset: Arc<MicroPartitionSet>,
) -> DaftResult<PartitionCacheEntry> {
    Err(DaftError::InternalError(
        "put_partition_set_into_cache requires 'python' feature".to_string(),
    ))
}
