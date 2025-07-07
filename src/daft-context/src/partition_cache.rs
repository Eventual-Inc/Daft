/// This module holds utilities for creating in-memory scans from SQL.
///
/// For now, it lives in daft-sql where it is used, but eventually
/// it may live in daft-micropartition or daft-logical-plan. The
/// current dependency tree makes it difficult to know which is right.
///
use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::prelude::SchemaRef;
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::{
    partitioning::{MicroPartitionSet, PartitionCacheEntry, PartitionSet},
    MicroPartition,
};

use crate::get_context;

/// Create an in-memory scan from arrow arrays, like `DataFrame._from_arrow`
pub fn logical_plan_from_arrow<S: Into<SchemaRef>>(
    schema: S,
    arrays: Vec<Box<dyn arrow2::array::Array>>,
) -> DaftResult<LogicalPlanBuilder> {
    let schema = schema.into();
    let part = MicroPartition::from_arrow(schema, arrays)?;
    logical_plan_from_micropartitions(vec![part])
}

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
    use pyo3::{types::PyAnyMethods, Python};

    let runner = get_context().get_or_create_runner()?;
    Python::with_gil(|py| {
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow2::array::Int64Array;
    use daft_core::prelude::{DataType, Field, Schema};

    use super::*;

    #[test]
    #[cfg(feature = "python")]
    fn test_from_arrow_sanity() {
        let schema = Schema::new(vec![Field::new("col1", DataType::Int64)]);
        let schema = Arc::new(schema);
        let array = Int64Array::from_vec(vec![1, 2, 3, 4]);
        let arrays = vec![Box::new(array) as Box<dyn arrow2::array::Array>];
        // verify from_arrow does not fail
        let result = logical_plan_from_arrow(schema, arrays);
        let builder = result.expect("from_arrow should have been ok");
        assert!(builder.schema().len() == 1);
    }
}
