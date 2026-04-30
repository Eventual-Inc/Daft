use std::collections::HashMap;
use std::sync::Arc;

use daft_rs::prelude::*;
use daft_rs::local_plan::{translate, Input};
use daft_rs::micropartition::MicroPartition;
use daft_rs::recordbatch::RecordBatch;

#[tokio::main]
async fn main() -> DaftResult<()> {
    // Build some in-memory data
    let rb = RecordBatch::from_nonempty_columns(vec![
        Int64Array::from_slice("id", &[1, 2, 3, 4, 5]).into_series(),
        Utf8Array::from_slice("name", &["alice", "bob", "carol", "dave", "eve"]).into_series(),
        Float64Array::from_slice("score", &[85.0, 92.0, 78.0, 95.0, 88.0]).into_series(),
    ])?;

    let schema = rb.schema.clone();
    let mp = Arc::new(MicroPartition::new_loaded(schema.clone(), Arc::new(vec![rb]), None));

    // Build logical plan
    let builder = LogicalPlanBuilder::in_memory_scan(
        "data",
        daft_rs::common::partitioning::PartitionCacheEntry::Rust {
            key: "data".into(),
            value: None,
        },
        schema,
        1,
        0,
        5,
    )?;

    let plan = builder
        .filter(resolved_col("score").gt(lit(90.0)))?
        .select(vec![resolved_col("id"), resolved_col("name"), resolved_col("score")])?
        .build();

    // Translate to physical plan
    let mut psets = HashMap::new();
    psets.insert("data".to_string(), vec![mp]);
    let (physical_plan, inputs) = translate(&plan, &psets)?;

    // Execute
    let mut executor = NativeExecutor::new(false, "");
    let exec_cfg = Arc::new(DaftExecutionConfig::default());

    let (fingerprint, future) = executor.run(
        &physical_plan,
        exec_cfg,
        vec![],
        None,
        inputs,
        0,
        true,
    )?;

    let result = future.await?;
    let partitions = result.collect_partitions_for_testing().await;

    executor.try_finish(fingerprint, 0)?.await?;

    // Display results
    for (i, partition) in partitions.iter().enumerate() {
        println!("Partition {i}:\n{partition}");
    }

    Ok(())
}
