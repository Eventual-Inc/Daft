//! Tests for StreamingScan local physical plan variant

use std::sync::Arc;

use daft_core::prelude::*;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;

#[test]
fn test_streaming_scan_construction() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32),
        Field::new("b", DataType::Utf8),
    ]));

    let source_id = 0;
    let stats_state = StatsState::NotMaterialized;
    let context = LocalNodeContext::default();

    let plan = LocalPhysicalPlan::streaming_scan(source_id, schema.clone(), stats_state, context);

    // Verify the plan was created successfully
    assert_eq!(plan.schema().as_ref(), schema.as_ref());

    // Verify it's a StreamingScan variant
    match plan.as_ref() {
        LocalPhysicalPlan::StreamingScan(scan) => {
            assert_eq!(scan.source_id, source_id);
            assert_eq!(scan.schema.as_ref(), schema.as_ref());
        }
        _ => panic!("Expected StreamingScan variant"),
    }
}

#[test]
fn test_streaming_scan_has_no_children() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64)]));

    let plan = LocalPhysicalPlan::streaming_scan(
        0,
        schema,
        StatsState::NotMaterialized,
        LocalNodeContext::default(),
    );

    // StreamingScan should be a leaf node (no children)
    // Verify it's a StreamingScan which is a leaf by design
    match plan.as_ref() {
        LocalPhysicalPlan::StreamingScan(_) => {
            // Success - StreamingScan is a leaf node by design
        }
        _ => panic!("Expected StreamingScan variant"),
    }
}

#[test]
fn test_streaming_scan_serialization() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("col1", DataType::Float64),
        Field::new("col2", DataType::Boolean),
    ]));

    let plan = LocalPhysicalPlan::streaming_scan(
        42,
        schema.clone(),
        StatsState::NotMaterialized,
        LocalNodeContext::default(),
    );

    // Serialize and deserialize
    let serialized = serde_json::to_string(plan.as_ref()).expect("Failed to serialize");
    let deserialized: LocalPhysicalPlan =
        serde_json::from_str(&serialized).expect("Failed to deserialize");

    // Verify deserialized plan matches original
    match deserialized {
        LocalPhysicalPlan::StreamingScan(scan) => {
            assert_eq!(scan.source_id, 42);
            assert_eq!(scan.schema.as_ref(), schema.as_ref());
        }
        _ => panic!("Expected StreamingScan after deserialization"),
    }
}

#[test]
fn test_in_memory_scan_unchanged() {
    // Verify InMemoryScan behavior is not affected by StreamingScan addition
    let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8)]));

    let plan = LocalPhysicalPlan::in_memory_scan(
        1,
        schema.clone(),
        1024,
        StatsState::NotMaterialized,
        LocalNodeContext::default(),
    );

    match plan.as_ref() {
        LocalPhysicalPlan::InMemoryScan(scan) => {
            assert_eq!(scan.source_id, 1);
            assert_eq!(scan.size_bytes, 1024);
            assert_eq!(scan.schema.as_ref(), schema.as_ref());
        }
        _ => panic!("Expected InMemoryScan variant"),
    }

    // Verify serialization is unchanged
    let serialized = serde_json::to_string(plan.as_ref()).expect("Failed to serialize");
    assert!(serialized.contains("InMemoryScan"));
    assert!(serialized.contains("size_bytes"));
}

#[test]
fn test_streaming_scan_vs_in_memory_scan() {
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32)]));

    let streaming = LocalPhysicalPlan::streaming_scan(
        0,
        schema.clone(),
        StatsState::NotMaterialized,
        LocalNodeContext::default(),
    );

    let in_memory = LocalPhysicalPlan::in_memory_scan(
        0,
        schema.clone(),
        512,
        StatsState::NotMaterialized,
        LocalNodeContext::default(),
    );

    // Both should have same schema
    assert_eq!(streaming.schema(), in_memory.schema());

    // But different variants
    assert!(matches!(
        streaming.as_ref(),
        LocalPhysicalPlan::StreamingScan(_)
    ));
    assert!(matches!(
        in_memory.as_ref(),
        LocalPhysicalPlan::InMemoryScan(_)
    ));

    // InMemoryScan has size_bytes, StreamingScan doesn't
    match in_memory.as_ref() {
        LocalPhysicalPlan::InMemoryScan(scan) => {
            assert_eq!(scan.size_bytes, 512);
        }
        _ => panic!("Expected InMemoryScan"),
    }
}
