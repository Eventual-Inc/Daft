use std::{collections::HashMap, sync::Arc};

use common_treenode::TreeNode;
use daft_dsl::{lit, resolved_col};
use daft_logical_plan::{
    builder::LogicalPlanBuilder, ops::Source, source_info::PlaceHolderInfo, ClusteringSpec,
    LogicalPlan, LogicalPlanRef, SourceInfo,
};
use daft_schema::{dtype::DataType, field::Field, schema::Schema};

use crate::{
    pipeline_node::NodeID,
    plan::PlanID,
    scheduling::task::{TaskContext, TaskID},
    stage::StageID,
    statistics::{http_subscriber::HttpSubscriber, PlanState, TaskExecutionStatus, TaskState},
};

/// Create a test logical plan with proper plan IDs that match task context node IDs
fn create_test_logical_plan() -> LogicalPlanRef {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8),
        Field::new("age", DataType::Int32),
        Field::new("salary", DataType::Float64),
    ]));

    // Create a simple plan: Source -> Filter -> Project -> Limit
    // Assign plan IDs that match the task context node IDs used in tests
    let source_plan = LogicalPlan::Source(Source::new(
        schema.clone(),
        Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
            source_schema: schema,
            clustering_spec: Arc::new(ClusteringSpec::unknown()),
        })),
    ))
    .arced()
    .with_node_id(1); // Node ID 1 for Source

    // Build a more complex plan using LogicalPlanBuilder
    let filter_plan = LogicalPlanBuilder::from(source_plan.arced())
        .filter(resolved_col("age").gt(lit(18)))
        .unwrap()
        .build()
        .with_node_id(2); // Node ID 2 for Filter

    let project_plan = LogicalPlanBuilder::from(filter_plan.arced())
        .select(vec![resolved_col("name"), resolved_col("salary")])
        .unwrap()
        .build()
        .with_node_id(3); // Node ID 3 for Project

    let limit_plan = LogicalPlanBuilder::from(project_plan.arced())
        .limit(100, false)
        .unwrap()
        .build()
        .with_node_id(4); // Node ID 4 for Limit

    limit_plan.arced()
}

/// Create a simple test logical plan for aggregation tests (Source -> Filter only)
fn create_simple_test_logical_plan() -> LogicalPlanRef {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8),
        Field::new("age", DataType::Int32),
        Field::new("salary", DataType::Float64),
    ]));

    // Create a simple plan: Source -> Filter
    // Assign plan IDs that match the task context node IDs used in aggregation tests
    let source_plan = LogicalPlan::Source(Source::new(
        schema.clone(),
        Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
            source_schema: schema,
            clustering_spec: Arc::new(ClusteringSpec::unknown()),
        })),
    ))
    .arced()
    .with_node_id(1); // Node ID 1 for Source

    // Build a filter plan using LogicalPlanBuilder
    let filter_plan = LogicalPlanBuilder::from(source_plan.arced())
        .filter(resolved_col("age").gt(lit(18)))
        .unwrap()
        .build()
        .with_node_id(2); // Node ID 2 for Filter

    filter_plan.arced()
}

/// Helper function to create test plan state
fn create_test_plan_state(plan_id: PlanID, logical_plan: LogicalPlanRef) -> PlanState {
    PlanState {
        plan_id,
        query_id: uuid::Uuid::new_v4().to_string(),
        logical_plan,
    }
}

/// Helper function to create test task context
fn create_test_task_context(
    plan_id: PlanID,
    stage_id: StageID,
    node_id: NodeID,
    task_id: TaskID,
    logical_node_id: Option<NodeID>,
) -> TaskContext {
    TaskContext {
        logical_node_id,
        plan_id,
        stage_id,
        node_id,
        task_id,
    }
}

/// Helper function to create test task state
fn create_test_task_state(name: &str, status: TaskExecutionStatus) -> TaskState {
    TaskState {
        name: name.to_string(),
        status,
        pending: 1,
        completed: 0,
        canceled: 0,
        failed: 0,
        total: 1,
    }
}

/// Helper function to create test plan data
fn create_test_plan_data(
    plan_id: PlanID,
    logical_plan: LogicalPlanRef,
) -> crate::statistics::http_subscriber::PlanData {
    let plan_state = create_test_plan_state(plan_id, logical_plan);
    crate::statistics::http_subscriber::PlanData::new(plan_state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_query_graph_simple() {
        // Create a simple logical plan
        let logical_plan = create_test_logical_plan();

        // Create plan data
        let mut plan_data = create_test_plan_data(1, logical_plan);

        // Create tasks for different nodes in the plan
        let source_context = create_test_task_context(1, 1, 1, 1, Some(1));
        let filter_context = create_test_task_context(1, 1, 2, 2, Some(2));
        let project_context = create_test_task_context(1, 1, 3, 3, Some(3));
        let limit_context = create_test_task_context(1, 1, 4, 4, Some(4));

        plan_data.tasks.insert(
            source_context,
            create_test_task_state("Source", TaskExecutionStatus::Completed),
        );
        plan_data.tasks.insert(
            filter_context,
            create_test_task_state("Filter", TaskExecutionStatus::Running),
        );
        plan_data.tasks.insert(
            project_context,
            create_test_task_state("Project", TaskExecutionStatus::Created),
        );
        plan_data.tasks.insert(
            limit_context,
            create_test_task_state("Limit", TaskExecutionStatus::Created),
        );

        // Build the query graph
        let query_graph = HttpSubscriber::build_query_graph(&plan_data);

        // Verify the basic structure
        assert_eq!(query_graph.version, "1.0.0");
        assert_eq!(query_graph.plan_id, 1);
        assert_eq!(query_graph.nodes.len(), 4);

        // Check that nodes have expected IDs
        let node_ids: Vec<NodeID> = query_graph.nodes.iter().map(|n| n.id).collect();
        assert!(node_ids.contains(&source_context.logical_node_id.unwrap()));
        assert!(node_ids.contains(&filter_context.logical_node_id.unwrap()));
        assert!(node_ids.contains(&project_context.logical_node_id.unwrap()));
        assert!(node_ids.contains(&limit_context.logical_node_id.unwrap()));

        // Check that adjacency list exists
        assert!(!query_graph.adjacency_list.is_empty());

        // Pretty print the adjacency list for debugging
        println!("Adjacency List:");
        for (node_id, neighbors) in &query_graph.adjacency_list {
            println!("  Node {}: {:?}", node_id, neighbors);
        }
    }

    #[test]
    fn test_adjacency_list_structure() {
        let _subscriber = HttpSubscriber::new();

        let logical_plan = create_test_logical_plan();
        let mut plan_data = create_test_plan_data(1, logical_plan);

        // Create a linear chain of tasks: 1 -> 2 -> 3 -> 4
        let context1 = create_test_task_context(1, 1, 1, 1, Some(1));
        let context2 = create_test_task_context(1, 1, 2, 2, Some(2));
        let context3 = create_test_task_context(1, 1, 3, 3, Some(3));
        let context4 = create_test_task_context(1, 1, 4, 4, Some(4));

        plan_data.tasks.insert(
            context1,
            create_test_task_state("Source", TaskExecutionStatus::Completed),
        );
        plan_data.tasks.insert(
            context2,
            create_test_task_state("Filter", TaskExecutionStatus::Running),
        );
        plan_data.tasks.insert(
            context3,
            create_test_task_state("Project", TaskExecutionStatus::Created),
        );
        plan_data.tasks.insert(
            context4,
            create_test_task_state("Limit", TaskExecutionStatus::Created),
        );

        let query_graph = HttpSubscriber::build_query_graph(&plan_data);

        // Generate expected node IDs
        let expected_node_ids = vec![
            context1.logical_node_id.unwrap(),
            context2.logical_node_id.unwrap(),
            context3.logical_node_id.unwrap(),
            context4.logical_node_id.unwrap(),
        ];

        // Check that we have the right number of nodes
        assert_eq!(query_graph.nodes.len(), 4);

        // Check that all expected nodes are present in the graph
        let actual_node_ids: Vec<NodeID> = query_graph.nodes.iter().map(|n| n.id).collect();
        for expected_id in &expected_node_ids {
            assert!(
                actual_node_ids.contains(expected_id),
                "Expected node ID {} not found in actual node IDs {:?}",
                expected_id,
                actual_node_ids
            );
        }
    }

    #[test]
    fn test_multiple_tasks_progress_aggregation() {
        let logical_plan = create_simple_test_logical_plan();

        let mut plan_data = create_test_plan_data(1, logical_plan);

        // Create multiple tasks for the same node (node_id = 1) with different task_ids
        // These should be aggregated into a single graph node
        let context1 = create_test_task_context(1, 1, 1, 1, Some(1)); // Task 1 for node 1
        let context2 = create_test_task_context(1, 1, 1, 2, Some(1)); // Task 2 for node 1
        let context3 = create_test_task_context(1, 1, 1, 3, Some(1));

        // Create task states with different progress metrics
        let mut task1 = create_test_task_state("Source", TaskExecutionStatus::Completed);
        task1.pending = 0;
        task1.completed = 5;
        task1.failed = 1;
        task1.canceled = 0;
        task1.total = 6;

        let mut task2 = create_test_task_state("Source", TaskExecutionStatus::Running);
        task2.pending = 2;
        task2.completed = 3;
        task2.failed = 0;
        task2.canceled = 1;
        task2.total = 6;

        let mut task3 = create_test_task_state("Source", TaskExecutionStatus::Created);
        task3.pending = 8;
        task3.completed = 0;
        task3.failed = 0;
        task3.canceled = 0;
        task3.total = 8;

        plan_data.tasks.insert(context1, task1);
        plan_data.tasks.insert(context2, task2);
        plan_data.tasks.insert(context3, task3);

        // Add a task for a different node to ensure we don't aggregate across different nodes
        let context4 = create_test_task_context(1, 1, 2, 4, Some(2)); // Task 4 for node 2
        let mut task4 = create_test_task_state("Filter", TaskExecutionStatus::Running);
        task4.pending = 1;
        task4.completed = 2;
        task4.failed = 0;
        task4.canceled = 0;
        task4.total = 3;

        plan_data.tasks.insert(context4, task4);

        let query_graph = HttpSubscriber::build_query_graph(&plan_data);

        // Verify basic structure
        assert_eq!(query_graph.version, "1.0.0");
        assert_eq!(query_graph.plan_id, 1);
        assert_eq!(query_graph.nodes.len(), 2); // Should have 2 nodes (node 1 and node 2)

        // Find the aggregated node for node_id = 1
        let source_node_id = context1.logical_node_id; // All contexts with same node_id generate same ID
        let source_node = query_graph
            .nodes
            .iter()
            .find(|node| node.id == source_node_id.unwrap())
            .expect("Source node should exist");

        // Verify aggregated metrics for node 1 (sum of all 3 tasks)
        // pending: 0 + 2 + 8 = 10
        // completed: 5 + 3 + 0 = 8
        // failed: 1 + 0 + 0 = 1
        // canceled: 0 + 1 + 0 = 1
        // total: 6 + 6 + 8 = 20
        assert_eq!(source_node.pending, 10);
        assert_eq!(source_node.completed, 8);
        assert_eq!(source_node.failed, 1);
        assert_eq!(source_node.canceled, 1);
        assert_eq!(source_node.total, 20);

        // The status should be based on the aggregation logic
        // With multiple statuses (Completed, Running, Created), it should be Running
        assert_eq!(
            source_node.status,
            crate::statistics::http_subscriber::NodeStatus::Running
        );

        // Find the node for node_id = 2
        let filter_node_id = context4.logical_node_id;
        let filter_node = query_graph
            .nodes
            .iter()
            .find(|node| node.id == filter_node_id.unwrap())
            .expect("Filter node should exist");

        // Verify metrics for node 2 (single task)
        assert_eq!(filter_node.pending, 1);
        assert_eq!(filter_node.completed, 2);
        assert_eq!(filter_node.failed, 0);
        assert_eq!(filter_node.canceled, 0);
        assert_eq!(filter_node.total, 3);
        assert_eq!(
            filter_node.status,
            crate::statistics::http_subscriber::NodeStatus::Running
        );

        // Verify metadata contains correct plan_id, stage_id, and node_id
        assert_eq!(source_node.metadata.get("plan_id"), Some(&"1".to_string()));
        assert_eq!(source_node.metadata.get("stage_id"), Some(&"1".to_string()));
        assert_eq!(source_node.metadata.get("node_id"), Some(&"1".to_string()));

        assert_eq!(filter_node.metadata.get("plan_id"), Some(&"1".to_string()));
        assert_eq!(filter_node.metadata.get("stage_id"), Some(&"1".to_string()));
        assert_eq!(filter_node.metadata.get("node_id"), Some(&"2".to_string()));

        // Verify adjacency list shows relationship
        assert!(!query_graph.adjacency_list.is_empty());
        // The filter node should point to the source node
        assert_eq!(
            query_graph.adjacency_list.get(&filter_node_id.unwrap()),
            Some(&vec![source_node_id.unwrap()])
        );
    }

    /// Pretty print the query graph structure
    #[allow(dead_code)]
    fn pretty_print_query_graph(query_graph: &crate::statistics::http_subscriber::QueryGraph) {
        println!("═══ Query Graph Debug Info ═══");
        println!("Version: {}", query_graph.version);
        println!("Plan ID: {}", query_graph.plan_id);
        println!("Nodes ({}):", query_graph.nodes.len());
        for node in &query_graph.nodes {
            println!(
                "  ├─ ID: {}, Label: '{}', Status: {:?}",
                node.id, node.label, node.status
            );
            println!("  │  Description: '{}'", node.description);
            println!(
                "  │  Metrics: P:{}, C:{}, F:{}, T:{}",
                node.pending, node.completed, node.failed, node.total
            );
            for (key, value) in &node.metadata {
                println!("  │  {}: {}", key, value);
            }
        }
        println!(
            "Adjacency List ({} entries):",
            query_graph.adjacency_list.len()
        );
        if query_graph.adjacency_list.is_empty() {
            println!("  (empty)");
        } else {
            for (node_id, neighbors) in &query_graph.adjacency_list {
                println!("  {} -> {:?}", node_id, neighbors);
            }
        }
        println!("═══════════════════════════════");
    }

    /// Debug logical plan structure
    #[allow(dead_code)]
    fn debug_logical_plan_structure(plan: &daft_logical_plan::LogicalPlan) {
        println!("═══ Logical Plan Debug Info ═══");
        let plan_arc = std::sync::Arc::new(plan.clone());

        let mut plan_ids = Vec::new();
        let _ = plan_arc.apply(|node| {
            if let Some(plan_id) = node.plan_id() {
                plan_ids.push(*plan_id);
                println!(
                    "Plan node ID: {}, Type: {:?}",
                    plan_id,
                    std::mem::discriminant(node.as_ref())
                );
            } else {
                println!(
                    "Plan node with no ID, Type: {:?}",
                    std::mem::discriminant(node.as_ref())
                );
            }
            Ok(common_treenode::TreeNodeRecursion::Continue)
        });

        println!("All plan IDs found: {:?}", plan_ids);
        println!("═══════════════════════════════");
    }

    #[test]
    fn test_query_graph_serialization() {
        // Create a test logical plan
        let logical_plan = create_test_logical_plan();

        // Create plan data
        let mut plan_data = create_test_plan_data(1, logical_plan);

        // Create task states
        let source_context = create_test_task_context(1, 1, 1, 1, Some(1));
        let filter_context = create_test_task_context(1, 1, 2, 2, Some(2));

        plan_data.tasks.insert(
            source_context,
            create_test_task_state("Source", TaskExecutionStatus::Completed),
        );
        plan_data.tasks.insert(
            filter_context,
            create_test_task_state("Filter", TaskExecutionStatus::Running),
        );

        // Build the query graph
        let original_query_graph = HttpSubscriber::build_query_graph(&plan_data);

        // Test QueryGraph serialization and deserialization
        let serialized_graph = serde_json::to_string(&original_query_graph)
            .expect("QueryGraph should serialize to JSON");

        println!("Serialized QueryGraph JSON: {}", serialized_graph);

        let deserialized_graph: crate::statistics::http_subscriber::QueryGraph =
            serde_json::from_str(&serialized_graph)
                .expect("QueryGraph should deserialize from JSON");

        // Verify the deserialized graph matches the original
        assert_eq!(deserialized_graph.version, original_query_graph.version);
        assert_eq!(deserialized_graph.plan_id, original_query_graph.plan_id);
        assert_eq!(
            deserialized_graph.nodes.len(),
            original_query_graph.nodes.len()
        );
        assert_eq!(
            deserialized_graph.adjacency_list.len(),
            original_query_graph.adjacency_list.len()
        );

        // Verify individual nodes
        for original_node in &original_query_graph.nodes {
            let deserialized_node = deserialized_graph
                .nodes
                .iter()
                .find(|n| n.id == original_node.id)
                .expect("Node should exist after deserialization");

            assert_eq!(deserialized_node.id, original_node.id);
            assert_eq!(deserialized_node.label, original_node.label);
            assert_eq!(deserialized_node.description, original_node.description);
            assert_eq!(deserialized_node.status, original_node.status);
            assert_eq!(deserialized_node.pending, original_node.pending);
            assert_eq!(deserialized_node.completed, original_node.completed);
            assert_eq!(deserialized_node.canceled, original_node.canceled);
            assert_eq!(deserialized_node.failed, original_node.failed);
            assert_eq!(deserialized_node.total, original_node.total);
            assert_eq!(deserialized_node.metadata, original_node.metadata);
        }

        // Verify adjacency list
        for (key, value) in &original_query_graph.adjacency_list {
            let deserialized_value = deserialized_graph
                .adjacency_list
                .get(key)
                .expect("Adjacency list entry should exist");
            assert_eq!(deserialized_value, value);
        }
    }

    #[test]
    fn test_query_payload_serialization() {
        // Create a test QueryPayload
        let original_payload = crate::statistics::http_subscriber::QueryPayload {
            id: "test-query-123".to_string(),
            optimized_plan: r#"{"version":"1.0.0","query_id":1,"nodes":[],"adjacency_list":{}}"#
                .to_string(),
            run_id: Some("run-456".to_string()),
            logs: "Test log message".to_string(),
            sequence: 1,
        };

        // Test serialization
        let serialized_payload = serde_json::to_string(&original_payload)
            .expect("QueryPayload should serialize to JSON");

        println!("Serialized QueryPayload JSON: {}", serialized_payload);

        // Test deserialization
        let deserialized_payload: crate::statistics::http_subscriber::QueryPayload =
            serde_json::from_str(&serialized_payload)
                .expect("QueryPayload should deserialize from JSON");

        // Verify the deserialized payload matches the original
        assert_eq!(deserialized_payload.id, original_payload.id);
        assert_eq!(
            deserialized_payload.optimized_plan,
            original_payload.optimized_plan
        );
        assert_eq!(deserialized_payload.run_id, original_payload.run_id);
        assert_eq!(deserialized_payload.logs, original_payload.logs);
    }

    #[test]
    fn test_node_status_serialization() {
        let test_cases = vec![
            (
                crate::statistics::http_subscriber::NodeStatus::Created,
                "\"created\"",
            ),
            (
                crate::statistics::http_subscriber::NodeStatus::Running,
                "\"running\"",
            ),
            (
                crate::statistics::http_subscriber::NodeStatus::Completed,
                "\"completed\"",
            ),
            (
                crate::statistics::http_subscriber::NodeStatus::Failed,
                "\"failed\"",
            ),
            (
                crate::statistics::http_subscriber::NodeStatus::Canceled,
                "\"canceled\"",
            ),
        ];

        for (status, expected_json) in test_cases {
            // Test serialization
            let serialized =
                serde_json::to_string(&status).expect("NodeStatus should serialize to JSON");
            assert_eq!(serialized, expected_json);

            // Test deserialization
            let deserialized: crate::statistics::http_subscriber::NodeStatus =
                serde_json::from_str(&serialized).expect("NodeStatus should deserialize from JSON");
            assert_eq!(deserialized, status);
        }
    }

    #[test]
    fn test_metric_display_information_serialization() {
        let original_metric = crate::statistics::http_subscriber::MetricDisplayInformation {
            name: "execution_time".to_string(),
            description: "Total execution time in seconds".to_string(),
            value: 123.45,
            unit: "seconds".to_string(),
        };

        // Test serialization
        let serialized_metric = serde_json::to_string(&original_metric)
            .expect("MetricDisplayInformation should serialize to JSON");

        println!(
            "Serialized MetricDisplayInformation JSON: {}",
            serialized_metric
        );

        // Test deserialization
        let deserialized_metric: crate::statistics::http_subscriber::MetricDisplayInformation =
            serde_json::from_str(&serialized_metric)
                .expect("MetricDisplayInformation should deserialize from JSON");

        // Verify the deserialized metric matches the original
        assert_eq!(deserialized_metric.name, original_metric.name);
        assert_eq!(deserialized_metric.description, original_metric.description);
        assert_eq!(deserialized_metric.value, original_metric.value);
        assert_eq!(deserialized_metric.unit, original_metric.unit);
    }

    #[test]
    fn test_complex_query_graph_with_metrics_serialization() {
        // Create a complex QueryGraph with metrics to test all serialization paths
        let metrics = vec![
            crate::statistics::http_subscriber::MetricDisplayInformation {
                name: "rows_processed".to_string(),
                description: "Number of rows processed".to_string(),
                value: 1000.0,
                unit: "rows".to_string(),
            },
            crate::statistics::http_subscriber::MetricDisplayInformation {
                name: "memory_used".to_string(),
                description: "Memory usage in MB".to_string(),
                value: 256.5,
                unit: "MB".to_string(),
            },
        ];

        let mut metadata = HashMap::new();
        metadata.insert("plan_id".to_string(), "1".to_string());
        metadata.insert("stage_id".to_string(), "1".to_string());
        metadata.insert("node_id".to_string(), "1".to_string());
        metadata.insert("custom_field".to_string(), "test_value".to_string());

        let node = crate::statistics::http_subscriber::QueryGraphNode {
            id: 12345,
            label: "TestNode".to_string(),
            description: "A test node with metrics".to_string(),
            metadata,
            status: crate::statistics::http_subscriber::NodeStatus::Running,
            pending: 10,
            completed: 20,
            canceled: 1,
            failed: 2,
            total: 33,
            metrics: Some(metrics.clone()),
        };

        let mut adjacency_list = HashMap::new();
        adjacency_list.insert(1, vec![2, 3]);
        adjacency_list.insert(2, vec![4]);

        let original_graph = crate::statistics::http_subscriber::QueryGraph {
            version: "2.0.0".to_string(),
            plan_id: 42,
            nodes: vec![node],
            adjacency_list,
            metrics: Some(metrics),
        };

        // Test serialization
        let serialized_graph = serde_json::to_string_pretty(&original_graph)
            .expect("Complex QueryGraph should serialize to JSON");

        println!("Serialized Complex QueryGraph JSON:\n{}", serialized_graph);

        // Test deserialization
        let deserialized_graph: crate::statistics::http_subscriber::QueryGraph =
            serde_json::from_str(&serialized_graph)
                .expect("Complex QueryGraph should deserialize from JSON");

        // Verify all fields
        assert_eq!(deserialized_graph.version, original_graph.version);
        assert_eq!(deserialized_graph.plan_id, original_graph.plan_id);
        assert_eq!(deserialized_graph.nodes.len(), original_graph.nodes.len());
        assert_eq!(
            deserialized_graph.adjacency_list.len(),
            original_graph.adjacency_list.len()
        );

        // Verify metrics at graph level
        assert!(deserialized_graph.metrics.is_some());
        let deserialized_metrics = deserialized_graph.metrics.unwrap();
        let original_metrics = original_graph.metrics.unwrap();
        assert_eq!(deserialized_metrics.len(), original_metrics.len());

        // Verify node details including metrics
        let deserialized_node = &deserialized_graph.nodes[0];
        let original_node = &original_graph.nodes[0];

        assert_eq!(deserialized_node.id, original_node.id);
        assert_eq!(deserialized_node.label, original_node.label);
        assert_eq!(deserialized_node.description, original_node.description);
        assert_eq!(deserialized_node.status, original_node.status);
        assert_eq!(deserialized_node.pending, original_node.pending);
        assert_eq!(deserialized_node.completed, original_node.completed);
        assert_eq!(deserialized_node.canceled, original_node.canceled);
        assert_eq!(deserialized_node.failed, original_node.failed);
        assert_eq!(deserialized_node.total, original_node.total);
        assert_eq!(deserialized_node.metadata, original_node.metadata);

        // Verify node-level metrics
        assert!(deserialized_node.metrics.is_some());
        let deserialized_node_metrics = deserialized_node.metrics.as_ref().unwrap();
        let original_node_metrics = original_node.metrics.as_ref().unwrap();
        assert_eq!(deserialized_node_metrics.len(), original_node_metrics.len());

        for (deserialized_metric, original_metric) in deserialized_node_metrics
            .iter()
            .zip(original_node_metrics.iter())
        {
            assert_eq!(deserialized_metric.name, original_metric.name);
            assert_eq!(deserialized_metric.description, original_metric.description);
            assert_eq!(deserialized_metric.value, original_metric.value);
            assert_eq!(deserialized_metric.unit, original_metric.unit);
        }
    }

    #[test]
    fn test_serialization_round_trip_preserves_data() {
        // Create a comprehensive test setup
        let logical_plan = create_test_logical_plan();
        let mut plan_data = create_test_plan_data(1, logical_plan);

        // Create multiple tasks with various states
        let contexts_and_states = vec![
            (
                create_test_task_context(1, 1, 1, 1, Some(1)),
                create_test_task_state("Source", TaskExecutionStatus::Completed),
            ),
            (
                create_test_task_context(1, 1, 2, 2, Some(2)),
                create_test_task_state("Filter", TaskExecutionStatus::Running),
            ),
            (
                create_test_task_context(1, 1, 3, 3, Some(3)),
                create_test_task_state("Project", TaskExecutionStatus::Created),
            ),
            (
                create_test_task_context(1, 1, 4, 4, Some(4)),
                create_test_task_state("Limit", TaskExecutionStatus::Failed),
            ),
        ];

        for (context, task_state) in contexts_and_states {
            plan_data.tasks.insert(context, task_state);
        }

        // Build original query graph
        let original_query_graph = HttpSubscriber::build_query_graph(&plan_data);

        // Perform multiple round-trip serializations
        let mut current_graph = original_query_graph;
        for i in 0..3 {
            let serialized = serde_json::to_string(&current_graph).expect(&format!(
                "Round trip {} serialization should succeed",
                i + 1
            ));

            let deserialized: crate::statistics::http_subscriber::QueryGraph =
                serde_json::from_str(&serialized).expect(&format!(
                    "Round trip {} deserialization should succeed",
                    i + 1
                ));

            // Verify that the data is preserved across serialization cycles
            assert_eq!(deserialized.version, current_graph.version);
            assert_eq!(deserialized.plan_id, current_graph.plan_id);
            assert_eq!(deserialized.nodes.len(), current_graph.nodes.len());
            assert_eq!(
                deserialized.adjacency_list.len(),
                current_graph.adjacency_list.len()
            );

            current_graph = deserialized;
        }
    }

    #[test]
    fn test_optimized_plan_query_graph_serialization() {
        // Create a more complex logical plan that would benefit from optimization
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64),
            Field::new("name", DataType::Utf8),
            Field::new("age", DataType::Int32),
            Field::new("salary", DataType::Float64),
            Field::new("department", DataType::Utf8),
        ]));

        // Create an optimized plan: Source -> Filter -> Project -> Sort -> Limit
        let source_plan = LogicalPlan::Source(Source::new(
            schema.clone(),
            Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
                source_schema: schema,
                clustering_spec: Arc::new(ClusteringSpec::unknown()),
            })),
        ))
        .arced()
        .with_node_id(1); // Node ID 1 for Source

        // Add multiple operations to create a more complex optimized plan
        let filter_plan = LogicalPlanBuilder::from(source_plan.arced())
            .filter(
                resolved_col("age")
                    .gt(lit(25))
                    .and(resolved_col("salary").gt(lit(50000.0))),
            )
            .unwrap()
            .build()
            .with_node_id(2); // Node ID 2 for Filter

        let project_plan = LogicalPlanBuilder::from(filter_plan.arced())
            .select(vec![
                resolved_col("id"),
                resolved_col("name"),
                resolved_col("department"),
                resolved_col("salary"),
            ])
            .unwrap()
            .build()
            .with_node_id(3); // Node ID 3 for Project

        let sort_plan = LogicalPlanBuilder::from(project_plan.arced())
            .sort(vec![resolved_col("salary")], vec![false], vec![true])
            .unwrap()
            .build()
            .with_node_id(4); // Node ID 4 for Sort

        let limit_plan = LogicalPlanBuilder::from(sort_plan.arced())
            .limit(10, false)
            .unwrap()
            .build()
            .with_node_id(5); // Node ID 5 for Limit

        // Create plan data for the optimized plan
        let mut plan_data = create_test_plan_data(1, limit_plan.arced());

        // Create task states for each operation in the optimized plan
        let source_context = create_test_task_context(1, 1, 1, 1, Some(1));
        let filter_context = create_test_task_context(1, 1, 2, 2, Some(2));
        let project_context = create_test_task_context(1, 1, 3, 3, Some(3));
        let sort_context = create_test_task_context(1, 1, 4, 4, Some(4));
        let limit_context = create_test_task_context(1, 1, 5, 5, Some(5));

        // Set different execution states to simulate a real query execution
        let mut source_task = create_test_task_state("Source", TaskExecutionStatus::Completed);
        source_task.pending = 0;
        source_task.completed = 100;
        source_task.failed = 0;
        source_task.canceled = 0;
        source_task.total = 100;

        let mut filter_task = create_test_task_state("Filter", TaskExecutionStatus::Completed);
        filter_task.pending = 0;
        filter_task.completed = 45;
        filter_task.failed = 0;
        filter_task.canceled = 0;
        filter_task.total = 45;

        let mut project_task = create_test_task_state("Project", TaskExecutionStatus::Running);
        project_task.pending = 10;
        project_task.completed = 35;
        project_task.failed = 0;
        project_task.canceled = 0;
        project_task.total = 45;

        let mut sort_task = create_test_task_state("Sort", TaskExecutionStatus::Created);
        sort_task.pending = 45;
        sort_task.completed = 0;
        sort_task.failed = 0;
        sort_task.canceled = 0;
        sort_task.total = 45;

        let mut limit_task = create_test_task_state("Limit", TaskExecutionStatus::Created);
        limit_task.pending = 10;
        limit_task.completed = 0;
        limit_task.failed = 0;
        limit_task.canceled = 0;
        limit_task.total = 10;

        plan_data.tasks.insert(source_context, source_task);
        plan_data.tasks.insert(filter_context, filter_task);
        plan_data.tasks.insert(project_context, project_task);
        plan_data.tasks.insert(sort_context, sort_task);
        plan_data.tasks.insert(limit_context, limit_task);

        // Build the query graph from the optimized plan
        let original_query_graph = HttpSubscriber::build_query_graph(&plan_data);

        // Verify the graph structure
        assert_eq!(original_query_graph.version, "1.0.0");
        assert_eq!(original_query_graph.plan_id, 1);
        assert_eq!(original_query_graph.nodes.len(), 5);

        // Test serialization
        let serialized_graph = serde_json::to_string_pretty(&original_query_graph)
            .expect("Optimized QueryGraph should serialize to JSON");

        // println!("=== Optimized Plan Query Graph ===");
        // println!("Serialized Optimized QueryGraph JSON:\n{}", serialized_graph);

        // Test deserialization
        let deserialized_graph: crate::statistics::http_subscriber::QueryGraph =
            serde_json::from_str(&serialized_graph)
                .expect("Optimized QueryGraph should deserialize from JSON");

        // Verify the deserialized graph matches the original
        assert_eq!(deserialized_graph.version, original_query_graph.version);
        assert_eq!(deserialized_graph.plan_id, original_query_graph.plan_id);
        assert_eq!(
            deserialized_graph.nodes.len(),
            original_query_graph.nodes.len()
        );
        assert_eq!(
            deserialized_graph.adjacency_list.len(),
            original_query_graph.adjacency_list.len()
        );

        // Verify individual nodes and their states
        for original_node in &original_query_graph.nodes {
            let deserialized_node = deserialized_graph
                .nodes
                .iter()
                .find(|n| n.id == original_node.id)
                .expect("Node should exist after deserialization");

            assert_eq!(deserialized_node.id, original_node.id);
            assert_eq!(deserialized_node.label, original_node.label);
            assert_eq!(deserialized_node.description, original_node.description);
            assert_eq!(deserialized_node.status, original_node.status);
            assert_eq!(deserialized_node.pending, original_node.pending);
            assert_eq!(deserialized_node.completed, original_node.completed);
            assert_eq!(deserialized_node.canceled, original_node.canceled);
            assert_eq!(deserialized_node.failed, original_node.failed);
            assert_eq!(deserialized_node.total, original_node.total);
            assert_eq!(deserialized_node.metadata, original_node.metadata);
        }

        // Verify adjacency list (execution order dependencies)
        for (key, value) in &original_query_graph.adjacency_list {
            let deserialized_value = deserialized_graph
                .adjacency_list
                .get(key)
                .expect("Adjacency list entry should exist");
            assert_eq!(deserialized_value, value);
        }

        // Verify that the optimized plan shows proper execution pipeline
        // Check that we have the expected node IDs
        let expected_node_ids = vec![
            source_context.logical_node_id.unwrap(),
            filter_context.logical_node_id.unwrap(),
            project_context.logical_node_id.unwrap(),
            sort_context.logical_node_id.unwrap(),
            limit_context.logical_node_id.unwrap(),
        ];

        let actual_node_ids: Vec<NodeID> = deserialized_graph.nodes.iter().map(|n| n.id).collect();
        for expected_id in &expected_node_ids {
            assert!(
                actual_node_ids.contains(expected_id),
                "Expected node ID {} not found in actual node IDs {:?}",
                expected_id,
                actual_node_ids
            );
        }

        // Verify execution flow: Source -> Filter -> Project -> Sort -> Limit
        let limit_node_id = limit_context.logical_node_id.unwrap();
        let sort_node_id = sort_context.logical_node_id.unwrap();
        let project_node_id = project_context.logical_node_id.unwrap();
        let filter_node_id = filter_context.logical_node_id.unwrap();
        let source_node_id = source_context.logical_node_id.unwrap();

        // Check dependencies in adjacency list
        assert_eq!(
            deserialized_graph.adjacency_list.get(&limit_node_id),
            Some(&vec![sort_node_id])
        );
        assert_eq!(
            deserialized_graph.adjacency_list.get(&sort_node_id),
            Some(&vec![project_node_id])
        );
        assert_eq!(
            deserialized_graph.adjacency_list.get(&project_node_id),
            Some(&vec![filter_node_id])
        );
        assert_eq!(
            deserialized_graph.adjacency_list.get(&filter_node_id),
            Some(&vec![source_node_id])
        );
        assert_eq!(
            deserialized_graph.adjacency_list.get(&source_node_id),
            Some(&vec![])
        );
    }

    #[test]
    fn test_flush_functionality_with_empty_payload() {
        // Test that flush() returns immediately when there are no pending HTTP requests
        let mut subscriber = HttpSubscriber::new();

        // Since no events have been processed, the payload should be empty
        // and flush should return immediately without waiting
        let start_time = std::time::Instant::now();
        let result = subscriber.flush();
        let elapsed = start_time.elapsed();

        // The flush should succeed
        assert!(result.is_ok(), "Flush should succeed with empty payload");

        // The flush should complete quickly (within 100ms) since there's nothing to flush
        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "Flush with empty payload should complete quickly, took {:?}",
            elapsed
        );
    }

    #[test]
    fn test_flush_functionality_with_payload() {
        use crate::statistics::{StatisticsEvent, StatisticsSubscriber};

        let mut subscriber = HttpSubscriber::new();
        let logical_plan = create_test_logical_plan();

        // Submit a plan to create a non-empty payload
        let plan_event = StatisticsEvent::PlanSubmitted {
            plan_id: 1,
            query_id: "test-query".to_string(),
            logical_plan,
        };

        // Process the plan event to create a payload
        let result = subscriber.handle_event(&plan_event);
        assert!(result.is_ok(), "Plan submission should succeed");

        // Now test flush - this should wait for the HTTP request to be processed
        // Since we don't have a real HTTP server running, the request will fail
        // but the flush mechanism should still work correctly
        let start_time = std::time::Instant::now();
        let result = subscriber.flush();
        let elapsed = start_time.elapsed();

        // The flush should succeed even if HTTP request fails
        assert!(
            result.is_ok(),
            "Flush should succeed even with failed HTTP request"
        );

        // The flush should take some time since it waits for HTTP processing
        // But it shouldn't hang indefinitely
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "Flush should not hang indefinitely, took {:?}",
            elapsed
        );
    }

    #[test]
    fn test_flush_with_plan_finished_event() {
        use crate::statistics::{StatisticsEvent, StatisticsSubscriber};

        let mut subscriber = HttpSubscriber::new();
        let logical_plan = create_test_logical_plan();

        // Submit a plan
        let plan_event = StatisticsEvent::PlanSubmitted {
            plan_id: 1,
            query_id: "test-query".to_string(),
            logical_plan,
        };

        let result = subscriber.handle_event(&plan_event);
        assert!(result.is_ok(), "Plan submission should succeed");

        // Submit a task to create some activity
        let task_context = create_test_task_context(1, 1, 1, 1, Some(1));
        let task_event = StatisticsEvent::TaskSubmitted {
            context: task_context,
            name: "TestTask".to_string(),
        };

        let result = subscriber.handle_event(&task_event);
        assert!(result.is_ok(), "Task submission should succeed");

        // Now send a PlanFinished event - this should trigger a flush internally
        let plan_finished_event = StatisticsEvent::PlanFinished { plan_id: 1 };

        let start_time = std::time::Instant::now();
        let result = subscriber.handle_event(&plan_finished_event);
        let elapsed = start_time.elapsed();

        // The event handling should succeed (even if HTTP fails)
        assert!(
            result.is_ok(),
            "Plan finished event should be handled successfully"
        );

        // The flush should complete within a reasonable time
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "Plan finished event handling should not hang, took {:?}",
            elapsed
        );
    }
}
