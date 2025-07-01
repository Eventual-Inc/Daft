use std::{collections::HashMap, sync::Arc};

use common_treenode::TreeNode;
use daft_dsl::{lit, resolved_col};
use daft_logical_plan::{
    builder::LogicalPlanBuilder, ops::Source, source_info::PlaceHolderInfo, ClusteringSpec,
    LogicalPlan, LogicalPlanRef, SourceInfo,
};
use daft_schema::{dtype::DataType, field::Field, schema::Schema};

use crate::{
    scheduling::task::TaskContext,
    statistics::{
        http_subscriber::{HttpSubscriber, NodeStatus},
        PlanState, TaskExecutionStatus, TaskState,
    },
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
    .with_plan_id(1); // Node ID 1 for Source

    // Build a more complex plan using LogicalPlanBuilder
    let filter_plan = LogicalPlanBuilder::from(source_plan.arced())
        .filter(resolved_col("age").gt(lit(18)))
        .unwrap()
        .build()
        .with_plan_id(2); // Node ID 2 for Filter

    let project_plan = LogicalPlanBuilder::from(filter_plan.arced())
        .select(vec![resolved_col("name"), resolved_col("salary")])
        .unwrap()
        .build()
        .with_plan_id(3); // Node ID 3 for Project

    let limit_plan = LogicalPlanBuilder::from(project_plan.arced())
        .limit(100, false)
        .unwrap()
        .build()
        .with_plan_id(4); // Node ID 4 for Limit

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
    .with_plan_id(1); // Node ID 1 for Source

    // Build a filter plan using LogicalPlanBuilder
    let filter_plan = LogicalPlanBuilder::from(source_plan.arced())
        .filter(resolved_col("age").gt(lit(18)))
        .unwrap()
        .build()
        .with_plan_id(2); // Node ID 2 for Filter

    filter_plan.arced()
}

/// Helper function to create test plan state
fn create_test_plan_state(plan_id: u32, logical_plan: LogicalPlanRef) -> PlanState {
    PlanState {
        plan_id: plan_id as usize,
        query_id: uuid::Uuid::new_v4().to_string(),
        logical_plan,
    }
}

/// Helper function to create test task context
fn create_test_task_context(
    plan_id: u16,
    stage_id: u16,
    node_id: u32,
    task_id: u32,
) -> TaskContext {
    TaskContext {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_node_id() {
        let context = create_test_task_context(1, 2, 3, 4);
        let node_id = HttpSubscriber::generate_node_id(&context);

        // The node ID should be: (plan_id << 16) | (stage_id << 8) | node_id
        // (1 << 16) | (2 << 8) | 3 = 65536 + 512 + 3 = 66051
        assert_eq!(node_id, 66051);
    }

    #[test]
    fn test_build_query_graph_simple() {
        let subscriber = HttpSubscriber::new("http://localhost:8080".to_string());

        // Create a simple logical plan
        let logical_plan = create_test_logical_plan();

        // Create plan state
        let mut plans = HashMap::new();
        plans.insert(1, create_test_plan_state(1, logical_plan));

        // Create task states
        let mut tasks = HashMap::new();

        // Create tasks for different nodes in the plan
        let source_context = create_test_task_context(1, 1, 1, 1);
        let filter_context = create_test_task_context(1, 1, 2, 2);
        let project_context = create_test_task_context(1, 1, 3, 3);
        let limit_context = create_test_task_context(1, 1, 4, 4);

        tasks.insert(
            source_context,
            create_test_task_state("Source", TaskExecutionStatus::Completed),
        );
        tasks.insert(
            filter_context,
            create_test_task_state("Filter", TaskExecutionStatus::Running),
        );
        tasks.insert(
            project_context,
            create_test_task_state("Project", TaskExecutionStatus::Created),
        );
        tasks.insert(
            limit_context,
            create_test_task_state("Limit", TaskExecutionStatus::Created),
        );

        // Build the query graph
        let query_graph = subscriber.build_query_graph(&plans, &tasks);

        // Verify the basic structure
        assert_eq!(query_graph.version, "1.0.0");
        assert_eq!(query_graph.query_id, 1);
        assert_eq!(query_graph.nodes.len(), 4);

        // Check that nodes have expected IDs
        let node_ids: Vec<u32> = query_graph.nodes.iter().map(|n| n.id).collect();
        assert!(node_ids.contains(&HttpSubscriber::generate_node_id(&source_context)));
        assert!(node_ids.contains(&HttpSubscriber::generate_node_id(&filter_context)));
        assert!(node_ids.contains(&HttpSubscriber::generate_node_id(&project_context)));
        assert!(node_ids.contains(&HttpSubscriber::generate_node_id(&limit_context)));

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
        let subscriber = HttpSubscriber::new("http://localhost:8080".to_string());

        let logical_plan = create_test_logical_plan();
        let mut plans = HashMap::new();
        plans.insert(1, create_test_plan_state(1, logical_plan));

        // Create a linear chain of tasks: 1 -> 2 -> 3 -> 4
        let mut tasks = HashMap::new();
        let context1 = create_test_task_context(1, 1, 1, 1);
        let context2 = create_test_task_context(1, 1, 2, 2);
        let context3 = create_test_task_context(1, 1, 3, 3);
        let context4 = create_test_task_context(1, 1, 4, 4);

        tasks.insert(
            context1,
            create_test_task_state("Source", TaskExecutionStatus::Completed),
        );
        tasks.insert(
            context2,
            create_test_task_state("Filter", TaskExecutionStatus::Running),
        );
        tasks.insert(
            context3,
            create_test_task_state("Project", TaskExecutionStatus::Created),
        );
        tasks.insert(
            context4,
            create_test_task_state("Limit", TaskExecutionStatus::Created),
        );

        let query_graph = subscriber.build_query_graph(&plans, &tasks);

        // Generate expected node IDs
        let expected_node_ids = vec![
            HttpSubscriber::generate_node_id(&context1),
            HttpSubscriber::generate_node_id(&context2),
            HttpSubscriber::generate_node_id(&context3),
            HttpSubscriber::generate_node_id(&context4),
        ];

        // Check that we have the right number of nodes
        assert_eq!(query_graph.nodes.len(), 4);

        // Check that all expected nodes are present in the graph
        let actual_node_ids: Vec<u32> = query_graph.nodes.iter().map(|n| n.id).collect();
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
        let subscriber = HttpSubscriber::new("http://localhost:8080".to_string());

        let logical_plan = create_simple_test_logical_plan();

        let mut plans = HashMap::new();
        plans.insert(1, create_test_plan_state(1, logical_plan));

        let mut tasks = HashMap::new();

        // Create multiple tasks for the same node (node_id = 1) with different task_ids
        // These should be aggregated into a single graph node
        let context1 = create_test_task_context(1, 1, 1, 1); // Task 1 for node 1
        let context2 = create_test_task_context(1, 1, 1, 2); // Task 2 for node 1
        let context3 = create_test_task_context(1, 1, 1, 3); // Task 3 for node 1

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

        tasks.insert(context1, task1);
        tasks.insert(context2, task2);
        tasks.insert(context3, task3);

        // Add a task for a different node to ensure we don't aggregate across different nodes
        let context4 = create_test_task_context(1, 1, 2, 4); // Task 4 for node 2
        let mut task4 = create_test_task_state("Filter", TaskExecutionStatus::Running);
        task4.pending = 1;
        task4.completed = 2;
        task4.failed = 0;
        task4.canceled = 0;
        task4.total = 3;

        tasks.insert(context4, task4);

        let query_graph = subscriber.build_query_graph(&plans, &tasks);

        // Verify basic structure
        assert_eq!(query_graph.version, "1.0.0");
        assert_eq!(query_graph.query_id, 1);
        assert_eq!(query_graph.nodes.len(), 2); // Should have 2 nodes (node 1 and node 2)

        // Find the aggregated node for node_id = 1
        let source_node_id = HttpSubscriber::generate_node_id(&context1); // All contexts with same node_id generate same ID
        let source_node = query_graph
            .nodes
            .iter()
            .find(|node| node.id == source_node_id)
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
        assert_eq!(source_node.status, NodeStatus::Running);

        // Find the node for node_id = 2
        let filter_node_id = HttpSubscriber::generate_node_id(&context4);
        let filter_node = query_graph
            .nodes
            .iter()
            .find(|node| node.id == filter_node_id)
            .expect("Filter node should exist");

        // Verify metrics for node 2 (single task)
        assert_eq!(filter_node.pending, 1);
        assert_eq!(filter_node.completed, 2);
        assert_eq!(filter_node.failed, 0);
        assert_eq!(filter_node.canceled, 0);
        assert_eq!(filter_node.total, 3);
        assert_eq!(filter_node.status, NodeStatus::Running);

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
            query_graph.adjacency_list.get(&(filter_node_id as usize)),
            Some(&vec![source_node_id as usize])
        );
    }

    /// Pretty print the query graph structure
    #[allow(dead_code)]
    fn pretty_print_query_graph(query_graph: &crate::statistics::http_subscriber::QueryGraph) {
        println!("═══ Query Graph Debug Info ═══");
        println!("Version: {}", query_graph.version);
        println!("Query ID: {}", query_graph.query_id);
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
}
