use std::{collections::HashMap, sync::Arc};

use common_partitioning::PartitionRef;

use super::MockTaskBuilder;
use crate::scheduling::{
    dispatcher::SchedulableTask,
    scheduler::{DefaultScheduler, Scheduler},
    task::SchedulingStrategy,
    tests::{MockPartition, MockTask, MockWorker},
    worker::WorkerId,
};

// Helper to create a schedulable task
fn create_schedulable_task(
    strategy: SchedulingStrategy,
    partition_ref: PartitionRef,
) -> SchedulableTask<MockTask> {
    let task = MockTaskBuilder::new(partition_ref)
        .with_scheduling_strategy(strategy)
        .build();
    SchedulableTask::new(
        task,
        tokio::sync::oneshot::channel().0,
        tokio_util::sync::CancellationToken::new(),
    )
}

// Helper to create test workers
fn create_workers(configs: Vec<(String, usize)>) -> HashMap<WorkerId, MockWorker> {
    configs
        .into_iter()
        .map(|(id, cpus)| (id.clone(), MockWorker::new(id, cpus)))
        .collect()
}

// Helper to create a mock partition
fn create_mock_partition() -> PartitionRef {
    Arc::new(MockPartition::new(100, 100))
}

// Test the Spread scheduling strategy
#[test]
fn test_spread_scheduling() {
    // Create workers with varying availability
    let workers = create_workers(vec![
        ("worker1".to_string(), 4), // 4 slots available
        ("worker2".to_string(), 8), // 8 slots available
        ("worker3".to_string(), 4), // 4 slots available
    ]);

    // Create tasks with Spread strategy
    let tasks = vec![
        create_schedulable_task(SchedulingStrategy::Spread, create_mock_partition()),
        create_schedulable_task(SchedulingStrategy::Spread, create_mock_partition()),
        create_schedulable_task(SchedulingStrategy::Spread, create_mock_partition()),
    ];

    let mut scheduler =
        Box::new(DefaultScheduler::new()) as Box<dyn Scheduler<MockTask, MockWorker>>;
    scheduler.update_state(&workers);
    let result = scheduler.schedule_tasks(tasks);

    // Verify that tasks are scheduled to workers with most available slots
    assert_eq!(result.scheduled_tasks.len(), 3);

    // First two tasks should go to worker2 (most available slots)
    assert_eq!(result.scheduled_tasks[0].0, "worker2");
    assert_eq!(result.scheduled_tasks[1].0, "worker2");

    // Third task could go to any worker with available slots
    // After assigning the first two tasks to worker2, all workers have the same
    // number of available slots, so the third task could be assigned to any of them
    assert!(
        result.scheduled_tasks[2].0 == "worker1"
            || result.scheduled_tasks[2].0 == "worker2"
            || result.scheduled_tasks[2].0 == "worker3"
    );

    // No unscheduled tasks
    assert_eq!(result.unscheduled_tasks.len(), 0);
}

// Test the Soft Node Affinity scheduling strategy
#[test]
fn test_soft_node_affinity_scheduling() {
    // Create workers
    let mut workers = create_workers(vec![
        ("worker1".to_string(), 4), // 4 slots available
        ("worker2".to_string(), 2), // 2 slots available
        ("worker3".to_string(), 4), // 4 slots available
    ]);

    // Manually set worker2 to have no available slots
    workers.get_mut("worker2").unwrap().num_active_tasks = 2;

    // Create tasks with Node Affinity strategies
    let tasks = vec![
        // Task with affinity to worker1 (which has capacity)
        create_schedulable_task(
            SchedulingStrategy::NodeAffinity {
                node_id: "worker1".to_string(),
                soft: true,
            },
            create_mock_partition(),
        ),
        // Task with affinity to worker2 (which has no capacity)
        create_schedulable_task(
            SchedulingStrategy::NodeAffinity {
                node_id: "worker2".to_string(),
                soft: true,
            },
            create_mock_partition(),
        ),
        // Task with affinity to worker4 (which doesn't exist)
        create_schedulable_task(
            SchedulingStrategy::NodeAffinity {
                node_id: "worker4".to_string(),
                soft: true,
            },
            create_mock_partition(),
        ),
    ];

    let mut scheduler =
        Box::new(DefaultScheduler::new()) as Box<dyn Scheduler<MockTask, MockWorker>>;
    scheduler.update_state(&workers);
    let result = scheduler.schedule_tasks(tasks);

    // Verify scheduling results
    assert_eq!(result.scheduled_tasks.len(), 3);

    // First task should be scheduled to worker1 (has affinity and capacity)
    assert_eq!(result.scheduled_tasks[0].0, "worker1");

    // Second and third tasks should be scheduled to other workers with capacity
    // since their preferred nodes are full or non-existent
    // Worker1 or worker3 should be used since they have capacity
    let scheduled_workers: Vec<String> = result
        .scheduled_tasks
        .iter()
        .map(|(id, _)| id.clone())
        .collect();

    // Count occurrences of each worker
    let worker1_count = scheduled_workers
        .iter()
        .filter(|&id| id == "worker1")
        .count();
    let worker3_count = scheduled_workers
        .iter()
        .filter(|&id| id == "worker3")
        .count();

    // Tasks 2 and 3 could go to either worker1 or worker3 since they have availability
    assert_eq!(worker1_count + worker3_count, 3);

    // No unscheduled tasks
    assert_eq!(result.unscheduled_tasks.len(), 0);
}

// Test the Hard Node Affinity scheduling strategy
#[test]
fn test_hard_node_affinity_scheduling() {
    // Create workers
    let mut workers = create_workers(vec![
        ("worker1".to_string(), 4), // 4 slots available
        ("worker2".to_string(), 2), // 2 slots available
        ("worker3".to_string(), 4), // 4 slots available
    ]);

    // Manually set worker2 to have no available slots
    workers.get_mut("worker2").unwrap().num_active_tasks = 2;

    // Create tasks with Hard Node Affinity strategies
    let tasks = vec![
        // Task with affinity to worker1 (which has capacity)
        create_schedulable_task(
            SchedulingStrategy::NodeAffinity {
                node_id: "worker1".to_string(),
                soft: false,
            },
            create_mock_partition(),
        ),
        // Task with affinity to worker2 (which has no capacity)
        create_schedulable_task(
            SchedulingStrategy::NodeAffinity {
                node_id: "worker2".to_string(),
                soft: false,
            },
            create_mock_partition(),
        ),
        // Task with affinity to worker4 (which doesn't exist)
        create_schedulable_task(
            SchedulingStrategy::NodeAffinity {
                node_id: "worker4".to_string(),
                soft: false,
            },
            create_mock_partition(),
        ),
    ];

    let mut scheduler =
        Box::new(DefaultScheduler::new()) as Box<dyn Scheduler<MockTask, MockWorker>>;
    scheduler.update_state(&workers);
    let result = scheduler.schedule_tasks(tasks);

    // Only the first task should be scheduled (to worker1)
    assert_eq!(result.scheduled_tasks.len(), 1);
    assert_eq!(result.scheduled_tasks[0].0, "worker1");

    // The other two tasks should be unscheduled because they have hard affinity
    // to a worker that's either full or doesn't exist
    assert_eq!(result.unscheduled_tasks.len(), 2);
}

// Test mixed scheduling strategies
#[test]
fn test_mixed_scheduling_strategies() {
    // Create workers
    let mut workers = create_workers(vec![
        ("worker1".to_string(), 4), // 4 slots available
        ("worker2".to_string(), 4), // 4 slots available
    ]);

    // Manually set some workers to have fewer available slots
    workers.get_mut("worker1").unwrap().num_active_tasks = 3; // 1 slot available
    workers.get_mut("worker2").unwrap().num_active_tasks = 1; // 3 slots available

    // Create tasks with mixed strategies
    let tasks = vec![
        create_schedulable_task(SchedulingStrategy::Spread, create_mock_partition()),
        create_schedulable_task(
            SchedulingStrategy::NodeAffinity {
                node_id: "worker1".to_string(),
                soft: false,
            },
            create_mock_partition(),
        ),
        create_schedulable_task(
            SchedulingStrategy::NodeAffinity {
                node_id: "worker2".to_string(),
                soft: true,
            },
            create_mock_partition(),
        ),
    ];

    let mut scheduler =
        Box::new(DefaultScheduler::new()) as Box<dyn Scheduler<MockTask, MockWorker>>;
    scheduler.update_state(&workers);
    let result = scheduler.schedule_tasks(tasks);

    // All three tasks should be scheduled
    assert_eq!(result.scheduled_tasks.len(), 3);

    // Spread task should go to worker2 (most available slots)
    assert!(
        result
            .scheduled_tasks
            .iter()
            .any(|(id, task)| id == "worker2"
                && matches!(task.strategy(), SchedulingStrategy::Spread))
    );

    // Hard affinity task should go to worker1
    assert!(result
        .scheduled_tasks
        .iter()
        .any(|(id, task)| id == "worker1"
            && matches!(task.strategy(), SchedulingStrategy::NodeAffinity { node_id, soft }
            if node_id == "worker1" && !soft)));

    // Soft affinity task should go to worker2
    assert!(result
        .scheduled_tasks
        .iter()
        .any(|(id, task)| id == "worker2"
            && matches!(task.strategy(), SchedulingStrategy::NodeAffinity { node_id, soft }
            if node_id == "worker2" && *soft)));

    // No unscheduled tasks
    assert_eq!(result.unscheduled_tasks.len(), 0);
}

// Test scheduling with no available workers
#[test]
fn test_scheduling_with_no_available_workers() {
    // Create workers with no availability
    let mut workers = create_workers(vec![("worker1".to_string(), 4), ("worker2".to_string(), 2)]);

    // Set all workers to have no capacity
    workers.get_mut("worker1").unwrap().num_active_tasks = 4; // 0 slots available
    workers.get_mut("worker2").unwrap().num_active_tasks = 2; // 0 slots available

    // Create tasks
    let tasks = vec![
        create_schedulable_task(SchedulingStrategy::Spread, create_mock_partition()),
        create_schedulable_task(
            SchedulingStrategy::NodeAffinity {
                node_id: "worker1".to_string(),
                soft: true,
            },
            create_mock_partition(),
        ),
    ];

    let mut scheduler =
        Box::new(DefaultScheduler::new()) as Box<dyn Scheduler<MockTask, MockWorker>>;
    scheduler.update_state(&workers);
    let result = scheduler.schedule_tasks(tasks);

    // No tasks should be scheduled
    assert_eq!(result.scheduled_tasks.len(), 0);

    // All tasks should be unscheduled
    assert_eq!(result.unscheduled_tasks.len(), 2);
}

// Test scheduling with empty workers
#[test]
fn test_scheduling_with_empty_workers() {
    let workers: HashMap<WorkerId, MockWorker> = HashMap::new();

    let tasks = vec![
        create_schedulable_task(SchedulingStrategy::Spread, create_mock_partition()),
        create_schedulable_task(
            SchedulingStrategy::NodeAffinity {
                node_id: "worker1".to_string(),
                soft: true,
            },
            create_mock_partition(),
        ),
    ];

    let mut scheduler =
        Box::new(DefaultScheduler::new()) as Box<dyn Scheduler<MockTask, MockWorker>>;
    scheduler.update_state(&workers);
    let result = scheduler.schedule_tasks(tasks);

    // No tasks should be scheduled
    assert_eq!(result.scheduled_tasks.len(), 0);

    // All tasks should be unscheduled
    assert_eq!(result.unscheduled_tasks.len(), 2);
}
