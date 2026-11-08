//! Contract tests for bounded externally-driven streaming input.

use std::{sync::Arc, time::Duration};

use daft_core::prelude::*;
use daft_local_execution::{
    prepared_execution::{
        ExecutionState, FinishResult, NextResult, PreparedNativeExecution, PushBatchResult,
    },
    sources::streaming::{StreamingPushError, StreamingPushResult, StreamingSource},
    testing::PipelineMessage,
};
use daft_micropartition::MicroPartition;
use futures::StreamExt;
use tokio::sync::mpsc;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("value", DataType::Int32)]))
}

fn empty_batch(schema: &SchemaRef) -> Arc<MicroPartition> {
    Arc::new(MicroPartition::empty(Some(schema.clone())))
}

#[tokio::test]
async fn backpressure_returns_the_exact_batch_and_allows_retry() {
    let schema = test_schema();
    let (source, sender) = StreamingSource::new(schema.clone(), 1, 1);
    let mut output = source.into_stream();

    assert_eq!(
        sender.try_push_batch(0, empty_batch(&schema)),
        StreamingPushResult::Ok
    );

    let retry_batch = empty_batch(&schema);
    let returned_batch = match sender.try_push_batch_owned(0, 7, retry_batch.clone()) {
        Err(StreamingPushError::Again(batch)) => batch,
        other => panic!("full queue must return AGAIN, got {other:?}"),
    };
    assert!(Arc::ptr_eq(&retry_batch, &returned_batch));

    let first = output
        .next()
        .await
        .expect("first morsel")
        .expect("valid morsel");
    assert!(matches!(first, PipelineMessage::Morsel { .. }));
    sender
        .try_push_batch_owned(0, 7, returned_batch)
        .expect("the exact returned batch can be retried");

    let second = output
        .next()
        .await
        .expect("retried morsel")
        .expect("valid morsel");
    assert!(matches!(
        second,
        PipelineMessage::Morsel { input_id: 7, .. }
    ));
}

#[tokio::test]
async fn backpressure_finish_retries_then_succeeds_exactly_once() {
    let schema = test_schema();
    let (source, sender) = StreamingSource::new(schema.clone(), 1, 1);
    let mut output = source.into_stream();

    assert_eq!(
        sender.try_push_batch(0, empty_batch(&schema)),
        StreamingPushResult::Ok
    );
    assert_eq!(
        sender.try_finish_for_input(0, 11),
        StreamingPushResult::Again
    );

    let morsel = output.next().await.expect("morsel").expect("valid morsel");
    assert!(matches!(morsel, PipelineMessage::Morsel { .. }));

    assert_eq!(sender.try_finish_for_input(0, 11), StreamingPushResult::Ok);
    assert_eq!(sender.try_finish_for_input(0, 11), StreamingPushResult::End);

    let flush = output.next().await.expect("flush").expect("valid flush");
    assert!(matches!(flush, PipelineMessage::Flush(11)));
    assert!(
        output.next().await.is_none(),
        "Finish must terminate this source"
    );
}

#[tokio::test]
async fn backpressure_is_bounded_and_independent_per_source() {
    let schema = test_schema();
    let (_source, sender) = StreamingSource::new(schema.clone(), 2, 2);

    for source_id in 0..2 {
        assert_eq!(
            sender.try_push_batch(source_id, empty_batch(&schema)),
            StreamingPushResult::Ok
        );
        assert_eq!(
            sender.try_push_batch(source_id, empty_batch(&schema)),
            StreamingPushResult::Ok
        );
        assert_eq!(
            sender.try_push_batch(source_id, empty_batch(&schema)),
            StreamingPushResult::Again
        );
    }
}

#[tokio::test]
async fn backpressure_cancel_wakes_waiters_and_ends_producers() {
    let schema = test_schema();
    let (_source, sender) = StreamingSource::new(schema.clone(), 1, 1);

    let waiter = {
        let sender = sender.clone();
        tokio::spawn(async move { sender.wait_for_progress(Duration::from_secs(10)).await })
    };
    tokio::task::yield_now().await;
    sender.cancel();

    assert!(
        tokio::time::timeout(Duration::from_millis(250), waiter)
            .await
            .expect("cancel must wake the waiter")
            .expect("waiter task must not panic")
    );
    assert_eq!(
        sender.try_push_batch(0, empty_batch(&schema)),
        StreamingPushResult::End
    );
    assert_eq!(sender.try_finish_source(0), StreamingPushResult::End);
}

#[tokio::test]
async fn backpressure_finish_each_source_produces_one_flush() {
    let schema = test_schema();
    let (source, sender) = StreamingSource::new(schema, 2, 1);
    let mut output = source.into_stream();

    assert_eq!(sender.try_finish_for_input(0, 20), StreamingPushResult::Ok);
    assert_eq!(sender.try_finish_for_input(1, 21), StreamingPushResult::Ok);
    assert_eq!(sender.try_finish_for_input(0, 20), StreamingPushResult::End);
    assert_eq!(sender.try_finish_for_input(1, 21), StreamingPushResult::End);

    let mut flush_ids = Vec::new();
    while let Some(message) = output.next().await {
        match message.expect("valid pipeline message") {
            PipelineMessage::Flush(input_id) => flush_ids.push(input_id),
            other => panic!("unexpected message: {other:?}"),
        }
    }
    flush_ids.sort_unstable();
    assert_eq!(flush_ids, vec![20, 21]);
}

#[tokio::test]
async fn prepared_execution_push_and_next_are_non_blocking() {
    let schema = test_schema();
    let (_source, input_sender) = StreamingSource::new(schema.clone(), 1, 1);
    let (output_sender, output_receiver) = mpsc::channel(1);
    let execution = PreparedNativeExecution::new(input_sender, output_receiver);

    assert!(matches!(
        execution.try_push_batch(0, empty_batch(&schema)),
        PushBatchResult::Accepted
    ));
    let retry_batch = empty_batch(&schema);
    let returned = match execution.try_push_batch(0, retry_batch.clone()) {
        PushBatchResult::Again(batch) => batch,
        other => panic!("full input queue must return AGAIN, got {other:?}"),
    };
    assert!(Arc::ptr_eq(&retry_batch, &returned));
    assert!(matches!(execution.try_next(), NextResult::NotReady));

    output_sender
        .try_send(empty_batch(&schema))
        .expect("bounded output has capacity");
    assert!(matches!(execution.try_next(), NextResult::Ready(_)));
    drop(output_sender);
    assert!(matches!(execution.try_next(), NextResult::Done));
    assert_eq!(execution.state(), ExecutionState::Completed);
    assert_eq!(execution.stats(), (1, 1));
}

#[tokio::test]
async fn prepared_execution_wait_is_bounded() {
    let schema = test_schema();
    let (_source, input_sender) = StreamingSource::new(schema, 1, 1);
    let (_output_sender, output_receiver) = mpsc::channel(1);
    let execution = PreparedNativeExecution::new(input_sender, output_receiver);
    let timeout = Duration::from_millis(30);
    let started = std::time::Instant::now();

    assert!(matches!(
        execution.wait_next(timeout).await,
        NextResult::NotReady
    ));
    assert!(started.elapsed() >= timeout);
    assert!(started.elapsed() < Duration::from_millis(250));
}

#[tokio::test]
async fn prepared_execution_cancel_wakes_waiter_and_is_idempotent() {
    let schema = test_schema();
    let (_source, input_sender) = StreamingSource::new(schema.clone(), 1, 1);
    let (_output_sender, output_receiver) = mpsc::channel(1);
    let execution = Arc::new(PreparedNativeExecution::new(input_sender, output_receiver));

    let waiter = {
        let execution = execution.clone();
        tokio::spawn(async move { execution.wait_next(Duration::from_secs(10)).await })
    };
    tokio::task::yield_now().await;
    execution.cancel();
    execution.cancel();

    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(250), waiter)
            .await
            .expect("cancel must wake output waiter")
            .expect("waiter must not panic"),
        NextResult::Cancelled
    ));
    assert_eq!(execution.state(), ExecutionState::Cancelled);
    assert!(matches!(
        execution.try_push_batch(0, empty_batch(&schema)),
        PushBatchResult::End(_)
    ));
    assert_eq!(execution.try_finish_source(0), FinishResult::End);
}

#[tokio::test]
async fn prepared_execution_finish_again_is_retryable_and_not_global_completion() {
    let schema = test_schema();
    let (source, input_sender) = StreamingSource::new(schema.clone(), 1, 1);
    let mut input = source.into_stream();
    let (_output_sender, output_receiver) = mpsc::channel(1);
    let execution = PreparedNativeExecution::new(input_sender, output_receiver);

    assert!(matches!(
        execution.try_push_batch(0, empty_batch(&schema)),
        PushBatchResult::Accepted
    ));
    assert_eq!(execution.try_finish_source(0), FinishResult::Again);
    assert!(input.next().await.is_some());
    assert_eq!(execution.try_finish_source(0), FinishResult::Accepted);
    assert_eq!(execution.try_finish_source(0), FinishResult::End);
    assert_eq!(execution.state(), ExecutionState::Running);
}

#[tokio::test]
async fn lifecycle_multi_batch_order_and_single_flush_per_source() {
    let schema = test_schema();
    let (source, sender) = StreamingSource::new(schema.clone(), 2, 3);
    let mut output = source.into_stream();

    for (source_id, input_ids, finish_id) in [(0, [100, 101], 109), (1, [200, 201], 209)] {
        for input_id in input_ids {
            sender
                .try_push_batch_owned(source_id, input_id, empty_batch(&schema))
                .expect("each source has independent configured capacity");
        }
        assert_eq!(
            sender.try_finish_for_input(source_id, finish_id),
            StreamingPushResult::Ok
        );
        assert_eq!(
            sender.try_finish_for_input(source_id, finish_id),
            StreamingPushResult::End
        );
    }

    let messages = tokio::time::timeout(Duration::from_millis(250), async move {
        let mut by_source = [Vec::new(), Vec::new()];
        let mut flushes = [0, 0];
        while let Some(message) = output.next().await {
            match message.expect("valid pipeline message") {
                PipelineMessage::Morsel { input_id, .. } => {
                    let source = usize::from(input_id >= 200);
                    by_source[source].push(input_id);
                }
                PipelineMessage::Flush(input_id) => {
                    let source = usize::from(input_id >= 200);
                    flushes[source] += 1;
                    by_source[source].push(input_id);
                }
                other => panic!("streaming source emitted unexpected message: {other:?}"),
            }
        }
        (by_source, flushes)
    })
    .await
    .expect("all finished source streams must terminate");

    assert_eq!(messages.0[0], vec![100, 101, 109]);
    assert_eq!(messages.0[1], vec![200, 201, 209]);
    assert_eq!(messages.1, [1, 1]);
}

#[tokio::test]
async fn lifecycle_repeated_create_finish_drop_has_no_hang() {
    tokio::time::timeout(Duration::from_secs(1), async {
        for input_id in 0..32 {
            let schema = test_schema();
            let (source, sender) = StreamingSource::new(schema.clone(), 1, 2);
            let mut output = source.into_stream();
            sender
                .try_push_batch_owned(0, input_id, empty_batch(&schema))
                .expect("fresh execution accepts its batch");
            assert_eq!(
                sender.try_finish_for_input(0, input_id),
                StreamingPushResult::Ok
            );
            assert!(matches!(
                output.next().await.expect("morsel").expect("valid morsel"),
                PipelineMessage::Morsel { .. }
            ));
            assert!(matches!(
                output.next().await.expect("flush").expect("valid flush"),
                PipelineMessage::Flush(_)
            ));
            assert!(output.next().await.is_none());
        }
    })
    .await
    .expect("repeated source lifecycles must release all channels");
}
