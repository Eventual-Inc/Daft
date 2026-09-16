use daft_core::prelude::{DataType, Field, Schema};
use daft_memory::MemoryManager;

use super::*;
use crate::spilling::{SpillManager, TARGET_FILE_BYTES as TARGET_SPILL_FILE_BYTES};

fn make_run(spawner: &ExecutionTaskSpawner, values: Vec<i64>, payload: usize) -> MemoryRun {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64),
        Field::new("data", DataType::Utf8),
    ]));
    let strings =
        arrow_array::LargeStringArray::from_iter_values(values.iter().map(|_| "x".repeat(payload)));
    let batch = RecordBatch::from_arrow(
        schema.clone(),
        vec![
            Arc::new(arrow_array::Int64Array::from(values)),
            Arc::new(strings),
        ],
    )
    .unwrap();
    let partition = MicroPartition::new_loaded(schema, Arc::new(vec![batch]), None);
    let memory = spawner
        .try_reserve_memory(partition.size_bytes() as u64)
        .unwrap()
        .unwrap();
    MemoryRun {
        partition,
        memory,
        max_row_working_bytes: OnceLock::new(),
    }
}

fn spawner(limit: u64) -> (MemoryManager, ExecutionTaskSpawner, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!("daft-sort-test-{}", uuid::Uuid::new_v4()));
    let manager = MemoryManager::new(limit);
    let spawner = ExecutionTaskSpawner::new(
        common_runtime::get_compute_runtime(),
        manager.worker_pool(),
        SpillManager::with_io_concurrency([root.clone()], 1, "test").unwrap(),
        Span::none(),
    );
    (manager, spawner, root)
}

fn params(schema: &SchemaRef) -> Arc<SortParams> {
    Arc::new(SortParams {
        sort_by: vec![BoundExpr::try_new(daft_dsl::unresolved_col("key"), schema).unwrap()],
        descending: vec![false],
        nulls_first: vec![true],
    })
}

fn assert_no_spill_files(path: &std::path::Path) {
    if !path.exists() {
        return;
    }
    for entry in std::fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        assert!(path.is_dir(), "spill file was not cleaned up: {path:?}");
        assert_no_spill_files(&path);
    }
}

#[tokio::test]
async fn empty_maps_form_bounded_runs_and_sort_under_low_memory() {
    use arrow_array::builder::{Int64Builder, LargeStringBuilder, MapBuilder};

    for (rows, limit) in [(32_000, 2 * 1024 * 1024), (100_000, 1024 * 1024)] {
        let (manager, spawner, root) = spawner(limit);
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64),
            Field::new(
                "payload",
                DataType::Map {
                    key: Box::new(DataType::Utf8),
                    value: Box::new(DataType::Int64),
                },
            ),
        ]));
        let mut maps = MapBuilder::new(None, LargeStringBuilder::new(), Int64Builder::new());
        for _ in 0..rows {
            maps.append(true).unwrap();
        }
        let batch = RecordBatch::from_arrow(
            schema.clone(),
            vec![
                Arc::new(arrow_array::Int64Array::from_iter_values((0..rows).rev())),
                Arc::new(maps.finish()),
            ],
        )
        .unwrap();
        let input = MicroPartition::new_loaded(schema.clone(), Arc::new(vec![batch]), None);
        let sink = SortSink {
            params: params(&schema),
        };
        let scope = spawner.spill_manager.scope(0, 0);
        let mut state = sink.make_state(0).unwrap();
        run_builder::sink_input(input, &mut state, &sink.params, &scope, &spawner)
            .await
            .unwrap();
        state.flush_pending(&sink.params).unwrap();
        let (runs, spills, _) = state.building_mut();
        assert!(
            runs.len() + spills.len() < 128,
            "empty maps must not create one run per row"
        );
        // Explicitly exercise write/restore even if every sorted run fits in memory.
        let (states, _) = sink
            .release_memory(vec![state], u64::MAX, scope.clone(), &spawner)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(manager.used_bytes(), 0);
        let files = match &states[0] {
            SortState::Building { spill_runs, .. } => {
                spill_runs.iter().map(|run| run.files.len()).sum::<usize>()
            }
            SortState::Done => unreachable!(),
        };
        assert!(files > 0 && files < 128);
        let output = sink
            .finalize(states, scope, &spawner)
            .await
            .unwrap()
            .unwrap();
        let BlockingSinkOutput::Partitions(mut stream) = output else {
            panic!("expected partitions")
        };
        let mut expected = 0;
        while let Some(partition) = stream.next().await {
            for batch in partition.unwrap().record_batches() {
                for row in 0..batch.len() {
                    assert_eq!(batch.get_column(0).i64().unwrap().get(row), Some(expected));
                    expected += 1;
                }
                assert_eq!(
                    batch.get_column(1).map().unwrap().physical.flat_child.len(),
                    0
                );
            }
        }
        assert_eq!(expected, rows);
        drop(stream);
        assert_eq!(manager.used_bytes(), 0);
        assert_no_spill_files(&root);
        drop(spawner);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn logical_secondary_keys_merge_in_memory_and_after_spilling() {
    for dtype in [
        DataType::Uuid,
        DataType::Embedding(Box::new(DataType::Float64), 2),
    ] {
        for spill in [false, true] {
            let (manager, spawner, root) = spawner(8 * 1024 * 1024);
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64),
                Field::new("value", dtype.clone()),
                Field::new("id", DataType::Int64),
            ]));
            let params = Arc::new(SortParams {
                sort_by: ["key", "value"]
                    .into_iter()
                    .map(|name| {
                        BoundExpr::try_new(daft_dsl::unresolved_col(name), &schema).unwrap()
                    })
                    .collect(),
                descending: vec![false; 2],
                nulls_first: vec![true; 2],
            });
            let mut runs = Vec::new();
            for ids in [[0_i64, 2], [1, 3]] {
                let values: arrow_array::ArrayRef = match &dtype {
                    DataType::Uuid => Arc::new(
                        arrow_array::FixedSizeBinaryArray::try_from_iter(
                            ids.iter().map(|id| (*id as u128).to_be_bytes()),
                        )
                        .unwrap(),
                    ),
                    DataType::Embedding(_, _) => Arc::new(arrow_array::FixedSizeListArray::new(
                        Arc::new(arrow_schema::Field::new(
                            "item",
                            arrow_schema::DataType::Float64,
                            true,
                        )),
                        2,
                        Arc::new(arrow_array::Float64Array::from_iter_values(
                            ids.iter().flat_map(|id| [*id as f64, 0.0]),
                        )),
                        None,
                    )),
                    _ => unreachable!(),
                };
                let batch = RecordBatch::from_arrow(
                    schema.clone(),
                    vec![
                        Arc::new(arrow_array::Int64Array::from(vec![0, 0])),
                        values,
                        Arc::new(arrow_array::Int64Array::from(ids.to_vec())),
                    ],
                )
                .unwrap();
                let partition =
                    MicroPartition::new_loaded(schema.clone(), Arc::new(vec![batch]), None);
                let memory = spawner
                    .try_reserve_memory(partition_bytes(&partition).unwrap())
                    .unwrap()
                    .unwrap();
                let run = MemoryRun {
                    partition,
                    memory,
                    max_row_working_bytes: OnceLock::new(),
                };
                runs.push(if spill {
                    MergeRun::Spill(
                        write_memory_run(&run, &spawner.spill_manager.scope(0, 0), &spawner)
                            .await
                            .unwrap(),
                    )
                } else {
                    MergeRun::Memory(run)
                });
            }
            let mut stream = merge_files(runs, schema.clone(), params, spawner.clone())
                .await
                .unwrap();
            let mut ids = Vec::new();
            while let Some(partition) = stream.next().await {
                let partition = partition.unwrap();
                assert_eq!(partition.schema(), schema);
                for batch in partition.record_batches() {
                    ids.extend(
                        batch
                            .get_column(2)
                            .i64()
                            .unwrap()
                            .into_iter()
                            .map(Option::unwrap),
                    );
                }
            }
            assert_eq!(ids, vec![0, 1, 2, 3]);
            drop(stream);
            assert_eq!(manager.used_bytes(), 0);
            assert_no_spill_files(&root);
            drop(spawner);
            if root.exists() {
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    }
}

fn boolean_list_batch(rows: usize, width: i32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64),
        Field::new(
            "data",
            DataType::FixedSizeList(Box::new(DataType::Boolean), width as usize),
        ),
    ]));
    RecordBatch::from_arrow(
        schema,
        vec![
            Arc::new(arrow_array::Int64Array::from_iter_values(
                (0..rows as i64).rev(),
            )),
            Arc::new(arrow_array::FixedSizeListArray::new(
                Arc::new(arrow_schema::Field::new(
                    "item",
                    arrow_schema::DataType::Boolean,
                    false,
                )),
                width,
                Arc::new(arrow_array::BooleanArray::from(vec![
                    false;
                    rows * width as usize
                ])),
                None,
            )),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn nested_take_workspace_limits_runs_and_survives_spill_restore() {
    let (manager, spawner, root) = spawner(8 * 1024 * 1024);
    let batch = boolean_list_batch(120, 8192);
    let input = MicroPartition::new_loaded(batch.schema.clone(), Arc::new(vec![batch]), None);
    let sink = SortSink {
        params: params(&input.schema()),
    };
    let scope = spawner.spill_manager.scope(0, 0);
    let mut state = sink.make_state(0).unwrap();
    run_builder::sink_input(input, &mut state, &sink.params, &scope, &spawner)
        .await
        .unwrap();
    state.flush_pending(&sink.params).unwrap();
    let runs = state.building_mut().0;
    assert!(
        runs.len() > 1,
        "small payloads can still need large take workspace"
    );
    for run in runs {
        let batch = &run.partition.record_batches()[0];
        let columns = batch
            .as_materialized_series()
            .into_iter()
            .map(SeriesSize::new)
            .collect::<DaftResult<Vec<_>>>()
            .unwrap();
        assert!(
            run_builder::selection_working_bytes(&columns, 0, batch.len()).unwrap()
                <= RunBuilder::target_bytes(spawner.memory_limit_bytes())
        );
        assert!(run.max_row_working_bytes().unwrap() >= 8192 * 8);
    }
    let (states, _) = sink
        .release_memory(vec![state], u64::MAX, scope.clone(), &spawner)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manager.used_bytes(), 0);
    let output = sink
        .finalize(states, scope, &spawner)
        .await
        .unwrap()
        .unwrap();
    let BlockingSinkOutput::Partitions(mut stream) = output else {
        panic!("expected partitions")
    };
    let mut expected = 0;
    while let Some(partition) = stream.next().await {
        for batch in partition.unwrap().record_batches() {
            for row in 0..batch.len() {
                assert_eq!(batch.get_column(0).i64().unwrap().get(row), Some(expected));
                expected += 1;
            }
            assert_eq!(
                batch
                    .get_column(1)
                    .fixed_size_list()
                    .unwrap()
                    .flat_child
                    .len(),
                batch.len() * 8192
            );
        }
    }
    assert_eq!(expected, 120);
    drop(stream);
    assert_eq!(manager.used_bytes(), 0);
    assert_no_spill_files(&root);
    drop(spawner);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn single_run_nested_take_uses_the_admitted_workspace() {
    let (manager, spawner, root) = spawner(2 * 1024 * 1024);
    let batch = boolean_list_batch(128, 8192);
    let schema = batch.schema.clone();
    let params = params(&schema);
    let input = MicroPartition::new_loaded(schema.clone(), Arc::new(vec![batch]), None);
    let partition = input
        .sort(&params.sort_by, &params.descending, &params.nulls_first)
        .unwrap();
    let memory = spawner
        .try_reserve_memory(partition_bytes(&partition).unwrap())
        .unwrap()
        .unwrap();
    let run = MergeRun::Memory(MemoryRun {
        partition,
        memory,
        max_row_working_bytes: OnceLock::new(),
    });
    let runs = vec![run];
    let admission = try_admit_merge(
        &runs,
        params.comparator_pair_bytes(&schema).unwrap(),
        &spawner,
    )
    .unwrap()
    .unwrap();
    let working_bytes = admission.workspace_bytes;
    let competitor = manager
        .reserve(manager.total_bytes() - manager.used_bytes())
        .await
        .unwrap();
    let mut stream = merge_admitted(runs, schema, params, spawner, admission)
        .await
        .unwrap();
    let mut expected = 0;
    let mut outputs = 0;
    while let Some(partition) = stream.next().await {
        for batch in partition.unwrap().record_batches() {
            let columns = batch
                .as_materialized_series()
                .into_iter()
                .map(SeriesSize::new)
                .collect::<DaftResult<Vec<_>>>()
                .unwrap();
            assert!(
                run_builder::selection_working_bytes(&columns, 0, batch.len()).unwrap()
                    <= working_bytes
            );
            for row in 0..batch.len() {
                assert_eq!(batch.get_column(0).i64().unwrap().get(row), Some(expected));
                expected += 1;
            }
            outputs += 1;
        }
    }
    assert!(
        outputs > 1,
        "payload alone fits, but the nested indices do not"
    );
    assert_eq!(expected, 128);
    drop(stream);
    drop(competitor);
    assert_eq!(manager.used_bytes(), 0);
    assert!(!root.exists());
}

#[tokio::test]
async fn sort_key_count_limits_merge_fan_in_before_allocating_comparators() {
    let (manager, spawner, root) = spawner(8 * 1024 * 1024);
    let schema = Arc::new(Schema::new(
        (0..128).map(|i| Field::new(format!("c{i}"), DataType::Int64)),
    ));
    let params = Arc::new(SortParams {
        sort_by: (0..128)
            .map(|i| {
                BoundExpr::try_new(daft_dsl::unresolved_col(format!("c{i}")), &schema).unwrap()
            })
            .collect(),
        descending: vec![false; 128],
        nulls_first: vec![true; 128],
    });
    let mut runs = (0..64)
        .map(|i| {
            let arrays = (0..128)
                .map(|_| {
                    Arc::new(arrow_array::Int64Array::from(vec![i, i + 64]))
                        as arrow_array::ArrayRef
                })
                .collect();
            let batch = RecordBatch::from_arrow(schema.clone(), arrays).unwrap();
            let partition = MicroPartition::new_loaded(schema.clone(), Arc::new(vec![batch]), None);
            let memory = spawner
                .try_reserve_memory(partition_bytes(&partition).unwrap())
                .unwrap()
                .unwrap();
            MergeRun::Memory(MemoryRun {
                partition,
                memory,
                max_row_working_bytes: OnceLock::new(),
            })
        })
        .collect::<Vec<_>>();
    let single_key = SortParams {
        sort_by: params.sort_by[..1].to_vec(),
        descending: vec![false],
        nulls_first: vec![true],
    };
    let narrow = try_admit_merge(
        &runs,
        single_key.comparator_pair_bytes(&schema).unwrap(),
        &spawner,
    )
    .unwrap()
    .unwrap();
    let narrow_count = narrow.run_count;
    drop(narrow);
    let admission = try_admit_merge(
        &runs,
        params.comparator_pair_bytes(&schema).unwrap(),
        &spawner,
    )
    .unwrap()
    .unwrap();
    assert!(admission.run_count < narrow_count);
    assert!(
        admission.comparator_bytes >= (admission.run_count * admission.run_count * 128 * 16) as u64
    );
    let count = admission.run_count;
    drop(runs.split_off(count));
    let competitor = manager
        .reserve(manager.total_bytes() - manager.used_bytes())
        .await
        .unwrap();
    let mut stream = merge_admitted(runs, schema, params, spawner, admission)
        .await
        .unwrap();
    let mut keys = Vec::new();
    while let Some(partition) = stream.next().await {
        for batch in partition.unwrap().record_batches() {
            keys.extend(
                batch
                    .get_column(0)
                    .i64()
                    .unwrap()
                    .into_iter()
                    .map(Option::unwrap),
            );
        }
    }
    assert_eq!(
        keys,
        (0..count as i64)
            .chain(64..64 + count as i64)
            .collect::<Vec<_>>()
    );
    drop(stream);
    drop(competitor);
    assert_eq!(manager.used_bytes(), 0);
    assert!(!root.exists());
}

#[test]
fn comparator_budget_includes_nested_children() {
    let primitive = arrow_schema::DataType::Int64;
    let fields = (0..128)
        .map(|i| arrow_schema::Field::new(format!("f{i}"), primitive.clone(), true))
        .collect::<Vec<_>>();
    let nested = arrow_schema::DataType::Struct(fields.into());
    assert!(comparator_type_bytes(&nested) > 128 * comparator_type_bytes(&primitive));
    assert_eq!(
        comparator_matrix_bytes(1, comparator_type_bytes(&nested)),
        0
    );
}

#[tokio::test]
async fn small_morsels_form_one_spill_run_and_restore_in_order() {
    let (manager, spawner, root) = spawner(32 * 1024 * 1024);
    let input = make_run(&spawner, vec![0], 16).partition;
    let sink = SortSink {
        params: params(&input.schema()),
    };
    let mut state = sink.make_state(0).unwrap();
    let scope = spawner.spill_manager.scope(0, 0);
    for chunk in (0..100).rev() {
        let input =
            make_run(&spawner, (chunk * 32..(chunk + 1) * 32).rev().collect(), 16).partition;
        run_builder::sink_input(input, &mut state, &sink.params, &scope, &spawner)
            .await
            .unwrap();
    }
    assert!(state.pending().bytes() > 0);
    assert!(
        state.building_mut().0.is_empty(),
        "input boundaries must not create runs"
    );
    let before = state.reclaim_bytes();
    assert_eq!(before, manager.used_bytes());
    let (mut states, released) = sink
        .release_memory(vec![state], u64::MAX, scope.clone(), &spawner)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(released, before);
    assert_eq!(manager.used_bytes(), 0);
    assert_eq!(states[0].building_mut().1.len(), 1);
    assert_eq!(
        states[0].building_mut().1[0].files.len(),
        1,
        "100 small inputs must not create 100 files"
    );
    let output = sink
        .finalize(states, scope, &spawner)
        .await
        .unwrap()
        .unwrap();
    let BlockingSinkOutput::Partitions(mut stream) = output else {
        panic!("expected partitions")
    };
    let mut expected = 0;
    while let Some(partition) = stream.next().await {
        for batch in partition.unwrap().record_batches() {
            for row in 0..batch.len() {
                assert_eq!(batch.get_column(0).i64().unwrap().get(row), Some(expected));
                expected += 1;
            }
        }
    }
    assert_eq!(expected, 3200);
    drop(stream);
    assert_eq!(manager.used_bytes(), 0);
    assert_no_spill_files(&root);
    drop(spawner);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn buffered_input_can_release_prepaid_workspace_with_no_free_memory() {
    let (manager, spawner, root) = spawner(8 * 1024 * 1024);
    let input = make_run(&spawner, (0..100).rev().collect(), 16).partition;
    let sink = SortSink {
        params: params(&input.schema()),
    };
    let scope = spawner.spill_manager.scope(0, 0);
    let mut state = sink.make_state(0).unwrap();
    run_builder::sink_input(input, &mut state, &sink.params, &scope, &spawner)
        .await
        .unwrap();
    let before = state.reclaim_bytes();
    let competitor = manager
        .worker_pool()
        .try_reserve(manager.total_bytes() - before)
        .unwrap()
        .unwrap();
    let (mut states, released) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        sink.release_memory(vec![state], 1, scope, &spawner),
    )
    .await
    .expect("must reclaim buffered input without additional admission")
    .unwrap()
    .unwrap();
    assert!(released > 0);
    assert_eq!(released, before - states[0].reclaim_bytes());
    assert_eq!(states[0].building_mut().0.len(), 1);
    assert!(
        states[0].building_mut().1.is_empty(),
        "returning workspace is sufficient here"
    );
    assert!(!root.exists());
    drop(competitor);
    drop(states);
    assert_eq!(manager.used_bytes(), 0);
}

#[tokio::test]
async fn finalize_sorts_the_unfilled_tail_and_keeps_empty_input_schema() {
    let (manager, spawner, _root) = spawner(8 * 1024 * 1024);
    let input = make_run(&spawner, vec![3, 1, 2], 16).partition;
    let schema = input.schema();
    let sink = SortSink {
        params: params(&schema),
    };
    let scope = spawner.spill_manager.scope(0, 0);
    let mut state = sink.make_state(0).unwrap();
    run_builder::sink_input(input, &mut state, &sink.params, &scope, &spawner)
        .await
        .unwrap();
    let (runs, spills, _) = state.finish(&sink.params).unwrap();
    assert_eq!(runs.len(), 1);
    assert!(spills.is_empty());
    for (row, expected) in [1, 2, 3].into_iter().enumerate() {
        assert_eq!(
            runs[0].partition.record_batches()[0]
                .get_column(0)
                .i64()
                .unwrap()
                .get(row),
            Some(expected)
        );
    }
    drop(runs);
    let mut state = sink.make_state(0).unwrap();
    run_builder::sink_input(
        MicroPartition::empty(Some(schema.clone())),
        &mut state,
        &sink.params,
        &scope,
        &spawner,
    )
    .await
    .unwrap();
    let (runs, spills, result_schema) = state.finish(&sink.params).unwrap();
    assert!(runs.is_empty() && spills.is_empty());
    assert_eq!(result_schema, Some(schema));
    assert_eq!(manager.used_bytes(), 0);
}

#[tokio::test]
async fn buffered_slice_detaches_upstream_buffers_and_cancellation_releases_memory() {
    use arrow_array::Array;

    let (manager, spawner, root) = spawner(8 * 1024 * 1024);
    let schema = make_run(&spawner, vec![0], 0).partition.schema();
    let payload =
        arrow_array::LargeStringArray::from(vec!["x".repeat(4 * 1024 * 1024), "abc".to_string()]);
    let backing = payload.to_data().buffers()[1].clone();
    let batch = RecordBatch::from_arrow(
        schema.clone(),
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![0, 1])),
            Arc::new(payload),
        ],
    )
    .unwrap();
    let input = MicroPartition::new_loaded(
        schema.clone(),
        Arc::new(vec![batch.slice(1, 2).unwrap()]),
        None,
    );
    drop(batch);
    assert!(backing.strong_count() > 1);
    let sink = SortSink {
        params: params(&schema),
    };
    let mut state = sink.make_state(0).unwrap();
    run_builder::sink_input(
        input,
        &mut state,
        &sink.params,
        &spawner.spill_manager.scope(0, 0),
        &spawner,
    )
    .await
    .unwrap();
    assert_eq!(
        backing.strong_count(),
        1,
        "pending input must not retain the original 4 MiB backing buffer"
    );
    assert!(manager.used_bytes() > 0);
    drop(state);
    assert_eq!(manager.used_bytes(), 0);
    assert!(!root.exists());
}

#[tokio::test]
async fn mixed_morsel_sizes_and_skewed_rows_keep_every_row() {
    let (manager, spawner, root) = spawner(8 * 1024 * 1024);
    let schema = make_run(&spawner, vec![0], 0).partition.schema();
    let sink = SortSink {
        params: params(&schema),
    };
    let scope = spawner.spill_manager.scope(0, 0);
    let mut state = sink.make_state(0).unwrap();
    let mut count = 0_i64;
    for (rows, payload) in [
        (31, 8),
        (2000, 1024),
        (1, 2 * 1024 * 1024),
        (37, 0),
        (5000, 32),
    ] {
        let input = make_run(&spawner, (count..count + rows).rev().collect(), payload).partition;
        run_builder::sink_input(input, &mut state, &sink.params, &scope, &spawner)
            .await
            .unwrap();
        count += rows;
    }
    let (runs, spills, _) = state.finish(&sink.params).unwrap();
    let mut rows = runs.iter().map(|run| run.partition.len()).sum::<usize>();
    for spill in &spills {
        for file in &spill.files {
            // This validation is independent of the operator's retained reservations.
            let reader_memory = MemoryManager::new(64 * 1024 * 1024);
            let mut reader = spawner
                .spill_manager
                .open_micropartitions(file, |bytes| reader_memory.reserve(bytes))
                .await
                .unwrap();
            while let Some((batch, permit, next)) = reader
                .next_batch(|bytes| reader_memory.reserve(bytes))
                .await
                .unwrap()
            {
                rows += batch.len();
                drop(batch);
                drop(permit);
                reader = next;
            }
        }
    }
    assert_eq!(rows, count as usize);
    for run in &runs {
        let batch = &run.partition.record_batches()[0];
        let keys = batch.get_column(0).i64().unwrap();
        for row in 1..batch.len() {
            assert!(keys.get(row - 1) <= keys.get(row));
        }
    }
    drop(runs);
    drop(spills);
    assert_eq!(manager.used_bytes(), 0);
    assert_no_spill_files(&root);
    drop(spawner);
    if root.exists() {
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn final_output_reclaims_while_downstream_is_not_polling() {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let (manager, spawner, root) = spawner(32 * 1024 * 1024);
        // Larger than one batch even with column-wise, rather than per-row sizing.
        let run = make_run(&spawner, (0..100_000).collect(), 128);
        let schema = run.partition.schema();
        let sink = SortSink {
            params: params(&schema),
        };
        let output = sink
            .finalize(
                vec![SortState::Building {
                    pending: RunBuilder::default(),
                    memory_runs: vec![run],
                    spill_runs: vec![],
                    schema: Some(schema),
                }],
                spawner.spill_manager.scope(0, 0),
                &spawner,
            )
            .await
            .unwrap()
            .unwrap();
        let BlockingSinkOutput::Partitions(mut stream) = output else {
            panic!("expected partitions")
        };
        let first = stream.next().await.unwrap().unwrap();
        let mut count = first.len() as i64;
        assert!(count < 100_000);
        drop(first);

        // Simulate a downstream UDF waiting for all worker capacity. Do not poll
        // Sort until admission completes; this used to form a circular wait.
        let downstream = spawner.reserve_memory(32 * 1024 * 1024).await.unwrap();
        assert_eq!(manager.used_bytes(), downstream.bytes());
        assert!(root.exists());
        drop(downstream);
        while let Some(partition) = stream.next().await {
            for batch in partition.unwrap().record_batches() {
                for row in 0..batch.len() {
                    assert_eq!(batch.get_column(0).i64().unwrap().get(row), Some(count));
                    count += 1;
                }
            }
            // Restoring an output must not leave reader reservations behind either.
            let downstream = spawner.reserve_memory(32 * 1024 * 1024).await.unwrap();
            drop(downstream);
        }
        assert_eq!(count, 100_000);
        drop(stream);
        assert_eq!(manager.used_bytes(), 0);
        assert_no_spill_files(&root);
        drop(spawner);
        std::fs::remove_dir_all(root).unwrap();
    })
    .await
    .expect("Sort and downstream admission must make progress");
}

#[tokio::test]
async fn sixty_one_memory_runs_merge_without_spill() {
    let (manager, spawner, root) = spawner(256 * 1024 * 1024);
    let runs: Vec<_> = (0..61)
        .map(|i| make_run(&spawner, (0..100).map(|j| j * 61 + i).collect(), 16))
        .collect();
    let schema = runs[0].partition.schema();
    let sink = SortSink {
        params: params(&schema),
    };
    let state = SortState::Building {
        pending: RunBuilder::default(),
        memory_runs: runs,
        spill_runs: vec![],
        schema: Some(schema),
    };
    let output = sink
        .finalize(vec![state], spawner.spill_manager.scope(0, 0), &spawner)
        .await
        .unwrap();
    let BlockingSinkOutput::Partitions(mut stream) = output.unwrap() else {
        panic!("expected partitions")
    };
    let mut count = 0;
    while let Some(partition) = stream.next().await {
        for batch in partition.unwrap().record_batches() {
            for row in 0..batch.len() {
                assert_eq!(batch.get_column(0).i64().unwrap().get(row), Some(count));
                count += 1;
            }
        }
    }
    assert_eq!(count, 6100);
    drop(stream);
    assert!(!root.exists());
    assert_eq!(manager.used_bytes(), 0);
}

#[tokio::test]
async fn cancelled_final_output_releases_memory_and_spill_files() {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let (manager, spawner, root) = spawner(32 * 1024 * 1024);
        let run = make_run(&spawner, (0..100_000).collect(), 128);
        let schema = run.partition.schema();
        let sink = SortSink {
            params: params(&schema),
        };
        let output = sink
            .finalize(
                vec![SortState::Building {
                    pending: RunBuilder::default(),
                    memory_runs: vec![run],
                    spill_runs: vec![],
                    schema: Some(schema),
                }],
                spawner.spill_manager.scope(0, 0),
                &spawner,
            )
            .await
            .unwrap()
            .unwrap();
        let BlockingSinkOutput::Partitions(mut stream) = output else {
            panic!("expected partitions")
        };
        drop(stream.next().await.unwrap().unwrap());
        let downstream = spawner.reserve_memory(32 * 1024 * 1024).await.unwrap();
        assert!(root.exists());
        drop(downstream);
        // Resume one disk frame, then cancel with unread frames still on disk.
        drop(stream.next().await.unwrap().unwrap());
        drop(stream);
        // RuntimeTask cancellation is asynchronous. Wait for the producer's file handles
        // to be dropped, without retaining any references to its state in the test.
        loop {
            fn has_file(path: &std::path::Path) -> bool {
                path.exists()
                    && std::fs::read_dir(path).unwrap().any(|entry| {
                        let path = entry.unwrap().path();
                        path.is_file() || has_file(&path)
                    })
            }
            if manager.used_bytes() == 0 && !has_file(&root) {
                break;
            }
            tokio::task::yield_now().await;
        }
        drop(spawner);
        std::fs::remove_dir_all(root).unwrap();
    })
    .await
    .expect("cancellation must release the producer's resources");
}

#[tokio::test]
async fn spilled_run_has_multiple_bounded_frames_in_one_file() {
    let (manager, spawner, root) = spawner(128 * 1024 * 1024);
    let run = make_run(&spawner, (0..20_000).collect(), 1024);
    let schema = run.partition.schema();
    let spilled = write_memory_run(&run, &spawner.spill_manager.scope(0, 0), &spawner)
        .await
        .unwrap();
    assert_eq!(spilled.files.len(), 1);
    assert!(spilled.read_bytes < 18 * 1024 * 1024);
    assert!(spilled.files[0].len() > 19 * 1024 * 1024);
    drop(run);
    let mut stream = merge_files(
        vec![MergeRun::Spill(spilled)],
        schema.clone(),
        params(&schema),
        spawner.clone(),
    )
    .await
    .unwrap();
    let mut rows = 0;
    while let Some(partition) = stream.next().await {
        rows += partition.unwrap().len();
    }
    assert_eq!(rows, 20_000);
    drop(stream);
    assert_eq!(manager.used_bytes(), 0);
    drop(spawner);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn low_memory_finalize_reclaims_and_merges() {
    let (manager, spawner, root) = spawner(32 * 1024 * 1024);
    let runs: Vec<_> = (0..8)
        .map(|i| make_run(&spawner, (0..3000).map(|j| j * 8 + i).collect(), 1024))
        .collect();
    let schema = runs[0].partition.schema();
    let sink = SortSink {
        params: params(&schema),
    };
    let state = SortState::Building {
        pending: RunBuilder::default(),
        memory_runs: runs,
        spill_runs: vec![],
        schema: Some(schema),
    };
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        sink.finalize(vec![state], spawner.spill_manager.scope(0, 0), &spawner),
    )
    .await
    .unwrap()
    .unwrap();
    let BlockingSinkOutput::Partitions(mut stream) = output.unwrap() else {
        panic!("expected partitions")
    };
    let mut count = 0;
    while let Some(partition) = stream.next().await {
        for batch in partition.unwrap().record_batches() {
            for row in 0..batch.len() {
                assert_eq!(batch.get_column(0).i64().unwrap().get(row), Some(count));
                count += 1;
            }
        }
    }
    assert_eq!(count, 24_000);
    drop(stream);
    assert_eq!(manager.used_bytes(), 0);
    drop(spawner);
    if root.exists() {
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn large_run_rotates_files_at_byte_target() {
    let (_manager, spawner, root) = spawner(512 * 1024 * 1024);
    let run = make_run(&spawner, (0..140_000).collect(), 1024);
    let spilled = write_memory_run(&run, &spawner.spill_manager.scope(0, 0), &spawner)
        .await
        .unwrap();
    assert_eq!(spilled.files.len(), 2);
    assert!(spilled.files[0].len() >= TARGET_SPILL_FILE_BYTES);
    assert!(spilled.files[0].len() < TARGET_SPILL_FILE_BYTES + 9 * 1024 * 1024);
    assert!(spilled.files[1].len() < TARGET_SPILL_FILE_BYTES);
    let paths: Vec<_> = spilled
        .files
        .iter()
        .map(|file| file.path().to_owned())
        .collect();
    drop(spilled);
    assert!(paths.iter().all(|path| !path.exists()));
    drop(run);
    drop(spawner);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn oversized_row_gets_an_explicit_reservation() {
    let (manager, spawner, _root) = spawner(128 * 1024 * 1024);
    let run = make_run(&spawner, vec![0], 10 * 1024 * 1024);
    let schema = run.partition.schema();
    let runs = vec![MergeRun::Memory(run)];
    let params = params(&schema);
    let admission = try_admit_merge(
        &runs,
        params.comparator_pair_bytes(&schema).unwrap(),
        &spawner,
    )
    .unwrap()
    .unwrap();
    let competitor = manager
        .worker_pool()
        .try_reserve(manager.total_bytes() - manager.used_bytes())
        .unwrap()
        .unwrap();
    let mut stream = merge_admitted(runs, schema, params, spawner, admission)
        .await
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().len(), 1);
    assert!(stream.next().await.is_none());
    drop(stream);
    drop(competitor);
    assert_eq!(manager.used_bytes(), 0);
}

#[tokio::test]
async fn many_column_keys_reuse_input_memory() {
    let (manager, spawner, _root) = spawner(256 * 1024 * 1024);
    let schema = Arc::new(Schema::new(
        (0..32).map(|i| Field::new(format!("c{i}"), DataType::Int64)),
    ));
    let arrays = (0..32)
        .map(|_| {
            Arc::new(arrow_array::Int64Array::from_iter_values(0..50_000)) as arrow_array::ArrayRef
        })
        .collect();
    let batch = RecordBatch::from_arrow(schema.clone(), arrays).unwrap();
    let partition = MicroPartition::new_loaded(schema.clone(), Arc::new(vec![batch]), None);
    let memory = spawner
        .try_reserve_memory(partition.size_bytes() as u64)
        .unwrap()
        .unwrap();
    let params = Arc::new(SortParams {
        sort_by: (0..32)
            .map(|i| {
                BoundExpr::try_new(daft_dsl::unresolved_col(format!("c{i}")), &schema).unwrap()
            })
            .collect(),
        descending: vec![false; 32],
        nulls_first: vec![true; 32],
    });
    let mut stream = merge_files(
        vec![MergeRun::Memory(MemoryRun {
            partition,
            memory,
            max_row_working_bytes: OnceLock::new(),
        })],
        schema,
        params,
        spawner,
    )
    .await
    .unwrap();
    let mut rows = 0;
    let mut batches = 0;
    while let Some(partition) = stream.next().await {
        let partition = partition.unwrap();
        assert!(partition_bytes(&partition).unwrap() <= TARGET_MERGE_BATCH_BYTES);
        rows += partition.len();
        batches += 1;
    }
    assert_eq!(rows, 50_000);
    // 12.8 MB of column data needs two 8 MiB outputs, not one descriptor per row.
    assert_eq!(batches, 2);
    drop(stream);
    assert_eq!(manager.used_bytes(), 0);
}

#[tokio::test]
async fn single_run_transfers_a_whole_batch_without_copying() {
    let (manager, spawner, root) = spawner(64 * 1024 * 1024);
    let run = make_run(&spawner, (0..1000).collect(), 16);
    let schema = run.partition.schema();
    let original = run.partition.record_batches()[0]
        .get_column(0)
        .to_arrow()
        .unwrap();
    let mut stream = merge_files(
        vec![MergeRun::Memory(run)],
        schema.clone(),
        params(&schema),
        spawner,
    )
    .await
    .unwrap();
    let retained = manager.used_bytes();
    // No further ordinary reservation is needed after admission.
    let competitor = manager
        .reserve(manager.total_bytes() - retained)
        .await
        .unwrap();
    let output = stream.next().await.unwrap().unwrap();
    let actual = output.record_batches()[0].get_column(0).to_arrow().unwrap();
    assert_eq!(
        original.to_data().buffers()[0].as_ptr(),
        actual.to_data().buffers()[0].as_ptr()
    );
    assert_eq!(output.len(), 1000);
    assert_eq!(manager.used_bytes(), manager.total_bytes());
    drop(output);
    assert!(stream.next().await.is_none());
    drop(stream);
    assert_eq!(manager.used_bytes(), competitor.bytes());
    drop(competitor);
    assert_eq!(manager.used_bytes(), 0);
    assert!(!root.exists());
}

#[tokio::test]
async fn interleaved_wide_runs_merge_in_batches_with_prepaid_memory() {
    use arrow_array::{Array, Int64Array};

    let (manager, spawner, root) = spawner(64 * 1024 * 1024);
    let schema = Arc::new(Schema::new(
        std::iter::once(Field::new("key", DataType::Int64))
            .chain((1..32).map(|i| Field::new(format!("value_{i}"), DataType::Int64)))
            .collect::<Vec<_>>(),
    ));
    let mut runs = Vec::new();
    for run in 0..8 {
        let keys = (0..512).map(|i| i * 8 + run).collect::<Vec<i64>>();
        let mut columns: Vec<arrow_array::ArrayRef> =
            vec![Arc::new(Int64Array::from(keys.clone()))];
        for column in 1..32 {
            columns.push(Arc::new(Int64Array::from_iter(
                keys.iter()
                    .map(|&key| (key % 17 != 0).then_some(key + column)),
            )));
        }
        let batch = RecordBatch::from_arrow(schema.clone(), columns).unwrap();
        let arrays = batch
            .as_materialized_series()
            .into_iter()
            .map(SeriesSize::new)
            .collect::<DaftResult<Vec<_>>>()
            .unwrap();
        let size = RowWorkingSize::new(&arrays);
        // Fixed-width columns, including nullable ones, are sized once per frame.
        assert!(size.variable.is_empty());
        for row in [0, 17, 511] {
            assert_eq!(
                size.bytes(&arrays, row).unwrap(),
                run_builder::selection_working_bytes(&arrays, row, 1).unwrap()
            );
        }
        let partition = MicroPartition::new_loaded(schema.clone(), Arc::new(vec![batch]), None);
        let memory = spawner
            .try_reserve_memory(partition.size_bytes() as u64)
            .unwrap()
            .unwrap();
        runs.push(MergeRun::Memory(MemoryRun {
            partition,
            memory,
            max_row_working_bytes: OnceLock::new(),
        }));
    }
    let mut stream = merge_files(runs, schema.clone(), params(&schema), spawner)
        .await
        .unwrap();
    let competitor = manager
        .reserve(manager.total_bytes() - manager.used_bytes())
        .await
        .unwrap();
    let mut rows = 0_i64;
    let mut batches = 0;
    while let Some(partition) = stream.next().await {
        let partition = partition.unwrap();
        for batch in partition.record_batches() {
            for column in 0..32 {
                let array = batch.get_column(column).to_arrow().unwrap();
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                for row in 0..batch.len() {
                    let key = rows + row as i64;
                    if column > 0 && key % 17 == 0 {
                        assert!(array.is_null(row));
                    } else {
                        assert_eq!(array.value(row), key + column as i64);
                    }
                }
            }
            rows += batch.len() as i64;
            batches += 1;
        }
    }
    assert_eq!(rows, 4096);
    assert!(
        batches < 16,
        "alternating runs must still produce batch-sized outputs"
    );
    drop(stream);
    assert_eq!(manager.used_bytes(), competitor.bytes());
    drop(competitor);
    assert_eq!(manager.used_bytes(), 0);
    assert!(!root.exists());
}

#[tokio::test]
async fn admitted_merge_keeps_room_for_larger_later_frames_under_contention() {
    let (manager, spawner, root) = spawner(32 * 1024 * 1024);
    let small = make_run(&spawner, vec![0], 1);
    let large = make_run(&spawner, (1..7001).collect(), 1024);
    let schema = small.partition.schema();
    let writer = spawner
        .spill_manager
        .partition_writer(spawner.spill_manager.scope(0, 0), schema.clone())
        .await
        .unwrap();
    let writer = writer.append(small.partition.clone()).await.unwrap();
    let writer = writer.append(large.partition.clone()).await.unwrap();
    let read_bytes = writer.read_bytes();
    let file = writer.finish().await.unwrap();
    let max_row_working_bytes = small
        .max_row_working_bytes()
        .unwrap()
        .max(large.max_row_working_bytes().unwrap());
    drop(small);
    drop(large);
    let runs = vec![MergeRun::Spill(SpillRun {
        files: VecDeque::from([file]),
        read_bytes,
        max_row_working_bytes,
    })];
    let params = params(&schema);
    let admission = try_admit_merge(
        &runs,
        params.comparator_pair_bytes(&schema).unwrap(),
        &spawner,
    )
    .unwrap()
    .unwrap();
    let reserved = manager.used_bytes();
    assert!(reserved >= read_bytes + admission.workspace_bytes);
    // Another operator consumes every byte outside the admitted working set before readers
    // have even opened. Loading a larger second frame must not request additional worker memory.
    let competitor = manager
        .worker_pool()
        .try_reserve(manager.total_bytes() - reserved)
        .unwrap()
        .unwrap();
    let mut stream = merge_admitted(runs, schema, params, spawner.clone(), admission)
        .await
        .unwrap();
    let mut rows = 0;
    while let Some(partition) = stream.next().await {
        rows += partition.unwrap().len();
    }
    assert_eq!(rows, 7001);
    drop(stream);
    drop(competitor);
    assert_eq!(manager.used_bytes(), 0);
    drop(spawner);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn spilled_merge_waits_for_cooperative_recovery_before_admission() {
    let (manager, spawner, root) = spawner(1024 * 1024);
    let run = make_run(&spawner, (0..100).collect(), 1024);
    let schema = run.partition.schema();
    let scope = spawner.spill_manager.scope(0, 0);
    let spilled = write_memory_run(&run, &scope, &spawner).await.unwrap();
    drop(run);
    let mut target = manager
        .worker_pool()
        .register_release_target("competing operator");
    let competitor = manager
        .worker_pool()
        .try_reserve(manager.total_bytes())
        .unwrap()
        .unwrap();
    target.set_reclaim_bytes(competitor.bytes());
    let sink = SortSink {
        params: params(&schema),
    };
    let state = SortState::Building {
        pending: RunBuilder::default(),
        memory_runs: vec![],
        spill_runs: vec![spilled],
        schema: Some(schema),
    };
    let (result, ()) = tokio::join!(sink.finalize(vec![state], scope, &spawner), async {
        let request = tokio::time::timeout(std::time::Duration::from_secs(5), target.recv())
            .await
            .unwrap();
        let released = competitor.bytes();
        drop(competitor);
        request.complete(released);
    });
    let BlockingSinkOutput::Partitions(mut stream) = result.unwrap().unwrap() else {
        panic!("expected partitions")
    };
    let mut rows = 0;
    while let Some(partition) = stream.next().await {
        rows += partition.unwrap().len();
    }
    assert_eq!(rows, 100);
    drop(stream);
    assert_eq!(manager.used_bytes(), 0);
    assert!(manager.released_bytes() > 0);
    drop(spawner);
    std::fs::remove_dir_all(root).unwrap();
}
