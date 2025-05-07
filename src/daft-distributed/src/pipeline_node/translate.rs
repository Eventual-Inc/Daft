use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_scan_info::ScanState;
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{stats::StatsState, LogicalPlan, SourceInfo};

use super::PipelineInput;

static SOURCE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn get_next_source_id() -> usize {
    SOURCE_ID_COUNTER.fetch_add(1, Ordering::SeqCst)
}

#[derive(Clone)]
pub(crate) struct PipelinePlan {
    pub local_plan: LocalPhysicalPlanRef,
    pub input: PipelineInput,
}

#[allow(dead_code)]
pub(crate) fn translate_logical_plan_to_pipeline_plan(
    logical_plan: &LogicalPlan,
    execution_config: &DaftExecutionConfig,
    psets: HashMap<String, Vec<PartitionRef>>,
) -> DaftResult<PipelinePlan> {
    // Translate the logical plan to a local plan and a list of inputs
    fn translate_logical_plan_to_local_plan_and_inputs(
        logical_plan: &LogicalPlan,
        execution_config: &DaftExecutionConfig,
        psets: HashMap<String, Vec<PartitionRef>>,
        input: &mut Option<PipelineInput>,
    ) -> DaftResult<LocalPhysicalPlanRef> {
        match logical_plan {
            LogicalPlan::Source(source) => match source.source_info.as_ref() {
                SourceInfo::InMemory(info) => {
                    let partition_refs = psets.get(&info.cache_key).unwrap().clone();
                    assert!(input.is_none(), "Pipelines cannot have multiple inputs");
                    *input = Some(PipelineInput::InMemorySource {
                        cache_key: info.cache_key.clone(),
                        partition_refs,
                    });
                    let local_plan = LocalPhysicalPlan::in_memory_scan(
                        info.clone(),
                        daft_logical_plan::stats::StatsState::NotMaterialized,
                    );
                    Ok(local_plan)
                }
                SourceInfo::Physical(info) => {
                    let scan_tasks = match &info.scan_state {
                        ScanState::Operator(scan_op) => {
                            Arc::new(scan_op.0.to_scan_tasks(info.pushdowns.clone())?)
                        }
                        ScanState::Tasks(scan_tasks) => scan_tasks.clone(),
                    };
                    if scan_tasks.is_empty() {
                        Ok(LocalPhysicalPlan::empty_scan(source.output_schema.clone()))
                    } else {
                        assert!(input.is_none(), "Pipelines cannot have multiple inputs");
                        *input = Some(PipelineInput::ScanTasks {
                            source_id: get_next_source_id(),
                            pushdowns: info.pushdowns.clone(),
                            scan_tasks,
                        });
                        Ok(LocalPhysicalPlan::placeholder_scan(
                            source.output_schema.clone(),
                            get_next_source_id(),
                            source.stats_state.clone(),
                        ))
                    }
                }
                SourceInfo::PlaceHolder(info) => {
                    assert!(input.is_none(), "Pipelines cannot have multiple inputs");
                    *input = Some(PipelineInput::Intermediate);
                    Ok(LocalPhysicalPlan::placeholder_scan(
                        info.source_schema.clone(),
                        get_next_source_id(),
                        StatsState::NotMaterialized,
                    ))
                }
            },
            plan => {
                let translated_plan = translate_logical_plan_to_local_plan_and_inputs(
                    plan,
                    execution_config,
                    psets,
                    input,
                )?;
                Ok(translated_plan)
            }
        }
    }

    let mut input = None;
    let local_plan = translate_logical_plan_to_local_plan_and_inputs(
        logical_plan,
        execution_config,
        psets,
        &mut input,
    )?;
    Ok(PipelinePlan {
        local_plan,
        input: input.expect("Pipeline input should be set"),
    })
}
