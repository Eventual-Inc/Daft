use std::{cell::RefCell, collections::HashMap, sync::Arc};

use crate::simple::{common::SourceResultType, sinks::coalesce::CoalesceSink};

use super::{
    common::{Sink, SinkResultType, SourceType},
    intermediate_op::IntermediateOperatorType,
    sinks::collect::CollectSink,
};
use common_error::{DaftError, DaftResult};
use daft_dsl::{common_treenode::TreeNode, AggExpr, Expr, ExprRef};
use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_plan::{
    physical_ops::{Aggregate, Coalesce, Filter, InMemoryScan, Limit, Project, TabularScan},
    PhysicalPlan,
};
use daft_scan::ScanTask;
use futures::{future::select_all, sink};
use snafu::{futures::TryFutureExt, ResultExt};

async fn run_meta_pipeline(
    source: SourceType,
    intermediate_operators: Vec<IntermediateOperatorType>,
) -> DaftResult<Arc<MicroPartition>> {
    fn run(
        source: SourceType,
        intermediate_operators: Vec<IntermediateOperatorType>,
    ) -> DaftResult<Arc<MicroPartition>> {
        let mut partition = match source {
            SourceType::ScanTask(scan_task) => {
                let io_stats = IOStatsContext::new(format!(
                    "MicroPartition::from_scan_task for {:?}",
                    scan_task.sources
                ));
                Arc::new(MicroPartition::from_scan_task(scan_task, io_stats)?)
            }
            SourceType::InMemory(partition) => partition,
        };
        for operator in intermediate_operators {
            partition = operator.execute(&partition)?;
        }
        Ok(partition)
    }

    let result = tokio::spawn(async move {
        let (send, recv) = tokio::sync::oneshot::channel();
        rayon::spawn(move || {
            let result = run(source, intermediate_operators);
            let _ = send.send(result);
        });
        recv.await.context(crate::OneShotRecvSnafu {})?
    })
    .context(crate::JoinSnafu {})
    .await??;
    Ok(result)
}

pub struct Pipeline {
    pub sources: Option<Vec<SourceType>>,
    pub intermediate_operators: Vec<IntermediateOperatorType>,
    pub sink: Option<Box<dyn Sink>>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self {
            sources: None,
            sink: None,
            intermediate_operators: vec![],
        }
    }

    pub fn set_sources(&mut self, sources: Vec<SourceType>) {
        self.sources = Some(sources);
    }

    pub fn add_operator(&mut self, operator: IntermediateOperatorType) {
        self.intermediate_operators.push(operator);
    }

    pub fn set_sink(&mut self, sink: Box<dyn Sink>) {
        self.sink = Some(sink);
    }

    pub fn execute(mut self) -> DaftResult<Vec<Arc<MicroPartition>>> {
        if self.sink.is_none() {
            self.set_sink(Box::new(CollectSink::new()));
        }
        let local = tokio::task::LocalSet::new();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let result = local.block_on(&runtime, async move {
            let mut sink = self.sink.take().unwrap();

            tokio::task::spawn_local(async move {
                let mut meta_pipeline_futures = self
                    .sources
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|source| {
                        let intermediate_operators = self.intermediate_operators.clone();
                        Box::pin(run_meta_pipeline(source.clone(), intermediate_operators))
                    })
                    .collect::<Vec<_>>();

                while !meta_pipeline_futures.is_empty() {
                    let (result, _index, remaining_futures) =
                        select_all(meta_pipeline_futures).await;
                    let sink_result = sink.sink(&result?)?;
                    match sink_result {
                        SinkResultType::NeedMoreInput => {
                            meta_pipeline_futures = remaining_futures;
                        }
                        SinkResultType::Finished => {
                            break;
                        }
                    }
                }
                sink.finalize()
            })
            .await
            .unwrap()
        })?;
        Ok(result)
    }

    pub fn execute_with_new_sources(
        // nasty, need a way to cleanly wireup pipelines
        self,
        sources: Vec<SourceType>,
    ) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let mut new_pipeline = self;
        new_pipeline.set_sources(sources);
        new_pipeline.execute()
    }
}

pub fn execute_pipelines(mut pipelines: Vec<Pipeline>) -> DaftResult<Vec<Arc<MicroPartition>>> {
    // We can figure out which pipelines have no dependencies and execute them in parallel instead of sequentially like this
    let mut result = vec![];
    for pipeline in pipelines {
        if pipeline.sources.is_none() {
            result = pipeline.execute_with_new_sources(
                result
                    .iter()
                    .map(|x: &Arc<MicroPartition>| SourceType::InMemory(x.clone()))
                    .collect(),
            )?;
        } else {
            result = pipeline.execute()?;
        }
    }
    Ok(result)
}
