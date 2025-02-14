use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_treenode::{
    DynTreeNode, Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
};
use daft_logical_plan::{
    ops::Source,
    optimization::OptimizerBuilder,
    source_info::{InMemoryInfo, PlaceHolderInfo, SourceInfo},
    LogicalPlan, LogicalPlanRef,
};
use serde::{Deserialize, Serialize};

use super::translate::translate_single_logical_node;
use crate::{
    ops::{InMemoryScan, PlaceholderScan},
    PhysicalPlan, PhysicalPlanRef,
};

pub(super) struct PhysicalPlanTranslator {
    pub physical_children: Vec<Arc<PhysicalPlan>>,
    pub cfg: Arc<DaftExecutionConfig>,
}

impl TreeNodeVisitor for PhysicalPlanTranslator {
    type Node = Arc<LogicalPlan>;
    fn f_down(&mut self, _node: &Self::Node) -> DaftResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &Self::Node) -> DaftResult<TreeNodeRecursion> {
        let (output, _) =
            translate_single_logical_node(node, &mut self.physical_children, &self.cfg)?;
        self.physical_children.push(output);
        Ok(TreeNodeRecursion::Continue)
    }
}

pub(super) struct LogicalStageTranslator {
    pub physical_children: Vec<Arc<PhysicalPlan>>,
    pub root: Arc<LogicalPlan>,
    pub cfg: Arc<DaftExecutionConfig>,
}

impl TreeNodeRewriter for LogicalStageTranslator {
    type Node = Arc<LogicalPlan>;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        let (translated_pplan, is_logical_boundary) =
            translate_single_logical_node(&node, &mut self.physical_children, &self.cfg)?;

        let is_root_node = Arc::ptr_eq(&node, &self.root);
        if is_logical_boundary && !is_root_node {
            log::info!(
                "Detected Query Stage Boundary at {}",
                translated_pplan.name()
            );
            match &translated_pplan.arc_children()[..] {
                [] | [_] => {
                    self.physical_children.push(translated_pplan.clone());

                    let ph_info =
                        PlaceHolderInfo::new(node.schema(), translated_pplan.clustering_spec());

                    let new_scan = LogicalPlan::Source(Source::new(
                        node.schema(),
                        SourceInfo::PlaceHolder(ph_info).into(),
                    ));
                    Ok(Transformed::new(
                        new_scan.arced(),
                        true,
                        TreeNodeRecursion::Stop,
                    ))
                }
                [left, right] => {
                    fn num_in_memory_children(plan: &PhysicalPlan) -> usize {
                        if matches!(plan, PhysicalPlan::InMemoryScan(..)) {
                            1
                        } else {
                            plan.arc_children()
                                .iter()
                                .map(|c| num_in_memory_children(c))
                                .sum()
                        }
                    }

                    enum RunNext {
                        Left,
                        Right,
                    }

                    let run_next: RunNext = match (left.as_ref(), right.as_ref()) {
                        (PhysicalPlan::InMemoryScan(..), _) => {
                            // we know the left, so let's run the right
                            RunNext::Right
                        }
                        (_, PhysicalPlan::InMemoryScan(..)) => {
                            // we know the right, so let's run the left
                            RunNext::Left
                        }
                        (_, _) => {
                            let left_num_in_memory_children = num_in_memory_children(left.as_ref());
                            let right_num_in_memory_children =
                                num_in_memory_children(right.as_ref());

                            if left_num_in_memory_children > right_num_in_memory_children {
                                RunNext::Left
                            } else if left_num_in_memory_children < right_num_in_memory_children {
                                RunNext::Right
                            } else {
                                // both sides are not in memory, so we should rank which side to run
                                let left_stats = left.approximate_stats();
                                let right_stats = right.approximate_stats();

                                if left_stats.size_bytes <= right_stats.size_bytes {
                                    RunNext::Left
                                } else {
                                    RunNext::Right
                                }
                            }
                        }
                    };
                    match run_next {
                        RunNext::Left => {
                            self.physical_children.push(left.clone());
                            let logical_children = node.arc_children();
                            let logical_left = logical_children.first().expect(
                                "we expect the logical node of a binary op to also have 2 children",
                            );
                            let logical_right = logical_children.get(1).expect(
                                "we expect the logical node of a binary op to also have 2 children",
                            );
                            let ph_info =
                                PlaceHolderInfo::new(logical_left.schema(), left.clustering_spec());

                            let new_left_scan = LogicalPlan::Source(Source::new(
                                logical_left.schema(),
                                SourceInfo::PlaceHolder(ph_info).into(),
                            ));
                            let new_bin_node = node
                                .with_new_children(&[new_left_scan.arced(), logical_right.clone()]);
                            Ok(Transformed::new(
                                new_bin_node.arced(),
                                true,
                                TreeNodeRecursion::Stop,
                            ))
                        }
                        RunNext::Right => {
                            self.physical_children.push(right.clone());
                            let logical_children = node.arc_children();
                            let logical_left = logical_children.first().expect(
                                "we expect the logical node of a binary op to also have 2 children",
                            );
                            let logical_right = logical_children.get(1).expect(
                                "we expect the logical node of a binary op to also have 2 children",
                            );

                            let ph_info = PlaceHolderInfo::new(
                                logical_right.schema(),
                                right.clustering_spec(),
                            );

                            let new_right_scan = LogicalPlan::Source(Source::new(
                                logical_right.schema(),
                                SourceInfo::PlaceHolder(ph_info).into(),
                            ));
                            let new_bin_node = node
                                .with_new_children(&[logical_left.clone(), new_right_scan.arced()]);
                            Ok(Transformed::new(
                                new_bin_node.arced(),
                                true,
                                TreeNodeRecursion::Stop,
                            ))
                        }
                    }
                }
                _ => panic!("We shouldn't have any nodes that have more than 3 children"),
            }
        } else {
            self.physical_children.push(translated_pplan);
            Ok(Transformed::no(node))
        }
    }
}

struct ReplaceLogicalPlaceholderWithMaterializedResults {
    mat_results: Option<MaterializedResults>,
}

impl TreeNodeRewriter for ReplaceLogicalPlaceholderWithMaterializedResults {
    type Node = Arc<LogicalPlan>;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        match node.as_ref() {
            LogicalPlan::Source(Source {
                output_schema: _,
                source_info,
                ..
            }) => match source_info.as_ref() {
                SourceInfo::PlaceHolder(phi) => {
                    assert!(self.mat_results.is_some());
                    let mut mat_results = self.mat_results.take().unwrap();
                    // use the clustering spec from the original plan
                    mat_results.in_memory_info.clustering_spec = Some(phi.clustering_spec.clone());
                    mat_results.in_memory_info.source_schema = phi.source_schema.clone();

                    let new_source_node = LogicalPlan::Source(Source::new(
                        mat_results.in_memory_info.source_schema.clone(),
                        SourceInfo::InMemory(mat_results.in_memory_info).into(),
                    ))
                    .arced();
                    Ok(Transformed::new(
                        new_source_node,
                        true,
                        TreeNodeRecursion::Stop,
                    ))
                }
                _ => Ok(Transformed::no(node)),
            },
            _ => Ok(Transformed::no(node)),
        }
    }
}

struct PhysicalStageTranslator {
    partial_physical_plan: Option<PhysicalPlanRef>,
    root: Arc<PhysicalPlan>,
}

impl TreeNodeRewriter for PhysicalStageTranslator {
    type Node = Arc<PhysicalPlan>;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        let is_root = Arc::ptr_eq(&node, &self.root);
        if is_root {
            return Ok(Transformed::no(node));
        }

        // If the node is a shuffle exchange, we have reached a physical boundary
        let shuffle_exchange = match node.as_ref() {
            PhysicalPlan::ShuffleExchange(shuffle_exchange) => shuffle_exchange,
            _ => return Ok(Transformed::no(node)),
        };

        // If the input is an in memory scan, emit the shuffle exchange and return a placeholder
        if matches!(
            shuffle_exchange.input.as_ref(),
            PhysicalPlan::InMemoryScan(..)
        ) {
            self.partial_physical_plan = Some(node.clone());
            let placeholder =
                PhysicalPlan::PlaceholderScan(PlaceholderScan::new(node.clustering_spec().clone()));
            return Ok(Transformed::new(
                placeholder.arced(),
                true,
                TreeNodeRecursion::Stop,
            ));
        }

        // Otherwise, emit the child and add a placeholder as the child of the shuffle exchange
        let child = shuffle_exchange.input.clone();
        let placeholder =
            PhysicalPlan::PlaceholderScan(PlaceholderScan::new(child.clustering_spec().clone()));
        self.partial_physical_plan = Some(child);
        let node_with_placeholder_child = node.with_new_children(&[placeholder.arced()]);
        Ok(Transformed::new(
            node_with_placeholder_child.arced(),
            true,
            TreeNodeRecursion::Stop,
        ))
    }
}

struct ReplacePhysicalPlaceholderWithMaterializedResults {
    mat_results: Option<MaterializedResults>,
}

impl TreeNodeRewriter for ReplacePhysicalPlaceholderWithMaterializedResults {
    type Node = Arc<PhysicalPlan>;

    fn f_down(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> DaftResult<common_treenode::Transformed<Self::Node>> {
        match node.as_ref() {
            PhysicalPlan::PlaceholderScan(ph_scan) => {
                let mat_results = self.mat_results.take().unwrap();
                let new_source_node = PhysicalPlan::InMemoryScan(InMemoryScan::new(
                    mat_results.in_memory_info.source_schema.clone(),
                    mat_results.in_memory_info.clone(),
                    ph_scan.clustering_spec().clone(),
                ));
                Ok(Transformed::new(
                    new_source_node.arced(),
                    true,
                    TreeNodeRecursion::Stop,
                ))
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum QueryStageOutput {
    Partial { physical_plan: PhysicalPlanRef },
    Final { physical_plan: PhysicalPlanRef },
}

#[derive(PartialEq, Debug)]
enum AdaptivePlannerStatus {
    Ready,
    WaitingForStats,
    Done,
}

pub struct MaterializedResults {
    pub in_memory_info: InMemoryInfo,
}

pub struct AdaptivePlanner {
    remaining_logical_plan: Option<LogicalPlanRef>,
    remaining_physical_plan: Option<PhysicalPlanRef>,
    cfg: Arc<DaftExecutionConfig>,
    status: AdaptivePlannerStatus,
}

impl AdaptivePlanner {
    pub fn new(logical_plan: LogicalPlanRef, cfg: Arc<DaftExecutionConfig>) -> Self {
        Self {
            remaining_logical_plan: Some(logical_plan),
            remaining_physical_plan: None,
            cfg,
            status: AdaptivePlannerStatus::Ready,
        }
    }

    fn transform_physical_plan(
        &mut self,
        physical_plan: PhysicalPlanRef,
    ) -> DaftResult<PhysicalPlanRef> {
        let mut physical_stage_translator = PhysicalStageTranslator {
            partial_physical_plan: None,
            root: physical_plan.clone(),
        };
        let result = physical_plan.rewrite(&mut physical_stage_translator)?;
        if result.transformed {
            assert!(physical_stage_translator.partial_physical_plan.is_some());
            self.remaining_physical_plan = Some(result.data);
            let physical_plan = physical_stage_translator.partial_physical_plan.unwrap();
            return Ok(physical_plan);
        } else {
            assert!(physical_stage_translator.partial_physical_plan.is_none());
            let physical_plan = result.data;
            return Ok(physical_plan);
        }
    }

    pub fn next_stage(&mut self) -> DaftResult<QueryStageOutput> {
        assert_eq!(self.status, AdaptivePlannerStatus::Ready);

        // First, check if we have a remaining physical plan
        if let Some(remaining_physical_plan) = self.remaining_physical_plan.take() {
            let physical_plan = self.transform_physical_plan(remaining_physical_plan)?;
            // If we have no remaining logical plan and no remaining physical plan, we can emit the final plan
            if self.remaining_logical_plan.is_none() && self.remaining_physical_plan.is_none() {
                log::info!("Emitting final plan:\n {}", physical_plan.repr_ascii(true));
                self.status = AdaptivePlannerStatus::Done;
                return Ok(QueryStageOutput::Final { physical_plan });
            } else {
                // Otherwise, we need to emit a partial plan
                log::info!(
                    "Emitting partial plan:\n {}",
                    physical_plan.repr_ascii(true)
                );
                self.status = AdaptivePlannerStatus::WaitingForStats;
                return Ok(QueryStageOutput::Partial { physical_plan });
            }
        }

        // If we have no remaining physical plan, we need to translate the remaining logical plan to a physical plan
        let logical_plan = self.remaining_logical_plan.take().unwrap();
        let mut logical_rewriter = LogicalStageTranslator {
            physical_children: vec![],
            root: logical_plan.clone(),
            cfg: self.cfg.clone(),
        };
        let logical_output = logical_plan.rewrite(&mut logical_rewriter)?;
        let physical_plan = logical_rewriter
            .physical_children
            .pop()
            .expect("should have at least 1 child");
        let transformed_physical_plan = self.transform_physical_plan(physical_plan)?;
        // If we transformed the logical plan, this means we have a remaining logical plan
        if logical_output.transformed {
            self.remaining_logical_plan = Some(logical_output.data);
            self.status = AdaptivePlannerStatus::WaitingForStats;
            log::info!(
                "Logical plan remaining:\n {}",
                self.remaining_logical_plan
                    .as_ref()
                    .unwrap()
                    .repr_ascii(true)
            );

            log::info!(
                "Emitting partial plan:\n {}",
                transformed_physical_plan.repr_ascii(true)
            );
            return Ok(QueryStageOutput::Partial {
                physical_plan: transformed_physical_plan,
            });
        } else {
            log::info!("Logical plan translation complete");
            if self.remaining_physical_plan.is_some() {
                log::info!(
                    "Emitting partial plan:\n {}",
                    transformed_physical_plan.repr_ascii(true)
                );
                self.status = AdaptivePlannerStatus::WaitingForStats;
                return Ok(QueryStageOutput::Partial {
                    physical_plan: transformed_physical_plan,
                });
            } else {
                log::info!(
                    "Emitting final plan:\n {}",
                    transformed_physical_plan.repr_ascii(true)
                );
                self.status = AdaptivePlannerStatus::Done;
                return Ok(QueryStageOutput::Final {
                    physical_plan: transformed_physical_plan,
                });
            }
        }
    }

    pub fn is_done(&self) -> bool {
        self.status == AdaptivePlannerStatus::Done
    }

    pub fn update(&mut self, mat_results: MaterializedResults) -> DaftResult<()> {
        assert_eq!(self.status, AdaptivePlannerStatus::WaitingForStats);

        // If we have a remaining physical plan, we need to replace the physical placeholder with the materialized results
        if let Some(remaining_physical_plan) = self.remaining_physical_plan.take() {
            let mut rewriter = ReplacePhysicalPlaceholderWithMaterializedResults {
                mat_results: Some(mat_results),
            };
            let result = remaining_physical_plan.rewrite(&mut rewriter)?;

            assert!(result.transformed);

            self.remaining_physical_plan = Some(result.data);
        }
        // Otherwise, we need to replace the logical placeholder with the materialized results
        else {
            let mut rewriter = ReplaceLogicalPlaceholderWithMaterializedResults {
                mat_results: Some(mat_results),
            };
            let logical_plan = self.remaining_logical_plan.take().unwrap();
            let result = logical_plan.rewrite(&mut rewriter)?;

            assert!(result.transformed);

            let optimizer = OptimizerBuilder::new().enrich_with_stats().build();

            let optimized_logical_plan = optimizer.optimize(
                result.data,
                |new_plan, rule_batch, pass, transformed, seen| {
                    if transformed {
                        log::debug!(
                        "Rule batch {:?} transformed plan on pass {}, and produced {} plan:\n{}",
                        rule_batch,
                        pass,
                        if seen { "an already seen" } else { "a new" },
                        new_plan.repr_ascii(true),
                    );
                    } else {
                        log::debug!(
                            "Rule batch {:?} did NOT transform plan on pass {} for plan:\n{}",
                            rule_batch,
                            pass,
                            new_plan.repr_ascii(true),
                        );
                    }
                },
            )?;

            self.remaining_logical_plan = Some(optimized_logical_plan);
        }

        self.status = AdaptivePlannerStatus::Ready;
        Ok(())
    }
}
