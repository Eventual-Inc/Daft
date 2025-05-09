use std::{any::Any, collections::HashSet, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{expr::bound_expr::BoundExpr, ExprRef, WindowExpr};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::ExecutionTaskSpawner;

#[derive(Clone)]
struct RankState {
    current_rank: u64,
    next_rank: u64,
    previous_value: Option<RecordBatch>,
}

#[derive(Clone)]
struct DenseRankState {
    current_rank: u64,
    previous_value: Option<RecordBatch>,
}

pub struct WindowOrderByOnlyState {
    record_batches: Vec<RecordBatch>,
    window_exprs: Vec<WindowExpr>,
    aliases: Vec<String>,
    original_schema: SchemaRef,
    row_offset: u64,
    rank_state: RankState,
    dense_rank_state: DenseRankState,
}

impl WindowOrderByOnlyState {
    fn new(
        window_exprs: Vec<WindowExpr>,
        aliases: Vec<String>,
        original_schema: SchemaRef,
    ) -> Self {
        Self {
            record_batches: Vec::new(),
            window_exprs,
            aliases,
            original_schema,
            row_offset: 0,
            rank_state: RankState {
                current_rank: 1,
                next_rank: 1,
                previous_value: None,
            },
            dense_rank_state: DenseRankState {
                current_rank: 1,
                previous_value: None,
            },
        }
    }

    fn push(&mut self, input: Arc<MicroPartition>) -> DaftResult<()> {
        let tables = input.get_tables()?;
        for i in 0..tables.len() {
            self.record_batches.push(tables[i].clone());
        }
        Ok(())
    }

    fn drain_record_batches(&mut self) -> Vec<RecordBatch> {
        std::mem::take(&mut self.record_batches)
    }
}

impl IntermediateOpState for WindowOrderByOnlyState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct WindowOrderByOnlyOperator {
    order_by: Vec<ExprRef>,
    _descending: Vec<bool>,
    window_exprs: Vec<WindowExpr>,
    aliases: Vec<String>,
    original_schema: SchemaRef,
}

impl WindowOrderByOnlyOperator {
    pub fn new(
        window_exprs: &[WindowExpr],
        aliases: &[String],
        order_by: &[ExprRef],
        descending: &[bool],
        schema: &SchemaRef,
    ) -> DaftResult<Self> {
        Ok(Self {
            window_exprs: window_exprs.to_vec(),
            aliases: aliases.to_vec(),
            order_by: order_by.to_vec(),
            _descending: descending.to_vec(),
            original_schema: schema.clone(),
        })
    }
}

impl IntermediateOperator for WindowOrderByOnlyOperator {
    #[instrument(skip_all, name = "WindowOrderByOnlyOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn IntermediateOpState>,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult {
        let window_state = state
            .as_any_mut()
            .downcast_mut::<WindowOrderByOnlyState>()
            .expect("WindowOrderByOnlyOperator must own WindowOrderByOnlyState");

        window_state.push(input).unwrap();

        let window_exprs = window_state.window_exprs.clone();
        let aliases = window_state.aliases.clone();
        let original_schema = window_state.original_schema.clone();
        let mut batches = window_state.drain_record_batches();

        let mut row_offset = window_state.row_offset;
        let mut rank_state = window_state.rank_state.clone();
        let mut dense_rank_state = window_state.dense_rank_state.clone();

        let order_by = self
            .order_by
            .iter()
            .map(|expr| BoundExpr::try_new(expr.clone(), &original_schema))
            .collect::<DaftResult<Vec<_>>>()
            .unwrap();

        task_spawner
            .spawn(
                async move {
                    let mut out_batches = Vec::with_capacity(batches.len());

                    for mut batch in batches.drain(..) {
                        if batch.is_empty() {
                            continue;
                        }

                        let mut seen_exprs = HashSet::new();
                        for (wexpr, name) in window_exprs.iter().zip(&aliases) {
                            if !seen_exprs.insert(wexpr) {
                                return Err(DaftError::InternalError(format!(
                                    "Window expression {:?} appears more than once in window_exprs",
                                    wexpr
                                )));
                            }

                            batch = match wexpr {
                                WindowExpr::RowNumber => {
                                    batch.window_row_number_global(name.clone(), &mut row_offset)?
                                }
                                WindowExpr::Rank => batch.window_rank_global(
                                    name.clone(),
                                    &order_by,
                                    &mut rank_state.current_rank,
                                    &mut rank_state.next_rank,
                                    &mut rank_state.previous_value,
                                    false,
                                )?,
                                WindowExpr::DenseRank => {
                                    let mut next_rank_placeholder = 0; // unused
                                    batch.window_rank_global(
                                        name.clone(),
                                        &order_by,
                                        &mut dense_rank_state.current_rank,
                                        &mut next_rank_placeholder,
                                        &mut dense_rank_state.previous_value,
                                        true,
                                    )?
                                }
                                _ => unimplemented!("Unsupported window fn: {wexpr:?}"),
                            };
                        }
                        out_batches.push(batch);
                    }

                    if let Some(ws) = state.as_any_mut().downcast_mut::<WindowOrderByOnlyState>() {
                        ws.row_offset = row_offset;
                        ws.rank_state = rank_state;
                        ws.dense_rank_state = dense_rank_state;
                    }

                    let output = if out_batches.is_empty() {
                        MicroPartition::empty(Some(original_schema.clone()))
                    } else {
                        MicroPartition::new_loaded(
                            original_schema.clone(),
                            out_batches.into(),
                            None,
                        )
                    };

                    Ok((
                        state,
                        IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(output))),
                    ))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "WindowOrderByOnly"
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push(format!(
            "WindowOrderByOnly: {}",
            self.window_exprs.iter().map(|e| e.to_string()).join(", ")
        ));
        if !self.order_by.is_empty() {
            display.push(format!(
                "Order by: {}",
                self.order_by.iter().map(|e| e.to_string()).join(", ")
            ));
        }
        display
    }

    fn make_state(&self) -> DaftResult<Box<dyn IntermediateOpState>> {
        Ok(Box::new(WindowOrderByOnlyState::new(
            self.window_exprs.clone(),
            self.aliases.clone(),
            self.original_schema.clone(),
        )))
    }

    fn max_concurrency(&self) -> DaftResult<usize> {
        Ok(1)
    }
}
