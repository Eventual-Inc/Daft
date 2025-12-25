use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use common_runtime::get_compute_pool_num_threads;
use daft_core::prelude::{SchemaRef, UInt64Array};
use daft_local_plan::SamplingMethod;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use indexmap::IndexMap;
use rand::{
    SeedableRng,
    distributions::{Distribution, Standard},
    rngs::StdRng,
};
use tracing::{Span, instrument};

use super::base::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeOutput,
    StreamingSinkFinalizeResult, StreamingSinkOutput,
};
use crate::{ExecutionTaskSpawner, dynamic_batching::StaticBatchingStrategy, pipeline::NodeName};

fn build_rng(seed: Option<u64>) -> StdRng {
    match seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::from_rng(rand::thread_rng()).expect("failed to seed rng"),
    }
}

struct SampleRow {
    key: f64,
    record_batch: Arc<RecordBatch>,
    row_idx: u64,
}

pub(crate) struct WithReplacementState {
    slots: Vec<Option<SampleRow>>,
    rng: StdRng,
}

impl WithReplacementState {
    fn new(size: usize, seed: Option<u64>) -> Self {
        let mut slots = Vec::with_capacity(size);
        for _ in 0..size {
            slots.push(None);
        }
        Self {
            slots,
            rng: build_rng(seed),
        }
    }

    fn process_record_batch(&mut self, record_batch: RecordBatch) -> DaftResult<()> {
        if self.slots.is_empty() || record_batch.is_empty() {
            return Ok(());
        }
        let rb = Arc::new(record_batch);
        for row_idx in 0..rb.len() {
            for slot in &mut self.slots {
                let new_key: f64 = Standard.sample(&mut self.rng);
                match slot {
                    None => {
                        *slot = Some(SampleRow {
                            key: new_key,
                            record_batch: rb.clone(),
                            row_idx: row_idx as u64,
                        });
                    }
                    Some(existing) if new_key < existing.key => {
                        *slot = Some(SampleRow {
                            key: new_key,
                            record_batch: rb.clone(),
                            row_idx: row_idx as u64,
                        });
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    fn into_samples(self) -> DaftResult<Vec<RecordBatch>> {
        collect_sample_rows(self.slots.into_iter().flatten().collect())
    }
}

pub(crate) struct WithoutReplacementState {
    rows_seen: usize,
    entries: Vec<SampleRow>,
    rng: StdRng,
    capacity: usize,
}

impl WithoutReplacementState {
    fn new(size: usize, seed: Option<u64>) -> Self {
        Self {
            rows_seen: 0,
            entries: Vec::with_capacity(size),
            rng: build_rng(seed),
            capacity: size,
        }
    }

    fn process_record_batch(&mut self, record_batch: RecordBatch) -> DaftResult<()> {
        if record_batch.is_empty() {
            return Ok(());
        }
        let rb = Arc::new(record_batch);
        for row_idx in 0..rb.len() {
            self.rows_seen += 1;
            let key: f64 = Standard.sample(&mut self.rng);
            let new_entry = SampleRow {
                key,
                record_batch: rb.clone(),
                row_idx: row_idx as u64,
            };

            if self.entries.len() < self.capacity {
                self.entries.push(new_entry);
            } else if let Some(max_idx) = self.max_key_index()
                && key < self.entries[max_idx].key
            {
                self.entries[max_idx] = new_entry;
            }
        }
        Ok(())
    }

    fn into_samples(self) -> DaftResult<Vec<RecordBatch>> {
        collect_sample_rows(self.entries)
    }

    fn max_key_index(&self) -> Option<usize> {
        self.entries
            .iter()
            .enumerate()
            .max_by(|a, b| a.1.key.partial_cmp(&b.1.key).unwrap())
            .map(|(idx, _)| idx)
    }
}

fn collect_sample_rows(rows: Vec<SampleRow>) -> DaftResult<Vec<RecordBatch>> {
    let mut rows_needed_per_batch: IndexMap<usize, (Arc<RecordBatch>, Vec<u64>)> = IndexMap::new();
    for sample in rows {
        let key = Arc::as_ptr(&sample.record_batch) as usize;
        rows_needed_per_batch
            .entry(key)
            .or_insert_with(|| (sample.record_batch.clone(), Vec::new()))
            .1
            .push(sample.row_idx);
    }

    let mut taken_batches = Vec::with_capacity(rows_needed_per_batch.len());
    for (_, (record_batch, rows_needed)) in rows_needed_per_batch {
        let taken = record_batch.take(&UInt64Array::from(("idx", rows_needed)))?;
        taken_batches.push(taken);
    }
    Ok(taken_batches)
}

pub(crate) enum SampleBySizeState {
    WithReplacement(WithReplacementState),
    WithoutReplacement(WithoutReplacementState),
}

impl SampleBySizeState {
    fn new(size: usize, with_replacement: bool, seed: Option<u64>) -> Self {
        if with_replacement {
            Self::WithReplacement(WithReplacementState::new(size, seed))
        } else {
            Self::WithoutReplacement(WithoutReplacementState::new(size, seed))
        }
    }

    fn process_record_batch(&mut self, record_batch: RecordBatch) -> DaftResult<()> {
        match self {
            Self::WithReplacement(state) => state.process_record_batch(record_batch),
            Self::WithoutReplacement(state) => state.process_record_batch(record_batch),
        }
    }

    fn into_samples(self, size: usize) -> DaftResult<Vec<RecordBatch>> {
        match self {
            Self::WithReplacement(state) => state.into_samples(),
            Self::WithoutReplacement(state) => {
                if state.rows_seen < size {
                    return Err(DaftError::ValueError(
                        "Cannot take a sample larger than the population when 'replace=False'"
                            .to_string(),
                    ));
                }
                state.into_samples()
            }
        }
    }
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum SampleState {
    Fraction(()),
    Size(SampleBySizeState),
}

struct SampleParams {
    sampling_method: SamplingMethod,
    with_replacement: bool,
    seed: Option<u64>,
    schema: SchemaRef,
}

pub struct SampleSink {
    params: Arc<SampleParams>,
}

impl SampleSink {
    pub fn new(
        sampling_method: SamplingMethod,
        with_replacement: bool,
        seed: Option<u64>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            params: Arc::new(SampleParams {
                sampling_method,
                with_replacement,
                seed,
                schema,
            }),
        }
    }

    fn build_output(
        params: &SampleParams,
        samples: Vec<RecordBatch>,
    ) -> DaftResult<Vec<Arc<MicroPartition>>> {
        if samples.is_empty() {
            return Ok(vec![Arc::new(MicroPartition::empty(Some(
                params.schema.clone(),
            )))]);
        }

        let record_batches: Vec<RecordBatch> =
            samples.into_iter().map(|rb| rb.as_ref().clone()).collect();
        let mp = MicroPartition::new_loaded(params.schema.clone(), Arc::new(record_batches), None);
        Ok(vec![Arc::new(mp)])
    }
}

impl StreamingSink for SampleSink {
    type State = SampleState;
    type BatchingStrategy = StaticBatchingStrategy;
    #[instrument(skip_all, name = "SampleSink::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        let method = self.params.sampling_method;
        let with_replacement = self.params.with_replacement;
        let seed = self.params.seed;
        match (state, method) {
            (SampleState::Fraction(()), SamplingMethod::Fraction(fraction)) => spawner
                .spawn(
                    async move {
                        let out = input.sample_by_fraction(fraction, with_replacement, seed)?;
                        Ok((
                            SampleState::Fraction(()),
                            StreamingSinkOutput::NeedMoreInput(Some(Arc::new(out))),
                        ))
                    },
                    Span::current(),
                )
                .into(),
            (SampleState::Size(mut size_state), SamplingMethod::Size(_)) => spawner
                .spawn(
                    async move {
                        for rb in input.record_batches().iter().cloned() {
                            size_state.process_record_batch(rb)?;
                        }
                        Ok((
                            SampleState::Size(size_state),
                            StreamingSinkOutput::NeedMoreInput(None),
                        ))
                    },
                    Span::current(),
                )
                .into(),
            _ => {
                unreachable!("Invalid state/params combination")
            }
        }
    }

    #[instrument(skip_all, name = "SampleSink::finalize")]
    fn finalize(
        &self,
        mut states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult<Self> {
        let params = self.params.clone();
        spawner
            .spawn(
                async move {
                    match &params.sampling_method {
                        SamplingMethod::Fraction(_) => {
                            Ok(StreamingSinkFinalizeOutput::Finished(None))
                        }
                        SamplingMethod::Size(size) => {
                            let SampleState::Size(size_state) = states.pop().unwrap() else {
                                unreachable!("Invalid state/params combination");
                            };
                            let samples = size_state.into_samples(*size)?;
                            let output = Self::build_output(&params, samples)?;
                            Ok(StreamingSinkFinalizeOutput::Finished(Some(
                                output.into_iter().next().unwrap(),
                            )))
                        }
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        match &self.params.sampling_method {
            SamplingMethod::Fraction(fraction) => format!("Sample fraction = {}", fraction).into(),
            SamplingMethod::Size(size) => format!("Sample Size = {}", size).into(),
        }
    }

    fn op_type(&self) -> NodeType {
        NodeType::Sample
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        match &self.params.sampling_method {
            SamplingMethod::Fraction(fraction) => {
                res.push(format!("Sample fraction = {}", fraction));
            }
            SamplingMethod::Size(size) => {
                res.push(format!("Sample Size = {}", size));
            }
        }
        res.push(format!(
            "With replacement = {}",
            self.params.with_replacement
        ));
        res.push(format!("Seed = {:?}", self.params.seed));
        res
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        match &self.params.sampling_method {
            SamplingMethod::Fraction(_) => Ok(SampleState::Fraction(())),
            SamplingMethod::Size(size) => Ok(SampleState::Size(SampleBySizeState::new(
                *size,
                self.params.with_replacement,
                self.params.seed,
            ))),
        }
    }

    fn max_concurrency(&self) -> usize {
        match &self.params.sampling_method {
            SamplingMethod::Fraction(_) => get_compute_pool_num_threads(),
            SamplingMethod::Size(_) => 1,
        }
    }

    fn batching_strategy(&self) -> Self::BatchingStrategy {
        StaticBatchingStrategy::new(self.morsel_size_requirement().unwrap_or_default())
    }
}
