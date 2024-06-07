use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::{col, ExprRef};
use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;

use crate::partition::partition_ref::PartitionMetadata;

use super::PartitionTaskOp;

/// Boundary sampling task op.
#[derive(Debug)]
pub struct BoundarySamplingOp {
    size: usize,
    sort_by: Vec<ExprRef>,
    resource_request: ResourceRequest,
}

impl BoundarySamplingOp {
    pub fn new(size: usize, sort_by: Vec<ExprRef>) -> Self {
        Self {
            size,
            sort_by,
            resource_request: ResourceRequest::default_cpu(),
        }
    }
}

impl PartitionTaskOp for BoundarySamplingOp {
    type Input = MicroPartition;

    fn execute(&self, mut inputs: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        assert_eq!(inputs.len(), 1);
        let input = inputs.iter().next().unwrap();
        let predicate = self
            .sort_by
            .iter()
            .map(|e| col(e.name()).not_null())
            .collect::<Vec<_>>();
        let out = input
            .sample_by_size(self.size, false, None)?
            .eval_expression_list(self.sort_by.as_slice())?
            .filter(predicate.as_slice())?;
        Ok(vec![Arc::new(out)])
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn partial_metadata_from_input_metadata(&self, _: &[PartitionMetadata]) -> PartitionMetadata {
        todo!()
    }

    fn name(&self) -> &str {
        "BoundarySamplingOp"
    }
}

/// Samples to quantiles task op.
#[derive(Debug)]
pub struct SamplesToQuantilesOp {
    num_quantiles: usize,
    sort_by: Vec<ExprRef>,
    descending: Vec<bool>,
    num_inputs: usize,
    resource_request: ResourceRequest,
}

impl SamplesToQuantilesOp {
    pub fn new(
        num_quantiles: usize,
        sort_by: Vec<ExprRef>,
        descending: Vec<bool>,
        num_inputs: usize,
    ) -> Self {
        Self {
            num_quantiles,
            sort_by,
            descending,
            num_inputs,
            resource_request: ResourceRequest::default_cpu(),
        }
    }
}

impl PartitionTaskOp for SamplesToQuantilesOp {
    type Input = MicroPartition;

    fn execute(&self, inputs: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let inputs = inputs
            .iter()
            .map(|input| input.as_ref())
            .collect::<Vec<_>>();
        let input = MicroPartition::concat(inputs.as_slice())?;
        let sort_by = self
            .sort_by
            .iter()
            .map(|e| col(e.name()))
            .collect::<Vec<_>>();
        let merge_sorted = input.sort(sort_by.as_slice(), self.descending.as_slice())?;
        let out = merge_sorted.quantiles(self.num_quantiles)?;
        Ok(vec![Arc::new(out)])
    }

    fn num_inputs(&self) -> usize {
        self.num_inputs
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn partial_metadata_from_input_metadata(&self, _: &[PartitionMetadata]) -> PartitionMetadata {
        todo!()
    }

    fn name(&self) -> &str {
        "SamplesToQuantilesOp"
    }
}

/// Fanout range task op.
#[derive(Debug)]
pub struct FanoutRangeOp {
    num_outputs: usize,
    sort_by: Vec<ExprRef>,
    descending: Vec<bool>,
    resource_request: ResourceRequest,
}

impl FanoutRangeOp {
    pub fn new(num_outputs: usize, sort_by: Vec<ExprRef>, descending: Vec<bool>) -> Self {
        Self {
            num_outputs,
            sort_by,
            descending,
            resource_request: ResourceRequest::default_cpu(),
        }
    }
}

impl PartitionTaskOp for FanoutRangeOp {
    type Input = MicroPartition;

    fn execute(&self, inputs: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        assert!(inputs.len() == 2);
        let mut input_iter = inputs.iter();
        let boundaries = input_iter.next().unwrap();
        let inputs = input_iter.next().unwrap();
        if self.num_outputs == 1 {
            return Ok(vec![inputs.clone()]);
        }
        let io_stats = IOStatsContext::new("MicroPartition::to_table");
        let boundaries = boundaries.concat_or_get(io_stats)?;
        let boundaries = match &boundaries.as_ref()[..] {
            [table] => table,
            _ => unreachable!(),
        };
        log::warn!("Boundaries num rows: {}", boundaries.len());
        let partitioned = inputs.partition_by_range(&self.sort_by, boundaries, &self.descending)?;
        log::warn!("Partitioned num MicroPartitions: {}", partitioned.len());
        assert!(!partitioned.is_empty());
        let schema = partitioned[0].schema();
        let mut partitioned = partitioned.into_iter().map(Arc::new).collect::<Vec<_>>();
        if partitioned.len() != self.num_outputs {
            partitioned.extend(
                std::iter::repeat(Arc::new(MicroPartition::empty(Some(schema))))
                    .take(self.num_outputs - partitioned.len()),
            );
        }
        log::warn!(
            "Partitioned num MicroPartitions after padding: {}",
            partitioned.len()
        );
        Ok(partitioned)
    }

    fn num_inputs(&self) -> usize {
        2
    }

    fn num_outputs(&self) -> usize {
        self.num_outputs
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn partial_metadata_from_input_metadata(&self, _: &[PartitionMetadata]) -> PartitionMetadata {
        todo!()
    }

    fn name(&self) -> &str {
        "FanoutRangeOp"
    }
}

/// Sorted merge task op.
#[derive(Debug)]
pub struct SortedMergeOp {
    num_inputs: usize,
    sort_by: Vec<ExprRef>,
    descending: Vec<bool>,
    resource_request: ResourceRequest,
}

impl SortedMergeOp {
    pub fn new(num_inputs: usize, sort_by: Vec<ExprRef>, descending: Vec<bool>) -> Self {
        Self {
            num_inputs,
            sort_by,
            descending,
            resource_request: ResourceRequest::default_cpu(),
        }
    }
}

impl PartitionTaskOp for SortedMergeOp {
    type Input = MicroPartition;

    fn execute(&self, inputs: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let inputs = inputs
            .iter()
            .map(|input| input.as_ref())
            .collect::<Vec<_>>();
        let concated = MicroPartition::concat(inputs.as_slice())?;
        concated
            .sort(&self.sort_by, &self.descending)
            .map(|mp| vec![Arc::new(mp)])
    }

    fn num_inputs(&self) -> usize {
        self.num_inputs
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn partial_metadata_from_input_metadata(&self, _: &[PartitionMetadata]) -> PartitionMetadata {
        todo!()
    }

    fn name(&self) -> &str {
        "FanoutRangeOp"
    }
}
