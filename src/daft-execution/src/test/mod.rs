use std::{marker::PhantomData, sync::Arc};

use common_error::DaftResult;
use daft_core::{datatypes::Field, schema::Schema, DataType, Series};
use daft_micropartition::MicroPartition;
use daft_plan::ResourceRequest;
use daft_scan::{
    file_format::FileFormatConfig, storage_config::StorageConfig, DataFileSource, ScanTask,
};
use daft_table::Table;

use crate::ops::PartitionTaskOp;

pub(crate) fn mock_scan_task() -> ScanTask {
    ScanTask::new(
        vec![DataFileSource::AnonymousDataFile {
            path: "foo".to_string(),
            chunk_spec: None,
            size_bytes: None,
            metadata: None,
            partition_spec: None,
            statistics: None,
        }],
        FileFormatConfig::Json(Default::default()).into(),
        Schema::empty().into(),
        StorageConfig::Native(Default::default()).into(),
        Default::default(),
    )
}

pub(crate) fn mock_micropartition(num_rows: usize) -> MicroPartition {
    let field = Field::new("a", DataType::Int64);
    let schema = Arc::new(Schema::new(vec![field.clone()]).unwrap());
    MicroPartition::new_loaded(
        schema.clone(),
        Arc::new(vec![Table::new(
            schema.clone(),
            vec![Series::from_arrow(
                field.into(),
                arrow2::array::Int64Array::from_vec((0..num_rows).map(|n| n as i64).collect())
                    .boxed(),
            )
            .unwrap()],
        )
        .unwrap()]),
        None,
    )
}

#[derive(Debug)]
pub(crate) struct MockOutputOp<T> {
    num_outputs: usize,
    resource_request: ResourceRequest,
    name: String,
    marker: PhantomData<T>,
}

impl<T> MockOutputOp<T> {
    pub(crate) fn new(name: impl Into<String>, num_outputs: usize) -> Self {
        Self {
            num_outputs,
            resource_request: Default::default(),
            name: name.into(),
            marker: PhantomData,
        }
    }
}

impl<T: std::fmt::Debug + Sync + Send> PartitionTaskOp for MockOutputOp<T> {
    type Input = T;

    fn execute(&self, inputs: &[Arc<T>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let mp = mock_micropartition(1);
        let data = Arc::new(mp);
        Ok(std::iter::repeat_with(|| data.clone()).collect())
    }

    fn num_outputs(&self) -> usize {
        self.num_outputs
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn partial_metadata_from_input_metadata(
        &self,
        input_meta: &[crate::partition::partition_ref::PartitionMetadata],
    ) -> crate::partition::partition_ref::PartitionMetadata {
        todo!()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
pub(crate) struct MockInputOutputOp {
    num_inputs: usize,
    num_outputs: usize,
    resource_request: ResourceRequest,
    name: String,
}

impl MockInputOutputOp {
    pub(crate) fn new(name: impl Into<String>, num_inputs: usize, num_outputs: usize) -> Self {
        Self {
            num_inputs,
            num_outputs,
            resource_request: Default::default(),
            name: name.into(),
        }
    }
}

impl PartitionTaskOp for MockInputOutputOp {
    type Input = MicroPartition;

    fn execute(&self, inputs: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        Ok(inputs.to_vec())
    }

    fn num_inputs(&self) -> usize {
        self.num_inputs
    }

    fn num_outputs(&self) -> usize {
        self.num_outputs
    }

    fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    fn partial_metadata_from_input_metadata(
        &self,
        input_meta: &[crate::partition::partition_ref::PartitionMetadata],
    ) -> crate::partition::partition_ref::PartitionMetadata {
        todo!()
    }

    fn name(&self) -> &str {
        &self.name
    }
}
