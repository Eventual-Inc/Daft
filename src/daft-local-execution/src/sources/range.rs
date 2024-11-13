use std::sync::Arc;

use arrow2::array::Array;
use async_trait::async_trait;
use common_error::DaftResult;
use daft_core::{
    datatypes::{DataType, Field},
    prelude::{Schema, SchemaRef},
    series::Series,
};
use daft_io::IOStatsRef;
use daft_micropartition::MicroPartition;
use daft_table::Table;
use futures::stream::{self, StreamExt};
use tracing::instrument;

use crate::sources::source::{Source, SourceStream};

pub struct RangeSource {
    pub start: i64,
    pub end: i64,
    pub step: usize,
    pub num_partitions: usize,
    pub schema: SchemaRef,
}

fn to_micropartition(start: i64, end: i64, step: usize) -> DaftResult<MicroPartition> {
    let values: Vec<_> = (start..end).step_by(step).map(Some).collect();
    let len = values.len();

    let field = Field::new("value", DataType::Int64);
    let schema = Schema::new(vec![field.clone()])?;

    let field = Arc::new(field);

    let int_array = arrow2::array::Int64Array::from_iter(values);
    let arrow_array: Box<dyn Array> = Box::new(int_array);
    let series = Series::from_arrow(field, arrow_array)?;

    let table = Table::new_unchecked(schema.clone(), vec![series], len);
    Ok(MicroPartition::new_loaded(
        schema.into(),
        vec![table].into(),
        None,
    ))
}

impl RangeSource {
    pub fn new(start: i64, end: i64, step: usize, num_partitions: usize) -> Self {
        let field = Field::new("value", DataType::Int64);
        let schema = Schema::new(vec![field]).unwrap();
        Self {
            start,
            end,
            step,
            num_partitions,
            schema: Arc::new(schema),
        }
    }

    pub fn arced(self) -> Arc<dyn Source> {
        Arc::new(self) as Arc<dyn Source>
    }
}

#[async_trait]
impl Source for RangeSource {
    #[instrument(name = "RangeSource::get_data", level = "info", skip_all)]
    async fn get_data(
        &self,
        _maintain_order: bool,
        _io_stats: IOStatsRef,
    ) -> DaftResult<SourceStream<'static>> {
        let total_elements = ((self.end - self.start) as usize + self.step - 1) / self.step;
        let elements_per_partition =
            (total_elements + self.num_partitions - 1) / self.num_partitions;

        let step = self.step;
        let start = self.start;
        let end = self.end;

        let partitions = (0..self.num_partitions).map(move |i| {
            let start = start + (i * elements_per_partition * step) as i64;
            let end = end.min(start + (elements_per_partition * step) as i64);

            to_micropartition(start, end, step).map(Arc::new)
        });

        Ok(Box::pin(stream::iter(partitions)))
    }

    fn name(&self) -> &'static str {
        "Range"
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}
