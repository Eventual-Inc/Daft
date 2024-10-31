use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{
    prelude::{Int64Array, Schema, UInt64Array, Utf8Array},
    series::IntoSeries,
};
use daft_micropartition::MicroPartition;
use daft_table::Table;

use crate::{FileWriter, WriterFactory};

pub(crate) struct DummyWriterFactory;

impl WriterFactory for DummyWriterFactory {
    type Input = Arc<MicroPartition>;
    type Result = Option<Table>;

    fn create_writer(
        &self,
        file_idx: usize,
        partition_values: Option<&Table>,
    ) -> DaftResult<Box<dyn FileWriter<Input = Self::Input, Result = Self::Result>>> {
        Ok(Box::new(DummyWriter {
            file_idx: file_idx.to_string(),
            partition_values: partition_values.cloned(),
            write_count: 0,
        })
            as Box<
                dyn FileWriter<Input = Self::Input, Result = Self::Result>,
            >)
    }
}

pub(crate) struct DummyWriter {
    file_idx: String,
    partition_values: Option<Table>,
    write_count: usize,
}

impl FileWriter for DummyWriter {
    type Input = Arc<MicroPartition>;
    type Result = Option<Table>;

    fn write(&mut self, _input: &Self::Input) -> DaftResult<()> {
        self.write_count += 1;
        Ok(())
    }

    fn close(&mut self) -> DaftResult<Self::Result> {
        let path_series =
            Utf8Array::from_values("path", std::iter::once(self.file_idx.clone())).into_series();
        let write_count_series =
            UInt64Array::from_values("write_count", std::iter::once(self.write_count as u64))
                .into_series();
        let path_table = Table::new_unchecked(
            Schema::new(vec![
                path_series.field().clone(),
                write_count_series.field().clone(),
            ])
            .unwrap(),
            vec![path_series.into(), write_count_series.into()],
            1,
        );
        if let Some(partition_values) = self.partition_values.take() {
            let unioned = path_table.union(&partition_values)?;
            Ok(Some(unioned))
        } else {
            Ok(Some(path_table))
        }
    }
}

pub(crate) fn make_dummy_mp(num_rows: usize) -> Arc<MicroPartition> {
    let series =
        Int64Array::from_values("ints", std::iter::repeat(42).take(num_rows)).into_series();
    let schema = Arc::new(Schema::new(vec![series.field().clone()]).unwrap());
    let table = Table::new_unchecked(schema.clone(), vec![series.into()], num_rows);
    Arc::new(MicroPartition::new_loaded(
        schema.into(),
        vec![table].into(),
        None,
    ))
}
