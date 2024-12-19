use std::{io::Cursor, sync::Arc};

use arrow2::io::ipc::read::{read_stream_metadata, StreamReader, StreamState};
use daft_core::{prelude::Schema, series::Series};
use daft_logical_plan::LogicalPlanBuilder;
use daft_table::Table;
use eyre::{bail, WrapErr};
use itertools::zip_eq;

use super::SparkAnalyzer;

impl SparkAnalyzer<'_> {
    pub fn local_relation(
        &self,
        plan_id: i64,
        plan: spark_connect::LocalRelation,
    ) -> eyre::Result<LogicalPlanBuilder> {
        // We can ignore spark schema. The true schema is sent in the
        // arrow data. (see read_stream_metadata)
        let spark_connect::LocalRelation { data, schema: _ } = plan;

        let Some(data) = data else {
            bail!("Data is required but was not provided in the LocalRelation plan.")
        };

        let mut reader = Cursor::new(&data);
        let metadata = read_stream_metadata(&mut reader)?;

        let arrow_schema = metadata.schema.clone();
        let daft_schema = Arc::new(
            Schema::try_from(&arrow_schema)
                .wrap_err("Failed to convert Arrow schema to Daft schema.")?,
        );

        let reader = StreamReader::new(reader, metadata, None);

        let tables = reader.into_iter().map(|ss| {
            let ss = ss.wrap_err("Failed to read next chunk from StreamReader.")?;

            let chunk = match ss {
                StreamState::Some(chunk) => chunk,
                StreamState::Waiting => {
                    bail!("StreamReader is waiting for data, but a chunk was expected. This likely indicates that the spark provided data is incomplete.")
                }
            };


            let arrays = chunk.into_arrays();
            let columns = zip_eq(arrays, &arrow_schema.fields)
                .map(|(array, arrow_field)| {
                    let field = Arc::new(arrow_field.into());

                    let series = Series::from_arrow(field, array)
                        .wrap_err("Failed to create Series from Arrow array.")?;

                    Ok(series)
                })
                .collect::<eyre::Result<Vec<_>>>()?;

            let batch = Table::from_nonempty_columns(columns)?;

            Ok(batch)
         }).collect::<eyre::Result<Vec<_>>>()?;

        self.create_in_memory_scan(plan_id as _, daft_schema, tables)
    }
}
