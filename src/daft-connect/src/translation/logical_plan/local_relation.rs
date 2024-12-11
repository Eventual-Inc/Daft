use std::{io::Cursor, sync::Arc};

use arrow2::io::ipc::{
    read::{StreamMetadata, StreamReader, StreamState, Version},
    IpcField, IpcSchema,
};
use daft_core::series::Series;
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::{
    partitioning::{
        InMemoryPartitionSetCache, MicroPartitionBatch, MicroPartitionSet, PartitionSet,
        PartitionSetCache,
    },
    MicroPartition,
};
use daft_schema::dtype::DaftDataType;
use daft_table::Table;
use eyre::{bail, ensure, WrapErr};
use itertools::Itertools;

use crate::translation::{deser_spark_datatype, to_daft_datatype};

pub fn local_relation(
    plan: spark_connect::LocalRelation,
    pset_cache: &InMemoryPartitionSetCache<MicroPartition>,
) -> eyre::Result<LogicalPlanBuilder> {
    let spark_connect::LocalRelation { data, schema } = plan;

    let Some(data) = data else {
        bail!("Data is required but was not provided in the LocalRelation plan.")
    };

    let Some(schema) = schema else {
        bail!("Schema is required but was not provided in the LocalRelation plan.")
    };

    let schema: serde_json::Value = serde_json::from_str(&schema)
        .wrap_err_with(|| format!("Failed to parse schema string into JSON format: {schema}"))?;

    // spark schema
    let schema = deser_spark_datatype(schema)?;

    // daft schema
    let schema = to_daft_datatype(&schema)?;

    // should be of type struct
    let daft_schema::dtype::DataType::Struct(daft_fields) = &schema else {
        bail!("schema must be struct")
    };

    let daft_schema = daft_schema::schema::Schema::new(daft_fields.clone())
        .wrap_err("Could not create schema")?;

    let daft_schema = Arc::new(daft_schema);

    let arrow_fields: Vec<_> = daft_fields
        .iter()
        .map(|daft_field| daft_field.to_arrow())
        .try_collect()?;

    let mut dict_idx = 0;

    let ipc_fields: Vec<_> = daft_fields
        .iter()
        .map(|field| {
            let required_dictionary = field.dtype == DaftDataType::Utf8;

            let dictionary_id = match required_dictionary {
                true => {
                    let res = dict_idx;
                    dict_idx += 1;
                    Some(res)
                }
                false => None,
            };

            //  For integer columns, we don't need dictionary encoding
            IpcField {
                fields: vec![], // No nested fields for primitive types
                dictionary_id,
            }
        })
        .collect();

    let schema = arrow2::datatypes::Schema::from(arrow_fields);

    let little_endian = true;
    let version = Version::V5;

    let tables = {
        let metadata = StreamMetadata {
            schema,
            version,
            ipc_schema: IpcSchema {
                fields: ipc_fields,
                is_little_endian: little_endian,
            },
        };

        let reader = Cursor::new(&data);
        let reader = StreamReader::new(reader, metadata, None);

        let chunks = reader.map(|value| match value {
            Ok(StreamState::Some(chunk)) => Ok(chunk.arrays().to_vec()),
            Ok(StreamState::Waiting) => {
                bail!("StreamReader is waiting for data, but a chunk was expected.")
            }
            Err(e) => bail!("Error occurred while reading chunk from StreamReader: {e}"),
        });

        // todo: eek
        let chunks = chunks.skip(1);

        let mut tables = Vec::new();

        for (idx, chunk) in chunks.enumerate() {
            let chunk = chunk.wrap_err_with(|| format!("chunk {idx} is invalid"))?;

            let mut columns = Vec::with_capacity(daft_schema.fields.len());
            let mut num_rows = Vec::with_capacity(daft_schema.fields.len());

            for (array, (_, daft_field)) in itertools::zip_eq(chunk, &daft_schema.fields) {
                // Note: Cloning field and array; consider optimizing to avoid unnecessary clones.
                let field = daft_field.clone();
                let field_ref = Arc::new(field);
                let series = Series::from_arrow(field_ref, array)
                    .wrap_err("Failed to create Series from Arrow array.")?;

                num_rows.push(series.len());
                columns.push(series);
            }

            ensure!(
                    num_rows.iter().all_equal(),
                    "Mismatch in row counts across columns; all columns must have the same number of rows."
                );

            let batch = Table::from_nonempty_columns(columns)?;

            tables.push(batch);
        }
        tables
    };

    let batch: MicroPartitionBatch = tables.try_into()?;

    let mut pset = MicroPartitionSet::default();

    let partition_id: Arc<str> = uuid::Uuid::new_v4().to_string().into();
    let pset_id: Arc<str> = uuid::Uuid::new_v4().to_string().into();

    pset.set_partition(partition_id, &batch)?;

    let num_partitions = pset.num_partitions();
    let size_bytes = pset.size_bytes()?;
    let len = pset.len();

    let lp = LogicalPlanBuilder::in_memory(&pset_id, daft_schema, num_partitions, size_bytes, len)?;
    pset_cache.put_partition_set(pset_id, Arc::new(pset))?;

    Ok(lp)
}
