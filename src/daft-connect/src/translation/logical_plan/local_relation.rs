use std::{collections::HashMap, io::Cursor, sync::Arc};

use arrow2::{
    compute::cast::CastOptions,
    io::ipc::{
        read::{StreamMetadata, StreamReader, StreamState, Version},
        IpcField, IpcSchema,
    },
};
use daft_core::series::Series;
use daft_logical_plan::{
    logical_plan::Source, InMemoryInfo, LogicalPlan, LogicalPlanBuilder, PyLogicalPlanBuilder,
    SourceInfo,
};
use daft_micropartition::partitioning::InMemoryPartitionSet;
use daft_table::Table;
use eyre::{bail, ensure, WrapErr};
use itertools::Itertools;

use crate::translation::{datatype::to_arrow_datatype, deser_spark_datatype, logical_plan::Plan};

pub fn local_relation(plan: spark_connect::LocalRelation) -> eyre::Result<Plan> {
    #[cfg(not(feature = "python"))]
    {
        bail!("LocalRelation plan is only supported in Python mode");
    }

    #[cfg(feature = "python")]
    {
        use daft_micropartition::{python::PyMicroPartition, MicroPartition};
        use pyo3::{types::PyAnyMethods, Python};
        let spark_connect::LocalRelation { data, schema } = plan;

        let Some(data) = data else {
            bail!("Data is required but was not provided in the LocalRelation plan.")
        };

        let Some(schema) = schema else {
            bail!("Schema is required but was not provided in the LocalRelation plan.")
        };

        let schema: serde_json::Value = serde_json::from_str(&schema).wrap_err_with(|| {
            format!("Failed to parse schema string into JSON format: {schema}")
        })?;

        // spark schema
        let schema = deser_spark_datatype(schema)?;

        // daft schema
        let schema = to_arrow_datatype(&schema)?;

        // should be of type struct
        let arrow2::datatypes::DataType::Struct(arrow_fields) = &schema else {
            bail!("schema must be struct")
        };

        //
        // let arrow_fields: Vec<_> = daft_fields
        //     .iter()
        //     .map(|daft_field| daft_field.to_arrow())
        //     .try_collect()?;

        let mut dict_idx = 0;

        let ipc_fields: Vec<_> = arrow_fields
            .iter()
            .map(|field| {
                let required_dictionary = require_dictionary(field);

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

        let daft_fields: Vec<_> = arrow_fields
            .iter()
            .map(|arrow_field| daft_schema::field::Field::from(arrow_field))
            .collect();

        let daft_schema = daft_schema::schema::Schema::new(daft_fields.clone())
            .wrap_err("Could not create schema")?;

        let daft_schema = Arc::new(daft_schema);

        let arrow_schema = arrow2::datatypes::Schema::from(arrow_fields.clone());

        let little_endian = true;
        let version = Version::V5;

        let tables = {
            let metadata = StreamMetadata {
                schema: arrow_schema,
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
                    // casting is needed in scenarios like where Daft only has Utf8 (arrow equivalent LargeUtf8)
                    // and we need to make sure the underlying data is the same as what Daft expects
                    let target_datatype = daft_field
                        .dtype
                        .to_arrow()
                        .wrap_err("could not convert to arrow field")?;

                    let array = arrow2::compute::cast::cast(
                        &*array,
                        &target_datatype,
                        CastOptions::default(),
                    )
                    .wrap_err("could not cast")?;

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

                let Some(&num_rows) = num_rows.first() else {
                    bail!("No columns were found; at least one column is required.")
                };

                let table = Table::new_with_size(daft_schema.clone(), columns, num_rows)
                    .wrap_err("Failed to create Table from columns and schema.")?;

                tables.push(table);
            }
            tables
        };

        // Note: Verify if the Daft schema used here matches the schema of the table.
        let micro_partition = MicroPartition::new_loaded(daft_schema, Arc::new(tables), None);
        let micro_partition = Arc::new(micro_partition);

        let plan = Python::with_gil(|py| {
            // Convert MicroPartition to a logical plan using Python interop.
            let py_micropartition = py
                .import_bound(pyo3::intern!(py, "daft.table"))?
                .getattr(pyo3::intern!(py, "MicroPartition"))?
                .getattr(pyo3::intern!(py, "_from_pymicropartition"))?
                .call1((PyMicroPartition::from(micro_partition.clone()),))?;

            // ERROR:   2: AttributeError: 'daft.daft.PySchema' object has no attribute '_schema'
            let py_plan_builder = py
                .import_bound(pyo3::intern!(py, "daft.dataframe.dataframe"))?
                .getattr(pyo3::intern!(py, "to_logical_plan_builder"))?
                .call1((py_micropartition,))?;

            let py_plan_builder = py_plan_builder.getattr(pyo3::intern!(py, "_builder"))?;

            let plan: PyLogicalPlanBuilder = py_plan_builder.extract()?;

            Ok::<_, eyre::Error>(plan.builder)
        })?;

        let cache_key = grab_singular_cache_key(&plan)?;

        let mut psets = HashMap::new();
        psets.insert(cache_key, vec![micro_partition]);

        let plan = Plan {
            builder: plan,
            psets: InMemoryPartitionSet::new(psets),
        };

        Ok(plan)
    }
}

fn grab_singular_cache_key(plan: &LogicalPlanBuilder) -> eyre::Result<String> {
    let plan = &*plan.plan;

    let LogicalPlan::Source(Source { source_info, .. }) = plan else {
        bail!("Expected a source plan");
    };

    let SourceInfo::InMemory(InMemoryInfo { cache_key, .. }) = &**source_info else {
        bail!("Expected an in-memory source");
    };

    Ok(cache_key.clone())
}

fn require_dictionary(field: &arrow2::datatypes::Field) -> bool {
    let datatype = field.data_type();
    matches!(
        datatype,
        arrow2::datatypes::DataType::Utf8 | arrow2::datatypes::DataType::LargeUtf8
    )
}
