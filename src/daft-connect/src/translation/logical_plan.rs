use std::{collections::HashMap, sync::Arc};

use daft_core::prelude::Series;
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::MicroPartition;
use daft_scan::{
    builder::{parquet_scan, ParquetScanBuilder},
    python::pylib::ScanOperatorHandle,
};
use daft_schema::schema::Schema;
use daft_table::Table;
use eyre::{bail, ensure, Context};
use pyo3::{prelude::*, types::PyDict};
use spark_connect::{
    read,
    read::{DataSource, ReadType},
    relation::RelType,
    Range, Read, Relation,
};
use tracing::warn;

use crate::translation::logical_plan;

#[derive(Debug)]
pub struct Builder {
    pub logical_plan: LogicalPlanBuilder,
    pub partition: HashMap<String, Vec<Arc<MicroPartition>>>,
}

impl Builder {
    pub fn plain(logical_plan: LogicalPlanBuilder) -> Self {
        Self {
            logical_plan,
            partition: HashMap::new(),
        }
    }

    pub fn new(
        logical_plan: LogicalPlanBuilder,
        partition: HashMap<String, Vec<Arc<MicroPartition>>>,
    ) -> Self {
        Self {
            logical_plan,
            partition,
        }
    }
}

pub fn to_logical_plan(relation: Relation) -> eyre::Result<Builder> {
    if let Some(common) = relation.common {
        warn!("Ignoring common metadata for relation: {common:?}; not yet implemented");
    };

    let Some(rel_type) = relation.rel_type else {
        bail!("Relation type is required");
    };

    match rel_type {
        RelType::Range(r) => range(r).wrap_err("Failed to apply range to logical plan"),
        RelType::Read(r) => read(r).wrap_err("Failed to apply read to logical plan"),
        plan => bail!("Unsupported relation type: {plan:?}"),
    }
}

fn range(range: Range) -> eyre::Result<Builder> {
    let Range {
        start,
        end,
        step,
        num_partitions,
    } = range;

    let num_partitions = num_partitions.unwrap_or(1);
    let num_partitions =
        usize::try_from(num_partitions).wrap_err("num_partitions must fit into usize")?;

    let start = start.unwrap_or(0);

    let step = usize::try_from(step).wrap_err("step must be a positive integer")?;
    ensure!(step > 0, "step must be greater than 0");

    let plan = Python::with_gil(|py| {
        // let io_config = io_config.unwrap_or_default();
        //
        // let native_storage_config = NativeStorageConfig {
        //     io_config: Some(io_config),
        //     multithreaded_io,
        // };
        //
        // let py_storage_config: PyStorageConfig =
        //     Arc::new(StorageConfig::Native(Arc::new(native_storage_config))).into();
        //
        // let py_io_config = PyIOConfig { config: io_config };
        let range_module = PyModule::import_bound(py, "daft.io.range")
            .wrap_err("Failed to import daft.io.range")?;

        let range = range_module
            .getattr(pyo3::intern!(py, "RangeScanOperator"))
            .wrap_err("Failed to get range function")?;

        let range = range
            .call1((start, end, step))
            .wrap_err("Failed to create range scan operator")?
            .to_object(py);

        let scan_operator_handle = ScanOperatorHandle::from_python_scan_operator(range, py)?;

        let plan = LogicalPlanBuilder::table_scan(scan_operator_handle.into(), None)?;

        eyre::Result::<_>::Ok(plan)
    })
    .wrap_err("Failed to create range scan")?;

    Ok(Builder::new(plan, HashMap::new()))
}

pub fn read(read: Read) -> eyre::Result<Builder> {
    let Read {
        is_streaming,
        read_type,
    } = read;

    warn!("Ignoring is_streaming for read: {is_streaming}; not yet implemented");

    let Some(read_type) = read_type else {
        bail!("Read type is required");
    };

    match read_type {
        ReadType::NamedTable(table) => {
            bail!("Table read not yet implemented");
        }
        ReadType::DataSource(data_source) => {
            let res = read_data_source(data_source)
                .wrap_err("Failed to apply data source to logical plan");

            warn!("Read data source: {:?}", res);
            res
        }
    }
}

fn read_data_source(data_source: read::DataSource) -> eyre::Result<Builder> {
    println!("{:?}", data_source);
    let read::DataSource {
        format,
        schema,
        options,
        paths,
        predicates,
    } = data_source;

    let Some(format) = format else {
        bail!("Format is required");
    };

    if let Some(schema) = schema {
        warn!("Ignoring schema for data source: {schema:?}; not yet implemented");
    }

    if !options.is_empty() {
        warn!("Ignoring options for data source: {options:?}; not yet implemented");
    }

    if !predicates.is_empty() {
        warn!("Ignoring predicates for data source: {predicates:?}; not yet implemented");
    }

    ensure!(!paths.is_empty(), "Paths are required");

    let plan = std::thread::scope(move |s| {
        s.spawn(move || {
            parquet_scan(paths)
                .finish()
                .wrap_err("Failed to create parquet scan")
        })
        .join()
    })
    .unwrap()?;

    let builder = Builder::plain(plan);
    Ok(builder)
}
