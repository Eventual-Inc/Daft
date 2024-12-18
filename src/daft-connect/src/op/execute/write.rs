use std::future::ready;

use common_daft_config::DaftExecutionConfig;
use common_file_formats::FileFormat;
use daft_local_execution::NativeExecutor;
use eyre::{bail, WrapErr};
use spark_connect::{
    write_operation::{SaveMode, SaveType},
    WriteOperation,
};
use tonic::Status;
use tracing::warn;

use crate::{
    op::execute::{ExecuteStream, PlanIds},
    session::Session,
    translation,
};

impl Session {
    pub async fn handle_write_command(
        &self,
        operation: WriteOperation,
        operation_id: String,
    ) -> Result<ExecuteStream, Status> {
        use futures::StreamExt;

        let context = PlanIds {
            session: self.client_side_session_id().to_string(),
            server_side_session: self.server_side_session_id().to_string(),
            operation: operation_id,
        };

        let finished = context.finished();
        let pset = self.psets.clone();

        let result = async move {
            let WriteOperation {
                input,
                source,
                mode,
                sort_column_names,
                partitioning_columns,
                bucket_by,
                options,
                clustering_columns,
                save_type,
            } = operation;

            let Some(input) = input else {
                bail!("Input is required");
            };

            let Some(source) = source else {
                bail!("Source is required");
            };

            if source != "parquet" {
                bail!("Unsupported source: {source}; only parquet is supported");
            }

            let Ok(mode) = SaveMode::try_from(mode) else {
                bail!("Invalid save mode: {mode}");
            };

            if !sort_column_names.is_empty() {
                // todo(completeness): implement sort
                warn!("Ignoring sort_column_names: {sort_column_names:?} (not yet implemented)");
            }

            if !partitioning_columns.is_empty() {
                // todo(completeness): implement partitioning
                warn!(
                    "Ignoring partitioning_columns: {partitioning_columns:?} (not yet implemented)"
                );
            }

            if let Some(bucket_by) = bucket_by {
                // todo(completeness): implement bucketing
                warn!("Ignoring bucket_by: {bucket_by:?} (not yet implemented)");
            }

            if !options.is_empty() {
                // todo(completeness): implement options
                warn!("Ignoring options: {options:?} (not yet implemented)");
            }

            if !clustering_columns.is_empty() {
                // todo(completeness): implement clustering
                warn!("Ignoring clustering_columns: {clustering_columns:?} (not yet implemented)");
            }

            match mode {
                SaveMode::Unspecified => {}
                SaveMode::Append => {}
                SaveMode::Overwrite => {}
                SaveMode::ErrorIfExists => {}
                SaveMode::Ignore => {}
            }

            let Some(save_type) = save_type else {
                bail!("Save type is required");
            };

            let path = match save_type {
                SaveType::Path(path) => path,
                SaveType::Table(table) => {
                    let name = table.table_name;
                    bail!("Tried to write to table {name} but it is not yet implemented. Try to write to a path instead.");
                }
            };

            let translator = translation::SparkAnalyzer::new(&pset);

            let plan = translator.to_logical_plan(input).await?;

            let plan = plan
                .table_write(&path, FileFormat::Parquet, None, None, None)
                .wrap_err("Failed to create table write plan")?;

            let optimized_plan = plan.optimize()?;
            let cfg = DaftExecutionConfig::default();
            let native_executor = NativeExecutor::from_logical_plan_builder(&optimized_plan)?;

            let mut result_stream = native_executor.run(&pset, cfg.into(), None)?.into_stream();

            // this is so we make sure the operation is actually done
            // before we return
            //
            // an example where this is important is if we write to a parquet file
            // and then read immediately after, we need to wait for the write to finish
            while let Some(_result) = result_stream.next().await {}

            Ok(())
        };

        use futures::TryFutureExt;

        let result = result.map_err(|e| Status::internal(format!("Error in Daft server: {e:?}")));

        let future = result.and_then(|()| ready(Ok(finished)));
        let stream = futures::stream::once(future);

        Ok(Box::pin(stream))
    }
}
