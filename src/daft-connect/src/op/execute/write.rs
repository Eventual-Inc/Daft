use std::future::ready;

use common_file_formats::FileFormat;
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

        let this = self.clone();

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

            let file_format: FileFormat = source.parse()?;

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

            let translator = translation::SparkAnalyzer::new(&this);

            let plan = translator.to_logical_plan(input).await?;

            let plan = plan
                .table_write(&path, file_format, None, None, None)
                .wrap_err("Failed to create table write plan")?;

            let mut result_stream = this.run_query(plan).await?;

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
