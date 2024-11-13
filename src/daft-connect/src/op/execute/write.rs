use std::collections::HashMap;

use common_daft_config::DaftExecutionConfig;
use common_file_formats::FileFormat;
use spark_connect::{
    write_operation::{SaveMode, SaveType},
    Relation, WriteOperation,
};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::{error, warn};

use crate::{
    invalid_argument_err, not_found_err,
    op::execute::{ExecuteStream, PlanIds},
    session::Session,
};

impl Session {
    pub async fn handle_write_operation(
        &self,
        command: WriteOperation,
    ) -> Result<ExecuteStream, Status> {
        println!("handling write operation {command:#?}");
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
        } = command;

        let Some(input) = input else {
            return invalid_argument_err!("input is required");
        };

        let Some(source) = source else {
            return invalid_argument_err!("source is required");
        };

        let Ok(mode) = SaveMode::try_from(mode) else {
            return invalid_argument_err!("mode is invalid");
        };

        if !sort_column_names.is_empty() {
            warn!(
                "sort_column_names is not yet implemented; got {:?}",
                sort_column_names
            );
        }

        if !partitioning_columns.is_empty() {
            warn!(
                "partitioning_columns is not yet implemented; got {:?}",
                partitioning_columns
            );
        }

        if let Some(bucket_by) = bucket_by {
            warn!("bucket_by is not yet implemented; got {:?}", bucket_by);
        }

        if !options.is_empty() {
            warn!("options is not yet implemented; got {:?}", options);
        }

        if !clustering_columns.is_empty() {
            warn!(
                "clustering_columns is not yet implemented; got {:?}",
                clustering_columns
            );
        }

        let Some(save_type) = save_type else {
            return invalid_argument_err!("save_type is required");
        };

        let plan_ids = PlanIds::new(self.client_side_session_id(), self.server_side_session_id());

        match save_type {
            SaveType::Path(path) => {
                self.write_to_path(plan_ids, path, input, source, mode)
                    .await
            }
            SaveType::Table(..) => {
                invalid_argument_err!("Table is not yet implemented. use path instead")
            }
        }
    }

    async fn write_to_path(
        &self,
        plan_ids: PlanIds,
        path: String,
        input: Relation,
        source: String,
        mode: SaveMode,
    ) -> Result<ExecuteStream, Status> {
        if source != "parquet" {
            return not_found_err!("{source} is not yet implemented; use parquet instead");
        }

        match mode {
            SaveMode::Unspecified => {}
            SaveMode::Append => {}
            SaveMode::Overwrite => {}
            SaveMode::ErrorIfExists => {}
            SaveMode::Ignore => {}
        }

        let plan_builder = match crate::translation::to_logical_plan(input) {
            Ok(plan) => plan,
            Err(e) => {
                error!("Failed to build logical plan: {e:?}");
                return invalid_argument_err!("Failed to build logical plan: {e:?}");
            }
        };

        println!("writing to path: {path}");
        let builder = plan_builder
            .logical_plan
            .table_write(&path, FileFormat::Parquet, None, None, None)
            .unwrap();

        let logical_plan = builder.build();

        let physical_plan = std::thread::scope(|s| {
            s.spawn(|| daft_local_plan::translate(&logical_plan).unwrap())
                .join()
        })
        .unwrap();

        println!("physical plan: {physical_plan:#?}");

        let cfg = DaftExecutionConfig::default();
        let results = daft_local_execution::run_local(
            &physical_plan,
            plan_builder.partition,
            cfg.into(),
            None,
            CancellationToken::new(), // todo: maybe implement cancelling
        )
        .unwrap();

        // todo: remove
        std::thread::scope(|s| {
            s.spawn(|| {
                for result in results {
                    println!("result: {result:?}");
                }
            });
        });

        let res = futures::stream::once(async move { Ok(plan_ids.finished()) });
        Ok(Box::pin(res))
    }
}
