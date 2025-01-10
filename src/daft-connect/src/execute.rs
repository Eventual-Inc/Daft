use std::{future::ready, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use daft_dsl::LiteralValue;
use daft_local_execution::NativeExecutor;
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::MicroPartition;
use daft_ray_execution::RayEngine;
use daft_table::Table;
use eyre::{bail, Context};
use futures::{
    stream::{self, BoxStream},
    TryFutureExt, TryStreamExt,
};
use itertools::Itertools;
use pyo3::Python;
use spark_connect::{
    relation::RelType,
    write_operation::{SaveMode, SaveType},
    ExecutePlanResponse, Relation, ShowString, WriteOperation,
};
use tonic::{codegen::tokio_stream::wrappers::ReceiverStream, Status};
use tracing::debug;

use crate::{
    response_builder::ResponseBuilder, session::Session, translation, ExecuteStream, Runner,
};

impl Session {
    pub fn get_runner(&self) -> eyre::Result<Runner> {
        let runner = match self.config_values().get("daft.runner") {
            Some(runner) => match runner.as_str() {
                "ray" => Runner::Ray,
                "native" => Runner::Native,
                _ => bail!("Invalid runner: {}", runner),
            },
            None => Runner::Native,
        };
        Ok(runner)
    }

    pub async fn run_query(
        &self,
        lp: LogicalPlanBuilder,
    ) -> eyre::Result<BoxStream<DaftResult<Arc<MicroPartition>>>> {
        match self.get_runner()? {
            Runner::Ray => {
                let runner = RayEngine::try_new(None, None, None)?;
                let result_set = tokio::task::spawn_blocking(move || {
                    Python::with_gil(|py| runner.run_iter_impl(py, lp, None))
                })
                .await??;

                Ok(Box::pin(stream::iter(result_set)))
            }

            Runner::Native => {
                let this = self.clone();
                let result_stream = tokio::task::spawn_blocking(move || {
                    let plan = lp.optimize()?;
                    let cfg = Arc::new(DaftExecutionConfig::default());
                    let native_executor = NativeExecutor::from_logical_plan_builder(&plan)?;
                    let results = native_executor.run(&*this.psets, cfg, None)?;
                    let it = results.into_iter();
                    Ok::<_, DaftError>(it.collect_vec())
                })
                .await??;

                Ok(Box::pin(stream::iter(result_stream)))
            }
        }
    }

    pub async fn execute_command(
        &self,
        command: Relation,
        operation_id: String,
    ) -> Result<ExecuteStream, Status> {
        use futures::{StreamExt, TryStreamExt};

        let response_builder = ResponseBuilder::new_with_op_id(
            self.client_side_session_id(),
            self.server_side_session_id(),
            operation_id,
        );

        // fallback response
        let result_complete = response_builder.result_complete_response();

        let (tx, rx) = tokio::sync::mpsc::channel::<eyre::Result<ExecutePlanResponse>>(1);

        let this = self.clone();

        tokio::spawn(async move {
            let execution_fut = async {
                let translator = translation::SparkAnalyzer::new(&this);
                match command.rel_type {
                    Some(RelType::ShowString(ss)) => {
                        let response = this.show_string(*ss, response_builder.clone()).await?;
                        if tx.send(Ok(response)).await.is_err() {
                            return Ok(());
                        }

                        Ok(())
                    }
                    _ => {
                        let lp = translator.to_logical_plan(command).await?;

                        let mut result_stream = this.run_query(lp).await?;

                        while let Some(result) = result_stream.next().await {
                            let result = result?;
                            let tables = result.get_tables()?;
                            for table in tables.as_slice() {
                                let response = response_builder.arrow_batch_response(table)?;
                                if tx.send(Ok(response)).await.is_err() {
                                    return Ok(());
                                }
                            }
                        }
                        Ok(())
                    }
                }
            };
            if let Err(e) = execution_fut.await {
                let _ = tx.send(Err(e)).await;
            }
        });

        let stream = ReceiverStream::new(rx);

        let stream = stream
            .map_err(|e| {
                Status::internal(
                    textwrap::wrap(&format!("Error in Daft server: {e}"), 120).join("\n"),
                )
            })
            .chain(stream::once(ready(Ok(result_complete))));

        Ok(Box::pin(stream))
    }

    pub async fn execute_write_operation(
        &self,
        operation: WriteOperation,
        operation_id: String,
    ) -> Result<ExecuteStream, Status> {
        use futures::StreamExt;

        let response_builder = ResponseBuilder::new_with_op_id(
            self.client_side_session_id(),
            self.server_side_session_id(),
            operation_id,
        );

        let finished = response_builder.result_complete_response();

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
                debug!("Ignoring sort_column_names: {sort_column_names:?} (not yet implemented)");
            }

            if !partitioning_columns.is_empty() {
                // todo(completeness): implement partitioning
                debug!(
                    "Ignoring partitioning_columns: {partitioning_columns:?} (not yet implemented)"
                );
            }

            if let Some(bucket_by) = bucket_by {
                // todo(completeness): implement bucketing
                debug!("Ignoring bucket_by: {bucket_by:?} (not yet implemented)");
            }

            if !options.is_empty() {
                // todo(completeness): implement options
                debug!("Ignoring options: {options:?} (not yet implemented)");
            }

            if !clustering_columns.is_empty() {
                // todo(completeness): implement clustering
                debug!("Ignoring clustering_columns: {clustering_columns:?} (not yet implemented)");
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

        let result = result.map_err(|e| {
            Status::internal(textwrap::wrap(&format!("Error in Daft server: {e}"), 120).join("\n"))
        });

        let future = result.and_then(|()| ready(Ok(finished)));
        let stream = futures::stream::once(future);

        Ok(Box::pin(stream))
    }

    async fn show_string(
        &self,
        show_string: ShowString,
        response_builder: ResponseBuilder,
    ) -> eyre::Result<ExecutePlanResponse> {
        let translator = translation::SparkAnalyzer::new(self);

        let ShowString {
            input,
            num_rows,
            truncate: _,
            vertical,
        } = show_string;

        if vertical {
            bail!("Vertical show string is not supported");
        }

        let Some(input) = input else {
            bail!("input must be set");
        };

        let plan = Box::pin(translator.to_logical_plan(*input)).await?;
        let plan = plan.limit(num_rows as i64, true)?;

        let results = translator.session.run_query(plan).await?;
        let results = results.try_collect::<Vec<_>>().await?;
        let single_batch = results
            .into_iter()
            .next()
            .ok_or_else(|| eyre::eyre!("No results"))?;

        let tbls = single_batch.get_tables()?;
        let tbl = Table::concat(&tbls)?;
        let output = tbl.to_comfy_table(None).to_string();

        let s = LiteralValue::Utf8(output)
            .into_single_value_series()?
            .rename("show_string");

        let tbl = Table::from_nonempty_columns(vec![s])?;
        let response = response_builder.arrow_batch_response(&tbl)?;
        Ok(response)
    }
}
