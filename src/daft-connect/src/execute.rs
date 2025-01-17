use std::{future::ready, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_file_formats::FileFormat;
use daft_dsl::LiteralValue;
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::MicroPartition;
use daft_ray_execution::RayEngine;
use daft_table::Table;
use eyre::{bail, Context};
use futures::{
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use pyo3::Python;
use spark_connect::{
    relation::RelType,
    write_operation::{SaveMode, SaveType},
    CreateDataFrameViewCommand, ExecutePlanResponse, Relation, ShowString, SqlCommand,
    WriteOperation,
};
use tonic::{codegen::tokio_stream::wrappers::ReceiverStream, Status};
use tracing::debug;

use crate::{
    not_yet_implemented, response_builder::ResponseBuilder, session::Session,
    spark_analyzer::SparkAnalyzer, util::FromOptionalField, ExecuteStream, Runner,
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
                let runner_address = self.config_values().get("daft.runner.ray.address");
                let runner_address = runner_address.map(|s| s.to_string());

                let runner = RayEngine::try_new(runner_address, None, None)?;
                let result_set = tokio::task::spawn_blocking(move || {
                    Python::with_gil(|py| runner.run_iter_impl(py, lp, None))
                })
                .await??;

                Ok(Box::pin(stream::iter(result_set)))
            }

            Runner::Native => {
                let this = self.clone();

                let plan = lp.optimize()?;

                let cfg = Arc::new(DaftExecutionConfig::default());

                let results = this.engine.run(&plan, &*this.psets, cfg, None)?;
                Ok(results.into_stream().boxed())
            }
        }
    }

    pub async fn execute_command(
        &self,
        command: Relation,
        res: ResponseBuilder<ExecutePlanResponse>,
    ) -> Result<ExecuteStream, Status> {
        use futures::{StreamExt, TryStreamExt};

        let result_complete = res.result_complete_response();

        let (tx, rx) = tokio::sync::mpsc::channel::<eyre::Result<ExecutePlanResponse>>(1);

        let this = self.clone();
        self.compute_runtime.runtime.spawn(async move {
            let execution_fut = async {
                let translator = SparkAnalyzer::new(&this);
                match command.rel_type {
                    Some(RelType::ShowString(ss)) => {
                        let response = this.show_string(*ss, res.clone()).await?;
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
                                let response = res.arrow_batch_response(table)?;
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
        res: ResponseBuilder<ExecutePlanResponse>,
    ) -> Result<ExecuteStream, Status> {
        fn check_write_operation(write_op: &WriteOperation) -> Result<(), Status> {
            if !write_op.sort_column_names.is_empty() {
                return not_yet_implemented!("Sort with column names");
            }
            if !write_op.partitioning_columns.is_empty() {
                return not_yet_implemented!("Partitioning with column names");
            }
            if !write_op.clustering_columns.is_empty() {
                return not_yet_implemented!("Clustering with column names");
            }

            if let Some(bucket_by) = &write_op.bucket_by {
                return not_yet_implemented!("Bucketing by: {:?}", bucket_by);
            }

            if !write_op.options.is_empty() {
                // todo(completeness): implement options
                debug!(
                    "Ignoring options: {:?} (not yet implemented)",
                    write_op.options
                );
            }

            let mode = SaveMode::try_from(write_op.mode)
                .map_err(|_| Status::internal("invalid write mode"))?;

            if mode == SaveMode::Unspecified {
                Ok(())
            } else {
                not_yet_implemented!("save mode: {}", mode.as_str_name())
            }
        }

        let finished = res.result_complete_response();

        let (tx, rx) = tokio::sync::mpsc::channel::<eyre::Result<ExecutePlanResponse>>(1);

        let this = self.clone();

        self.compute_runtime.runtime.spawn(async move {
            let result = async {
                check_write_operation(&operation)?;

                let WriteOperation {
                    input,
                    source,
                    save_type,
                    ..
                } = operation;

                let input = input.required("input")?;
                let source = source.required("source")?;

                let file_format: FileFormat = source.parse()?;

                let Some(save_type) = save_type else {
                    bail!("Save type is required");
                };

                let path = match save_type {
                    SaveType::Path(path) => path,
                    SaveType::Table(_) => {
                        return not_yet_implemented!("write to table").map_err(|e| e.into())
                    }
                };

                let translator = SparkAnalyzer::new(&this);

                let plan = translator.to_logical_plan(input).await?;

                let plan = plan.table_write(&path, file_format, None, None, None)?;

                let mut result_stream = this.run_query(plan).await?;

                // this is so we make sure the operation is actually done
                // before we return
                //
                // an example where this is important is if we write to a parquet file
                // and then read immediately after, we need to wait for the write to finish
                while let Some(_result) = result_stream.next().await {}

                Ok(())
            };

            if let Err(e) = result.await {
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
            .chain(stream::once(ready(Ok(finished))));

        Ok(Box::pin(stream))
    }

    pub async fn execute_create_dataframe_view(
        &self,
        create_dataframe: CreateDataFrameViewCommand,
        rb: ResponseBuilder<ExecutePlanResponse>,
    ) -> Result<ExecuteStream, Status> {
        let CreateDataFrameViewCommand {
            input,
            name,
            is_global,
            replace,
        } = create_dataframe;

        if is_global {
            return not_yet_implemented!("Global dataframe view");
        }

        let input = input.required("input")?;
        let input = SparkAnalyzer::new(self)
            .to_logical_plan(input)
            .await
            .map_err(|e| {
                Status::internal(
                    textwrap::wrap(&format!("Error in Daft server: {e}"), 120).join("\n"),
                )
            })?;

        {
            let catalog = self.catalog.read().unwrap();
            if !replace && catalog.contains_table(&name) {
                return Err(Status::internal("Dataframe view already exists"));
            }
        }

        let mut catalog = self.catalog.write().unwrap();

        catalog.register_table(&name, input).map_err(|e| {
            Status::internal(textwrap::wrap(&format!("Error in Daft server: {e}"), 120).join("\n"))
        })?;

        let response = rb.result_complete_response();
        let stream = stream::once(ready(Ok(response)));
        Ok(Box::pin(stream))
    }

    #[allow(deprecated)]
    pub async fn execute_sql_command(
        &self,
        SqlCommand {
            sql,
            args,
            pos_args,
            named_arguments,
            pos_arguments,
            input,
        }: SqlCommand,
        res: ResponseBuilder<ExecutePlanResponse>,
    ) -> Result<ExecuteStream, Status> {
        if !args.is_empty() {
            return not_yet_implemented!("Named arguments");
        }
        if !pos_args.is_empty() {
            return not_yet_implemented!("Positional arguments");
        }
        if !named_arguments.is_empty() {
            return not_yet_implemented!("Named arguments");
        }
        if !pos_arguments.is_empty() {
            return not_yet_implemented!("Positional arguments");
        }

        if input.is_some() {
            return not_yet_implemented!("Input");
        }

        let catalog = self.catalog.read().unwrap();
        let catalog = catalog.clone();

        let mut planner = daft_sql::SQLPlanner::new(catalog);

        let plan = planner
            .plan_sql(&sql)
            .wrap_err("Error planning SQL")
            .map_err(|e| {
                Status::internal(
                    textwrap::wrap(&format!("Error in Daft server: {e}"), 120).join("\n"),
                )
            })?;

        let plan = LogicalPlanBuilder::from(plan);

        // TODO: code duplication
        let result_complete = res.result_complete_response();

        let (tx, rx) = tokio::sync::mpsc::channel::<eyre::Result<ExecutePlanResponse>>(1);

        let this = self.clone();

        tokio::spawn(async move {
            let execution_fut = async {
                let mut result_stream = this.run_query(plan).await?;
                while let Some(result) = result_stream.next().await {
                    let result = result?;
                    let tables = result.get_tables()?;
                    for table in tables.as_slice() {
                        let response = res.arrow_batch_response(table)?;
                        if tx.send(Ok(response)).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                Ok(())
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

    async fn show_string(
        &self,
        show_string: ShowString,
        response_builder: ResponseBuilder<ExecutePlanResponse>,
    ) -> eyre::Result<ExecutePlanResponse> {
        let translator = SparkAnalyzer::new(self);

        let ShowString {
            input,
            num_rows,
            truncate: _,
            vertical,
        } = show_string;

        if vertical {
            bail!("Vertical show string is not supported");
        }

        let input = input.required("input")?;

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
        response_builder.arrow_batch_response(&tbl)
    }
}
