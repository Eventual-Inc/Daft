use std::{ops::ControlFlow, thread};

use arrow2::io::ipc::write::StreamWriter;
use common_file_formats::FileFormat;
use daft_table::Table;
use eyre::Context;
use futures::TryStreamExt;
use spark_connect::{
    execute_plan_response::{ArrowBatch, ResponseType, ResultComplete},
    spark_connect_service_server::SparkConnectService,
    write_operation::{SaveMode, SaveType},
    ExecutePlanResponse, Relation, WriteOperation,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Status;
use uuid::Uuid;

use crate::{
    convert::{convert_data, run_local, to_logical_plan},
    invalid_argument_err, unimplemented_err, DaftSparkConnectService, Session,
};

type DaftStream = <DaftSparkConnectService as SparkConnectService>::ExecutePlanStream;

struct ExecutablePlanChannel {
    session_id: String,
    server_side_session_id: String,
    operation_id: String,
    tx: tokio::sync::mpsc::UnboundedSender<eyre::Result<ExecutePlanResponse>>,
}

pub trait ConcreteDataChannel {
    fn send_table(&mut self, table: &Table) -> eyre::Result<()>;
}

impl ConcreteDataChannel for ExecutablePlanChannel {
    fn send_table(&mut self, table: &Table) -> eyre::Result<()> {
        let mut data = Vec::new();

        let mut writer = StreamWriter::new(
            &mut data,
            arrow2::io::ipc::write::WriteOptions { compression: None },
        );

        let row_count = table.num_rows();

        let schema = table
            .schema
            .to_arrow()
            .wrap_err("Failed to convert Daft schema to Arrow schema")?;

        writer
            .start(&schema, None)
            .wrap_err("Failed to start Arrow stream writer with schema")?;

        let arrays = table.get_inner_arrow_arrays().collect();
        let chunk = arrow2::chunk::Chunk::new(arrays);

        writer
            .write(&chunk, None)
            .wrap_err("Failed to write Arrow chunk to stream writer")?;

        let response = ExecutePlanResponse {
            session_id: self.session_id.to_string(),
            server_side_session_id: self.server_side_session_id.to_string(),
            operation_id: self.operation_id.to_string(),
            response_id: Uuid::new_v4().to_string(), // todo: implement this
            metrics: None,                           // todo: implement this
            observed_metrics: vec![],
            schema: None,
            response_type: Some(ResponseType::ArrowBatch(ArrowBatch {
                row_count: row_count as i64,
                data,
                start_offset: None,
            })),
        };

        self.tx
            .send(Ok(response))
            .wrap_err("Error sending response to client")?;

        Ok(())
    }
}

impl Session {
    pub async fn handle_root_command(
        &self,
        command: Relation,
        operation_id: String,
    ) -> Result<DaftStream, Status> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let mut channel = ExecutablePlanChannel {
            session_id: self.client_side_session_id().to_string(),
            server_side_session_id: self.server_side_session_id().to_string(),
            operation_id: operation_id.clone(),
            tx: tx.clone(),
        };

        thread::spawn({
            let session_id = self.client_side_session_id().to_string();
            let server_side_session_id = self.server_side_session_id().to_string();
            move || {
                let result = convert_data(command, &mut channel);

                if let Err(e) = result {
                    tx.send(Err(e)).unwrap();
                } else {
                    let finished = ExecutePlanResponse {
                        session_id,
                        server_side_session_id,
                        operation_id: operation_id.to_string(),
                        response_id: Uuid::new_v4().to_string(),
                        metrics: None,
                        observed_metrics: vec![],
                        schema: None,
                        response_type: Some(ResponseType::ResultComplete(ResultComplete {})),
                    };

                    tx.send(Ok(finished)).unwrap();
                }
            }
        });

        let recv_stream =
            UnboundedReceiverStream::new(rx).map_err(|e| Status::internal(e.to_string()));

        Ok(Box::pin(recv_stream))
    }

    pub fn handle_write_operation(
        &self,
        operation: WriteOperation,
        operation_id: String,
    ) -> Result<DaftStream, Status> {
        let mode = operation.mode();

        let WriteOperation {
            input,
            source,
            sort_column_names,
            partitioning_columns,
            bucket_by,
            options,
            clustering_columns,
            save_type,
            mode: _,
        } = operation;

        let Some(input) = input else {
            return invalid_argument_err!("input is None");
        };

        let source = source.unwrap_or_else(|| "parquet".to_string());
        if source != "parquet" {
            return unimplemented_err!(
                "Only writing parquet is supported for now but got {source}"
            );
        }

        match mode {
            SaveMode::Unspecified => {}
            SaveMode::Append => {
                return unimplemented_err!("Append mode is not yet supported");
            }
            SaveMode::Overwrite => {
                return unimplemented_err!("Overwrite mode is not yet supported");
            }
            SaveMode::ErrorIfExists => {
                return unimplemented_err!("ErrorIfExists mode is not yet supported");
            }
            SaveMode::Ignore => {
                return unimplemented_err!("Ignore mode is not yet supported");
            }
        }

        if !sort_column_names.is_empty() {
            return unimplemented_err!("Sort by columns is not yet supported");
        }

        if !partitioning_columns.is_empty() {
            return unimplemented_err!("Partitioning columns is not yet supported");
        }

        if bucket_by.is_some() {
            return unimplemented_err!("Bucket by columns is not yet supported");
        }

        if !options.is_empty() {
            return unimplemented_err!("Options are not yet supported");
        }

        if !clustering_columns.is_empty() {
            return unimplemented_err!("Clustering columns is not yet supported");
        }
        let Some(save_type) = save_type else {
            return invalid_argument_err!("save_type is required");
        };

        let save_path = match save_type {
            SaveType::Path(path) => path,
            SaveType::Table(_) => {
                return unimplemented_err!("Save type table is not yet supported");
            }
        };

        thread::scope(|scope| {
            let res = scope.spawn(|| {
                let plan = to_logical_plan(input)
                    .map_err(|_| Status::internal("Failed to convert to logical plan"))?;

                // todo: assuming this is parquet
                // todo: is save_path right?
                let plan = plan
                    .table_write(&save_path, FileFormat::Parquet, None, None, None)
                    .map_err(|_| Status::internal("Failed to write table"))?;

                let plan = plan.build();

                run_local(
                    &plan,
                    |_table| ControlFlow::Continue(()),
                    || ControlFlow::Break(()),
                )
                .map_err(|e| Status::internal(format!("Failed to write table: {e}")))?;

                Result::<(), Status>::Ok(())
            });

            res.join().unwrap()
        })?;

        let session_id = self.client_side_session_id().to_string();
        let server_side_session_id = self.server_side_session_id().to_string();

        Ok(Box::pin(futures::stream::once(async {
            Ok(ExecutePlanResponse {
                session_id,
                server_side_session_id,
                operation_id,
                response_id: "abcxyz".to_string(),
                metrics: None,
                observed_metrics: vec![],
                schema: None,
                response_type: Some(ResponseType::ResultComplete(ResultComplete {})),
            })
        })))
    }
}
