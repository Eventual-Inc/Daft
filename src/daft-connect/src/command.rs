// Stream of Result<ExecutePlanResponse, Status>

use std::{collections::HashMap, sync::Arc, thread};

use anyhow::bail;
use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use daft_local_execution::run::run_local;
use daft_micropartition::MicroPartition;
use daft_plan::LogicalPlanRef;
use daft_table::Table;
use tonic::Status;
use uuid::Uuid;

use crate::{
    convert::to_logical_plan,
    spark_connect::{
        execute_plan_response::{ArrowBatch, ResponseType, ResultComplete},
        spark_connect_service_server::SparkConnectService,
        ExecutePlanResponse, Relation, WriteOperation,
    },
    DaftSparkConnectService, Session,
};

type DaftStream = <DaftSparkConnectService as SparkConnectService>::ExecutePlanStream;

impl Session {
    pub async fn handle_root_command(
        &self,
        command: Relation,
        operation_id: String,
    ) -> Result<DaftStream, Status> {
        let data = thread::spawn(move || {
            let logical_plan = to_logical_plan(command).unwrap().build();

            let result = execute_plan(logical_plan);
            process_result(result)
        });
        let data = tokio::task::spawn_blocking(move || data.join().unwrap())
            .await
            .unwrap();
        let response = create_response(&self.id, &self.server_side_session_id, &operation_id, data);
        let result = create_stream(
            response,
            &self.id,
            &self.server_side_session_id,
            &operation_id,
        );

        Ok(result)
    }

    pub fn write_operation(&self, operation: WriteOperation) -> Result<DaftStream, Status> {
        println!("write_operation {:#?}", operation);
        Err(Status::unimplemented(
            "write_operation operation is not yet implemented",
        ))
    }
}

pub fn execute_plan(
    logical_plan: LogicalPlanRef,
) -> impl Iterator<Item = DaftResult<Arc<MicroPartition>>> {
    let physical_plan = daft_physical_plan::translate(&logical_plan).unwrap();

    let cfg = Arc::new(DaftExecutionConfig::default());
    let psets = HashMap::new();
    run_local(&physical_plan, psets, cfg, None).unwrap()
}

fn process_result(result: impl Iterator<Item = DaftResult<Arc<MicroPartition>>>) -> Vec<u8> {
    let mut data = Vec::new();
    let options = arrow2::io::ipc::write::WriteOptions { compression: None };
    let mut writer = arrow2::io::ipc::write::StreamWriter::new(&mut data, options);

    for elem in result {
        let elem = elem.unwrap();
        let tables = elem.get_tables().unwrap();
        let tables = vec![tables.first().unwrap()];

        for table in tables {
            write_table_to_arrow(&mut writer, table);
        }
    }

    data
}

fn write_table_to_arrow(
    writer: &mut arrow2::io::ipc::write::StreamWriter<&mut Vec<u8>>,
    table: &Table,
) {
    let schema = table.schema.to_arrow().unwrap();
    writer.start(&schema, None).unwrap();

    let arrays = table.get_inner_arrow_arrays();
    let chunk = arrow2::chunk::Chunk::new(arrays);
    writer.write(&chunk, None).unwrap();
}

fn create_response(
    session_id: &str,
    server_side_session_id: &str,
    operation_id: &str,
    data: Vec<u8>,
) -> ExecutePlanResponse {
    let response_type = ResponseType::ArrowBatch(ArrowBatch {
        row_count: 10i64,
        data,
        start_offset: None,
    });

    ExecutePlanResponse {
        session_id: session_id.to_string(),
        server_side_session_id: server_side_session_id.to_string(),
        operation_id: operation_id.to_string(),
        response_id: Uuid::new_v4().to_string(),
        metrics: None,
        observed_metrics: vec![],
        schema: None,
        response_type: Some(response_type),
    }
}

fn create_stream(
    response: ExecutePlanResponse,
    session_id: &str,
    server_side_session_id: &str,
    operation_id: &str,
) -> DaftStream {
    let stream = futures::stream::iter(vec![
        Ok(response),
        Ok(ExecutePlanResponse {
            session_id: session_id.to_string(),
            server_side_session_id: server_side_session_id.to_string(),
            operation_id: operation_id.to_string(),
            response_id: Uuid::new_v4().to_string(),
            metrics: None,
            observed_metrics: vec![],
            schema: None,
            response_type: Some(ResponseType::ResultComplete(ResultComplete {})),
        }),
    ]);
    Box::pin(stream)
}
