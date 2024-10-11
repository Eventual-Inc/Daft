// Stream of Result<ExecutePlanResponse, Status>

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use uuid::Uuid;
use common_daft_config::DaftExecutionConfig;
use common_display::{DisplayAs, DisplayLevel};
use common_error::DaftResult;
use daft_core::datatypes::{DataType, Field, Utf8Array};
use daft_core::prelude::Schema;
use daft_core::series::IntoSeries;
use daft_plan::{logical_to_physical, LogicalPlanBuilder, LogicalPlanRef, ParquetScanBuilder};
use crate::{DaftSparkConnectService, Session};
use crate::spark_connect::spark_connect_service_server::SparkConnectService;
use crate::spark_connect::{ExecutePlanResponse, Relation, WriteOperation};
use daft_dsl::col;
use daft_local_execution::run::run_local;
use daft_table::Table;
use crate::spark_connect::execute_plan_response::{ArrowBatch, ResponseType, ResultComplete};

type DaftStream = <DaftSparkConnectService as SparkConnectService>::ExecutePlanStream;

fn create_logical_plan() -> DaftResult<LogicalPlanRef> {
    // Step 1: Create a table scan for the Parquet file
    let scan_builder = ParquetScanBuilder::new("/Users/andrewgazelka/Projects/Daft/tests/assets/parquet-data/mvp.parquet")
        .finish()?;

    // Step 2: Build the logical plan
    let plan = scan_builder
        // Step 3: Apply a limit operation to show only 20 rows
        .limit(20, false)?
        // Step 4: Apply a project operation to handle truncation (if needed)
        // Note: This step might need to be adjusted based on the actual schema
        .select(scan_builder.schema().fields.iter().map(|(name, _)| col(name.as_str())).collect())?
        .build();

    Ok(plan)
}

impl Session {
    pub async fn handle_root_command(&self,
                                     command: Relation,
                                     operation_id: String,
    ) -> DaftStream {

        // plan Root(
        //     Relation {
        //         common: Some(
        //             RelationCommon {
        //                 source_info: "",
        //                 plan_id: Some(
        //                     2,
        //                 ),
        //                 origin: None,
        //             },
        //         ),
        //         rel_type: Some(
        //             ShowString(
        //                 ShowString {
        //                     input: Some(
        //                         Relation {
        //                             common: Some(
        //                                 RelationCommon {
        //                                     source_info: "",
        //                                     plan_id: Some(
        //                                         1,
        //                                     ),
        //                                     origin: None,
        //                                 },
        //                             ),
        //                             rel_type: Some(
        //                                 Read(
        //                                     Read {
        //                                         is_streaming: false,
        //                                         read_type: Some(
        //                                             DataSource(
        //                                                 DataSource {
        //                                                     format: Some(
        //                                                         "parquet",
        //                                                     ),
        //                                                     schema: Some(
        //                                                         "",
        //                                                     ),
        //                                                     options: {},
        //                                                     paths: [
        //                                                         "myparquet.parquet",
        //                                                     ],
        //                                                     predicates: [],
        //                                                 },
        //                                             ),
        //                                         ),
        //                                     },
        //                                 ),
        //                             ),
        //                         },
        //                     ),
        //                     num_rows: 20,
        //                     truncate: 20,
        //                     vertical: false,
        //                 },
        //             ),
        //         ),
        //     },
        // )

        let handle = std::thread::spawn(|| {
            let logical_plan = create_logical_plan().unwrap();
            let cfg = Arc::new(DaftExecutionConfig::default());
            // let physical_plan = logical_to_physical(logical_plan, cfg).unwrap();
            let physical_plan = daft_physical_plan::translate(&logical_plan).unwrap();

            let cfg = Arc::new(DaftExecutionConfig::default());
            let psets = HashMap::new(); // This should be populated with actual partition sets if needed
            let result = run_local(&physical_plan, psets, cfg, None).unwrap();

            let mut data = Vec::new();

            let options = arrow2::io::ipc::write::WriteOptions { compression: None };
            let mut writer = arrow2::io::ipc::write::StreamWriter::new(&mut data, options);

            for elem in result {
                let elem = elem.unwrap();

                let tables = elem.get_tables().unwrap();
                let tables = vec![tables.get(0).unwrap()];

                for table in tables {
                    println!("table ... {table:?}");

                    let before_schema = &table.schema;
                    let display = before_schema.display_as(DisplayLevel::Default);

                    let mut utf8_data = vec![None; 10];
                    utf8_data[0] = Some(display.to_string());
                    let utf8_array = arrow2::array::Utf8Array::<i64>::from(utf8_data).boxed();

                    let field = Field::new("show_string", DataType::Utf8);

                    let series = Utf8Array::try_from((Arc::new(field), utf8_array)).unwrap().into_series();


                    let len = series.len();

                    let jank = Table::new_with_size(
                        Schema::new(vec![series.field().clone()]).unwrap(),
                        vec![series],
                        len,
                    ).unwrap();

                    let table = table.union(&jank).unwrap();

                    let schema = table.schema.to_arrow().unwrap();
                    writer.start(&schema, None).unwrap();


                    let arrays = table.get_inner_arrow_arrays();
                    let chunk = arrow2::chunk::Chunk::new(arrays);
                    writer.write(&chunk, None).unwrap();
                }
            }

            data
        });

        let blocking = tokio::task::spawn_blocking(|| handle.join());

        let data = blocking.await.unwrap().unwrap();

        let response_type = ResponseType::ArrowBatch(ArrowBatch {
            row_count: 10i64, // todo: sure
            data,
            start_offset: None, // todo: mmmmmmm yea idk
        });

        let response = ExecutePlanResponse {
            session_id: self.session_id.clone(),
            server_side_session_id: self.server_side_session_id.clone(),
            operation_id: operation_id.clone(),
            response_id: Uuid::new_v4().to_string(), // todo
            metrics: None,
            observed_metrics: vec![],
            schema: None,
            response_type: Some(response_type),
        };

        // Create a stream with the response and an end-of-stream message
        let stream = futures::stream::iter(vec![
            Ok(response),
            Ok(ExecutePlanResponse {
                session_id: self.session_id.clone(),
                server_side_session_id: self.server_side_session_id.clone(),
                operation_id: operation_id.clone(),
                response_id: Uuid::new_v4().to_string(),
                metrics: None,
                observed_metrics: vec![],
                schema: None,
                response_type: Some(ResponseType::ResultComplete(ResultComplete {})),
            }),
        ]);
        Box::pin(stream)
    }


    pub fn write_operation(&self, operation: WriteOperation) -> DaftStream {
        println!("write_operation {:#?}", operation);

        // write_operation WriteOperation {
        //     input: Some(
        //         Relation {
        //             common: Some(
        //                 RelationCommon {
        //                     source_info: "",
        //                     plan_id: Some(
        //                         0,
        //                     ),
        //                     origin: None,
        //                 },
        //             ),
        //             rel_type: Some(
        //                 Range(
        //                     Range {
        //                         start: Some(
        //                             0,
        //                         ),
        //                         end: 5,
        //                         step: 1,
        //                         num_partitions: None,
        //                     },
        //                 ),
        //             ),
        //         },
        //     ),
        //     source: Some(
        //         "parquet",
        //     ),
        //     mode: Overwrite,
        //     sort_column_names: [],
        //     partitioning_columns: [],
        //     bucket_by: None,
        //     options: {},
        //     clustering_columns: [],
        //     save_type: Some(
        //         Path(
        //             "myparquet.parquet",
        //         ),
        //     ),
        // }

        todo!()
    }
}