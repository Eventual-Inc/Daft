// Stream of Result<ExecutePlanResponse, Status>

use crate::{DaftSparkConnectService, Session};
use crate::spark_connect::spark_connect_service_server::SparkConnectService;
use crate::spark_connect::WriteOperation;

type DaftStream = <DaftSparkConnectService as SparkConnectService>::ExecutePlanStream;

impl Session {
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