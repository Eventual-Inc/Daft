// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::sync::Arc;

use async_stream::try_stream;
use futures::pin_mut;
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status, Streaming};

use arrow_format::flight::data::flight_descriptor::*;
use arrow_format::flight::data::*;
use arrow_format::flight::service::flight_service_server::*;

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::io::flight::{
    deserialize_message, deserialize_schemas, serialize_batch, serialize_schema,
    serialize_schema_to_info,
};
use arrow2::io::ipc;
use arrow2::io::ipc::read::Dictionaries;

use super::{Result, TonicStream};

pub async fn scenario_setup(port: u16) -> Result {
    let addr = super::listen_on(port).await?;

    let service = Service {
        server_location: format!("grpc+tcp://{}", addr),
        ..Default::default()
    };
    let svc = FlightServiceServer::new(service);

    let server = Server::builder().add_service(svc).serve(addr);

    // NOTE: Log output used in tests to signal server is ready
    println!("Server listening on localhost:{}", addr.port());
    server.await?;
    Ok(())
}

#[derive(Debug, Clone)]
struct IntegrationDataset {
    schema: Schema,
    ipc_schema: ipc::IpcSchema,
    chunks: Vec<Chunk<Box<dyn Array>>>,
}

#[derive(Clone, Default)]
struct Service {
    server_location: String,
    uploaded_chunks: Arc<Mutex<HashMap<String, IntegrationDataset>>>,
}

impl Service {
    fn endpoint_from_path(&self, path: &str) -> FlightEndpoint {
        super::endpoint(path, &self.server_location)
    }
}

#[tonic::async_trait]
impl FlightService for Service {
    type HandshakeStream = TonicStream<Result<HandshakeResponse, Status>>;
    type ListFlightsStream = TonicStream<Result<FlightInfo, Status>>;
    type DoGetStream = TonicStream<Result<FlightData, Status>>;
    type DoPutStream = TonicStream<Result<PutResult, Status>>;
    type DoActionStream = TonicStream<Result<arrow_format::flight::data::Result, Status>>;
    type ListActionsStream = TonicStream<Result<ActionType, Status>>;
    type DoExchangeStream = TonicStream<Result<FlightData, Status>>;

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();

        let key = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid ticket: {:?}", e)))?;

        let uploaded_chunks = self.uploaded_chunks.lock().await;

        let flight = uploaded_chunks
            .get(&key)
            .ok_or_else(|| Status::not_found(format!("Could not find flight. {}", key)))?;

        let options = ipc::write::WriteOptions { compression: None };

        let schema = serialize_schema(&flight.schema, Some(&flight.ipc_schema.fields));

        let batches = flight
            .chunks
            .iter()
            .enumerate()
            .flat_map(|(counter, batch)| {
                let (dictionaries, mut chunk) =
                    serialize_batch(batch, &flight.ipc_schema.fields, &options).unwrap();

                // Only the record batch's FlightData gets app_metadata
                let metadata = counter.to_string().into_bytes();
                chunk.app_metadata = metadata;

                dictionaries
                    .into_iter()
                    .chain(std::iter::once(chunk))
                    .map(Ok)
            });

        let output = futures::stream::iter(
            std::iter::once(Ok(schema))
                .chain(batches)
                .collect::<Vec<_>>(),
        );

        Ok(Response::new(Box::pin(output) as Self::DoGetStream))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();

        match descriptor.r#type {
            t if t == DescriptorType::Path as i32 => {
                let path = &descriptor.path;
                if path.is_empty() {
                    return Err(Status::invalid_argument("Invalid path"));
                }

                let uploaded_chunks = self.uploaded_chunks.lock().await;
                let flight = uploaded_chunks.get(&path[0]).ok_or_else(|| {
                    Status::not_found(format!("Could not find flight. {}", path[0]))
                })?;

                let endpoint = self.endpoint_from_path(&path[0]);

                let total_records: usize = flight.chunks.iter().map(|chunk| chunk.len()).sum();

                let schema =
                    serialize_schema_to_info(&flight.schema, Some(&flight.ipc_schema.fields))
                        .expect(
                            "Could not generate schema bytes from schema stored by a DoPut; \
                         this should be impossible",
                        );

                let info = FlightInfo {
                    schema,
                    flight_descriptor: Some(descriptor.clone()),
                    endpoint: vec![endpoint],
                    total_records: total_records as i64,
                    total_bytes: -1,
                };

                Ok(Response::new(info))
            }
            other => Err(Status::unimplemented(format!("Request type: {}", other))),
        }
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut input_stream = request.into_inner();
        let flight_data = input_stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("Must send some FlightData"))?;

        let descriptor = flight_data
            .flight_descriptor
            .clone()
            .ok_or_else(|| Status::invalid_argument("Must have a descriptor"))?;

        if descriptor.r#type != DescriptorType::Path as i32 || descriptor.path.is_empty() {
            return Err(Status::invalid_argument("Must specify a path"));
        }

        let key = descriptor.path[0].clone();

        let (schema, ipc_schema) = deserialize_schemas(&flight_data.data_header)
            .map_err(|e| Status::invalid_argument(format!("Invalid schema: {:?}", e)))?;

        let uploaded_chunks = self.uploaded_chunks.clone();

        let mut dictionaries = Dictionaries::default();
        let mut chunks = vec![];

        let stream = try_stream! {
            pin_mut!(input_stream);
            for await item in input_stream {
                let data = item.map_err(|_| Status::invalid_argument(format!("Invalid")))?;
                let maybe_chunk = deserialize_message(&data, &schema.fields,
                    &ipc_schema,
                    &mut dictionaries).map_err(|_| Status::invalid_argument(format!("Invalid")))?;
                if let Some(chunk) = maybe_chunk {
                    chunks.push(chunk)
                }
                yield PutResult {app_metadata: data.app_metadata}
            }
            let dataset = IntegrationDataset {
                schema,
                chunks,
                ipc_schema,
            };
            let mut uploaded_chunks = uploaded_chunks.lock().await;
            uploaded_chunks.insert(key, dataset);
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
}
