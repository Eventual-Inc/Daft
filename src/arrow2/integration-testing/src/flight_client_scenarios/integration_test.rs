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

use crate::{read_json_file, ArrowFile};

use arrow2::{
    array::Array,
    chunk::Chunk,
    datatypes::*,
    io::{
        flight::{self, deserialize_batch, serialize_batch},
        ipc::{read::Dictionaries, write, IpcField, IpcSchema},
    },
};
use arrow_format::flight::data::{
    flight_descriptor::DescriptorType, FlightData, FlightDescriptor, Location, Ticket,
};
use arrow_format::flight::service::flight_service_client::FlightServiceClient;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use tonic::{Request, Streaming};

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

type Client = FlightServiceClient<tonic::transport::Channel>;

type ChunkBox = Chunk<Box<dyn Array>>;

pub async fn run_scenario(host: &str, port: u16, path: &str) -> Result {
    let url = format!("http://{}:{}", host, port);

    let client = FlightServiceClient::connect(url).await?;

    let ArrowFile {
        schema,
        chunks,
        fields,
        ..
    } = read_json_file(path)?;

    let ipc_schema = IpcSchema {
        fields,
        is_little_endian: true,
    };

    let mut descriptor = FlightDescriptor::default();
    descriptor.set_type(DescriptorType::Path);
    descriptor.path = vec![path.to_string()];

    upload_data(
        client.clone(),
        &schema,
        ipc_schema.fields.clone(),
        descriptor.clone(),
        chunks.clone(),
    )
    .await?;
    verify_data(client, descriptor, &schema, &ipc_schema, &chunks).await?;

    Ok(())
}

async fn upload_data(
    mut client: Client,
    schema: &Schema,
    fields: Vec<IpcField>,
    descriptor: FlightDescriptor,
    chunks: Vec<ChunkBox>,
) -> Result {
    let stream = new_stream(schema, fields, descriptor, chunks);

    // put the stream in the client
    let responses = client.do_put(Request::new(stream)).await?.into_inner();

    // confirm that all chunks were received in the right order
    let results = responses.try_collect::<Vec<_>>().await?;
    assert!(results
        .into_iter()
        // only record batches have a metadata; ignore dictionary batches
        .filter(|r| !r.app_metadata.is_empty())
        .enumerate()
        .all(|(counter, r)| r.app_metadata == counter.to_string().as_bytes()));

    Ok(())
}

fn new_stream(
    schema: &Schema,
    fields: Vec<IpcField>,
    descriptor: FlightDescriptor,
    chunks: Vec<ChunkBox>,
) -> BoxStream<'static, FlightData> {
    let options = write::WriteOptions { compression: None };

    let mut schema = flight::serialize_schema(schema, Some(&fields));
    schema.flight_descriptor = Some(descriptor);

    // iterator of [dictionaries0, chunk0, dictionaries1, chunk1, ...]
    let iter = chunks
        .into_iter()
        .enumerate()
        .flat_map(move |(counter, chunk)| {
            let metadata = counter.to_string().into_bytes();
            let (mut dictionaries, mut chunk) = serialize_batch(&chunk, &fields, &options).unwrap();

            // assign `app_metadata` to chunks
            chunk.app_metadata = metadata.to_vec();
            dictionaries.push(chunk);
            dictionaries
        });

    // the stream as per flight spec: the schema followed by stream of chunks
    futures::stream::once(futures::future::ready(schema))
        .chain(futures::stream::iter(iter))
        .boxed()
}

async fn verify_data(
    mut client: Client,
    descriptor: FlightDescriptor,
    expected_schema: &Schema,
    ipc_schema: &IpcSchema,
    expected_chunks: &[ChunkBox],
) -> Result {
    let resp = client.get_flight_info(Request::new(descriptor)).await?;
    let info = resp.into_inner();

    assert!(
        !info.endpoint.is_empty(),
        "No endpoints returned from Flight server",
    );
    for endpoint in info.endpoint {
        let ticket = endpoint
            .ticket
            .expect("No ticket returned from Flight server");

        assert!(
            !endpoint.location.is_empty(),
            "No locations returned from Flight server",
        );
        for location in endpoint.location {
            consume_flight_location(
                location,
                ticket.clone(),
                expected_chunks,
                expected_schema,
                ipc_schema,
            )
            .await?;
        }
    }

    Ok(())
}

async fn consume_flight_location(
    location: Location,
    ticket: Ticket,
    expected_chunks: &[ChunkBox],
    schema: &Schema,
    ipc_schema: &IpcSchema,
) -> Result {
    let mut location = location;
    // The other Flight implementations use the `grpc+tcp` scheme, but the Rust http libs
    // don't recognize this as valid.
    location.uri = location.uri.replace("grpc+tcp://", "http://");

    let mut client = FlightServiceClient::connect(location.uri).await?;
    let resp = client.do_get(ticket).await?;
    let mut stream = resp.into_inner();

    // We already have the schema from the FlightInfo, but the server sends it again as the
    // first FlightData. Ignore this one.
    let _schema_again = stream.next().await.unwrap();

    let mut dictionaries = Default::default();

    for (counter, expected_chunk) in expected_chunks.iter().enumerate() {
        let data = read_dictionaries(&mut stream, &schema.fields, ipc_schema, &mut dictionaries)
            .await
            .unwrap_or_else(|| {
                panic!(
                    "Got fewer chunkes than expected, received so far: {} expected: {}",
                    counter,
                    expected_chunks.len(),
                )
            });

        let metadata = counter.to_string().into_bytes();
        assert_eq!(metadata, data.app_metadata);

        let chunk = deserialize_batch(&data, &schema.fields, ipc_schema, &dictionaries)
            .expect("Unable to convert flight data to Arrow chunk");

        assert_eq!(&chunk, expected_chunk);
    }

    assert!(
        stream.next().await.is_none(),
        "Got more chunkes than the expected: {}",
        expected_chunks.len(),
    );

    Ok(())
}

async fn read_dictionaries(
    stream: &mut Streaming<FlightData>,
    fields: &[Field],
    ipc_schema: &IpcSchema,
    dictionaries: &mut Dictionaries,
) -> Option<FlightData> {
    let mut data = stream.next().await?.ok()?;

    let existing = dictionaries.len();
    loop {
        flight::deserialize_dictionary(&data, fields, ipc_schema, dictionaries).ok()?;
        if dictionaries.len() == existing {
            break;
        }
        data = stream.next().await?.ok()?;
    }

    Some(data)
}
