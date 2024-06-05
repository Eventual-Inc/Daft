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

//! Common code used in the integration test binaries

use arrow2::array::Array;
use arrow2::io::ipc::IpcField;
use serde_json::Value;

use arrow2::chunk::Chunk;
use arrow2::AHashMap;
use arrow2::datatypes::*;
use arrow2::error::Result;
use arrow2::io::json_integration::{read, ArrowJsonBatch, ArrowJsonDictionaryBatch};

use std::fs::File;
use std::io::BufReader;

/// The expected username for the basic auth integration test.
pub const AUTH_USERNAME: &str = "arrow";
/// The expected password for the basic auth integration test.
pub const AUTH_PASSWORD: &str = "flight";

pub mod flight_client_scenarios;
pub mod flight_server_scenarios;

pub struct ArrowFile {
    pub schema: Schema,
    pub fields: Vec<IpcField>,
    // we can evolve this into a concrete Arrow type
    // this is temporarily not being read from
    pub _dictionaries: AHashMap<i64, ArrowJsonDictionaryBatch>,
    pub chunks: Vec<Chunk<Box<dyn Array>>>,
}

pub fn read_json_file(json_name: &str) -> Result<ArrowFile> {
    let json_file = File::open(json_name)?;
    let reader = BufReader::new(json_file);
    let arrow_json: Value = serde_json::from_reader(reader).unwrap();

    let (schema, fields) = read::deserialize_schema(&arrow_json["schema"])?;
    // read dictionaries
    let mut dictionaries = AHashMap::new();
    if let Some(dicts) = arrow_json.get("dictionaries") {
        for d in dicts
            .as_array()
            .expect("Unable to get dictionaries as array")
        {
            let json_dict: ArrowJsonDictionaryBatch =
                serde_json::from_value(d.clone()).expect("Unable to get dictionary from JSON");
            // TODO: convert to a concrete Arrow type
            dictionaries.insert(json_dict.id, json_dict);
        }
    }

    let chunks = arrow_json["batches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| {
            let json_batch: ArrowJsonBatch = serde_json::from_value(b.clone()).unwrap();
            read::deserialize_chunk(&schema, &fields, &json_batch, &dictionaries)
        })
        .collect::<Result<_>>()?;
    Ok(ArrowFile {
        schema,
        fields,
        _dictionaries: dictionaries,
        chunks,
    })
}
