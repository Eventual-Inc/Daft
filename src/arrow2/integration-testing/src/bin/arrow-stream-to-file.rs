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

use std::io;

use arrow2::error::Result;
use arrow2::io::ipc::read;
use arrow2::io::ipc::write;

fn main() -> Result<()> {
    let mut reader = io::stdin();
    let metadata = read::read_stream_metadata(&mut reader)?;
    let mut arrow_stream_reader = read::StreamReader::new(reader, metadata.clone(), None);

    let writer = io::stdout();

    let options = write::WriteOptions { compression: None };
    let mut writer = write::FileWriter::try_new(
        writer,
        metadata.schema.clone(),
        Some(metadata.ipc_schema.fields),
        options,
    )?;

    arrow_stream_reader.try_for_each(|batch| writer.write(&batch?.unwrap(), None))?;
    writer.finish()?;

    Ok(())
}
