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

use arrow_integration_testing::flight_server_scenarios;
use clap::Parser;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

#[derive(clap::ArgEnum, Debug, Clone)]
enum Scenario {
    Middleware,
    #[clap(name = "auth:basic_proto")]
    AuthBasicProto,
}

#[derive(Debug, Parser)]
#[clap(author, version, about("rust flight-test-integration-server"), long_about = None)]
struct Args {
    #[clap(long)]
    port: u16,
    #[clap(long, arg_enum)]
    scenario: Option<Scenario>,
}

#[tokio::main]
async fn main() -> Result {
    #[cfg(feature = "logging")]
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let port = args.port;

    match args.scenario {
        Some(Scenario::Middleware) => {
            flight_server_scenarios::middleware::scenario_setup(port).await?
        }
        Some(Scenario::AuthBasicProto) => {
            flight_server_scenarios::auth_basic_proto::scenario_setup(port).await?
        }
        None => {
            flight_server_scenarios::integration_test::scenario_setup(port).await?;
        }
    }
    Ok(())
}
