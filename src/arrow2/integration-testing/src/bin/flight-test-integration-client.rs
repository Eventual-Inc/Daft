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

use arrow_integration_testing::flight_client_scenarios;
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
#[clap(author, version, about("rust flight-test-integration-client"), long_about = None)]
struct Args {
    #[clap(long, help = "host of flight server")]
    host: String,
    #[clap(long, help = "port of flight server")]
    port: u16,
    #[clap(
        short,
        long,
        help = "path to the descriptor file, only used when scenario is not provided"
    )]
    path: Option<String>,
    #[clap(long, arg_enum)]
    scenario: Option<Scenario>,
}

#[tokio::main]
async fn main() -> Result {
    #[cfg(feature = "logging")]
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let host = args.host;
    let port = args.port;

    match args.scenario {
        Some(Scenario::Middleware) => {
            flight_client_scenarios::middleware::run_scenario(&host, port).await?
        }
        Some(Scenario::AuthBasicProto) => {
            flight_client_scenarios::auth_basic_proto::run_scenario(&host, port).await?
        }
        None => {
            let path = args.path.expect("No path is given");
            flight_client_scenarios::integration_test::run_scenario(&host, port, &path)
                .await?;
        }
    }

    Ok(())
}
