#![cfg_attr(feature = "python", allow(unused))]

use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Clone, PartialEq, Eq, Parser)]
struct Cli {
    /// Have this server also serve the (pre-compiled) dashboard HTML files available at this path.
    #[arg(long, short)]
    static_assets_path: Option<PathBuf>,
}

#[tokio::main]
#[cfg(not(feature = "python"))]
async fn main() {
    let cli = Cli::parse();
    daft_dashboard::launch(cli.static_assets_path.as_deref()).await;
}

#[cfg(feature = "python")]
fn main() {
    unreachable!()
}
