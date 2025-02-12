use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Clone, PartialEq, Eq, Parser)]
struct Cli {
    /// Also have this process serve the (pre-compiled) dashboard HTML files.
    #[arg(long, short)]
    static_assets_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    daft_dashboard_server::run(cli.static_assets_path.as_deref()).await;
}
