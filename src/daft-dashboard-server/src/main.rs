use clap::Parser;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Parser)]
struct Cli {
    /// Also have this process serve the (pre-compiled) dashboard HTML files.
    #[arg(long, short)]
    serve_html: bool,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    daft_dashboard_server::run(cli.serve_html).await;
}
