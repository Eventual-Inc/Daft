use clap::{Args, Parser, Subcommand, arg};
use pyo3::prelude::*;

#[derive(Debug, Args)]
struct DashboardArgs {
    #[arg(short, long, default_value_t = 80)]
    /// The port to launch the dashboard on
    port: u16,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the Daft dashboard server
    Dashboard(DashboardArgs),
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

// ---------------- Run CLI Commands ---------------- //

fn run_dashboard(py: Python, args: DashboardArgs) {
    println!("🚀 Launching the Daft Dashboard!");

    let port = args.port;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    runtime.spawn(async move {
        daft_dashboard::launch_server(port, async move { shutdown_rx.await.unwrap() })
            .await
            .unwrap();
    });

    println!(
        "✨ View the Daft Dashboard at http://{}:{}",
        daft_dashboard::DEFAULT_SERVER_ADDR,
        port
    );

    loop {
        if py.check_signals().is_err() {
            println!("👋 Thanks for using Daft Dashboard! Shutting down...");
            shutdown_tx
                .send(())
                .expect("Failed to shutdown Daft Dashboard");
            break;
        }
    }
}

#[pyfunction]
pub fn cli(py: Python, args: Vec<String>) {
    let cli = Cli::parse_from(args);
    match cli.command {
        Commands::Dashboard(args) => run_dashboard(py, args),
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_wrapped(wrap_pyfunction!(cli))?;
    Ok(())
}
