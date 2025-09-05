use clap::{Parser, Subcommand};
#[cfg(feature = "python")]
use pyo3::types::PyModule;
#[cfg(feature = "python")]
use pyo3::{pyfunction, Bound, PyResult, Python};
use tokio::sync::oneshot;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the Daft dashboard server
    Dashboard,
}

async fn launch_dashboard(shutdown_rx: oneshot::Receiver<()>) -> Result<(), std::io::Error> {
    println!("🚀 Launching Daft Dashboard!");

    let listener = daft_dashboard::make_listener().await?;
    println!(
        "✨ View the Daft Dashboard at http://{}",
        listener.local_addr().unwrap()
    );
    println!("🔥 Press Ctrl+C to shutdown");

    daft_dashboard::launch_server(listener, async { shutdown_rx.await.unwrap() }).await;
    Ok(())
}

#[cfg(feature = "python")]
#[pyfunction]
fn cli(py: Python, args: Vec<String>) -> PyResult<()> {
    let cli = Cli::parse_from(args);
    match cli.command {
        Commands::Dashboard => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let handle = rt.spawn(launch_dashboard(shutdown_rx));

            loop {
                if py.check_signals().is_err() {
                    println!("👋 Thanks for using Daft Dashboard! Shutting down...");
                    shutdown_tx.send(()).unwrap();
                    rt.block_on(handle).unwrap()?;
                    break;
                }
            }
            Ok(())
        }
    }
}

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    use pyo3::{types::PyModuleMethods, wrap_pyfunction};

    parent.add_wrapped(wrap_pyfunction!(cli))?;
    Ok(())
}
