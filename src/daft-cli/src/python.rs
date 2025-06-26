use clap::{Parser, Subcommand};
use pyo3::prelude::*;

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

#[pyfunction]
pub fn cli(py: Python, args: Vec<String>) {
    let cli = Cli::parse_from(args);
    match cli.command {
        Commands::Dashboard => {
            println!("🚀 Launching Daft Dashboard!");
            println!(
                "✨ View the Daft Dashboard at http://{}:{}",
                daft_dashboard::SERVER_ADDR,
                daft_dashboard::SERVER_PORT
            );
            let mut handle =
                daft_dashboard::python::launch(false, py).expect("Failed to launch Daft Dashboard");
            loop {
                if py.check_signals().is_err() {
                    println!("👋 Thanks for using Daft Dashboard! Shutting down...");
                    handle
                        .shutdown(true)
                        .expect("Failed to shutdown Daft Dashboard");
                    break;
                }
            }
        }
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_wrapped(wrap_pyfunction!(cli))?;
    Ok(())
}
