use clap::{Parser, Subcommand};
use pyo3::prelude::*;

use crate::dashboard::{DashboardArgs, run as run_dashboard};

#[derive(Subcommand)]
enum Commands {
    /// Control the running of Daft dashboard server
    Dashboard(DashboardArgs),
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
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
