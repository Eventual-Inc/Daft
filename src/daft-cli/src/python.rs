use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
};

use clap::{Args, Parser, Subcommand, arg};
use pyo3::prelude::*;
use tracing_subscriber::{self, filter::Directive, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Args)]
struct DashboardArgs {
    /// The address to launch the dashboard on
    #[arg(short, long, default_value = "0.0.0.0")]
    addr: IpAddr,
    #[arg(short, long, default_value_t = 80)]
    /// The port to launch the dashboard on
    port: u16,
    #[arg(short, long, default_value_t = false)]
    /// Log HTTP requests and responses from server
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the Daft dashboard server
    Dashboard(DashboardArgs),
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

// ---------------- Run CLI Commands ---------------- //

fn run_dashboard(py: Python, args: DashboardArgs) {
    println!("🚀 Launching the Daft Dashboard!");

    let filter = Directive::from_str(if args.verbose { "INFO" } else { "ERROR" })
        .expect("Failed to parse tracing filter");

    if args.addr.is_unspecified() {
        println!("{}", console::style(format!(
            "⚠️  Listening on all network interfaces ({})! This is not recommended in production.",
            args.addr
        )).yellow().bold());
    }

    let socket_addr = SocketAddr::from((args.addr, args.port));

    // Set the subscriber for the detached run
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(filter)
                .from_env_lossy(),
        )
        .with(tracing_subscriber::fmt::layer())
        .try_init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    runtime.spawn(async move {
        println!(
            "{}  To get started, run your Daft script with env `{}`",
            console::style("█").magenta(),
            console::style(format!(
                "DAFT_DASHBOARD_URL=\"http://{}\" python ...",
                socket_addr
            ))
            .bold(),
        );
        println!(
            "✨ View the dashboard at {}. Press Ctrl+C to shutdown",
            console::style(format!("http://{}", socket_addr))
                .bold()
                .magenta()
                .underlined(),
        );
        daft_dashboard::launch_server(
            args.addr,
            args.port,
            async move { shutdown_rx.await.unwrap() },
        )
        .await
        .expect("Failed to launch dashboard server");
    });

    loop {
        if py.check_signals().is_err() {
            println!("👋 Thanks for using Daft Dashboard! Shutting down...");
            shutdown_tx
                .send(())
                .expect("Failed to shutdown Daft Dashboard");
            return;
        }
        // Necessary to allow other threads to acquire the GIL
        // Such as for Python array deserialization
        py.detach(|| {
            std::thread::sleep(std::time::Duration::from_millis(100));
        });
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
