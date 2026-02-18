use std::{
    fs,
    fs::File,
    io,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    process,
    process::{Command, Stdio},
    str::FromStr,
    sync::OnceLock,
};

use clap::{Args, Subcommand};
use pyo3::prelude::*;
use tracing_subscriber::{self, filter::Directive, layer::SubscriberExt, util::SubscriberInitExt};

const ENV_DAFT_DASHBOARD_PID_DIR: &str = "DAFT_DASHBOARD_PID_DIR";
const ENV_DAFT_DASHBOARD_LOG_DIR: &str = "DAFT_DASHBOARD_LOG_DIR";

/// Dashboard command: holds the start/stop subcommand.
#[derive(Args)]
pub(crate) struct DashboardArgs {
    #[command(subcommand)]
    command: DashboardCommand,
}

#[derive(Subcommand)]
enum DashboardCommand {
    /// Start the dashboard server
    Start(StartArgs),
    /// Stop a currently running background dashboard server
    Stop(StopArgs),
}

#[derive(Args)]
struct StartArgs {
    /// The address to launch the dashboard on
    #[arg(short, long, default_value = "0.0.0.0")]
    pub addr: IpAddr,
    /// The port to launch the dashboard on
    #[arg(short, long, default_value_t = daft_dashboard::DEFAULT_SERVER_PORT)]
    pub port: u16,
    /// Log HTTP requests and responses from server
    #[arg(short, long)]
    pub verbose: bool,
    /// Run the dashboard in daemon mode
    #[arg(short, long)]
    pub daemon: bool,
    /// The directory used to store the PID file
    #[arg(long)]
    pub pid_dir: Option<String>,
    /// The directory used to store the LOG file
    #[arg(long)]
    pub log_dir: Option<String>,
}

#[derive(Args)]
struct StopArgs {
    /// The directory used to store the PID file
    #[arg(long)]
    pub pid_dir: Option<String>,
}

pub fn run(py: Python, args: DashboardArgs) {
    match args.command {
        DashboardCommand::Start(opts) => start(py, opts),
        DashboardCommand::Stop(opts) => stop(opts),
    }
}

fn start(py: Python, opts: StartArgs) {
    let pid_path = get_pid_filepath(opts.pid_dir.clone());
    if pid_path.exists() {
        println!(
            "⚠️ PID file '{}' already exists. Is a daft dashboard instance already running?",
            pid_path.display()
        );
        return;
    }

    if opts.daemon {
        #[cfg(unix)]
        {
            let sys = py
                .import(pyo3::intern!(py, "sys"))
                .expect("Failed to import sys");
            let exec: String = sys
                .getattr(pyo3::intern!(py, "executable"))
                .expect("Failed to access sys.executable")
                .extract()
                .expect("sys.executable is not a string");

            let mut cmd = Command::new(exec);
            cmd.arg("-m")
                .arg("daft.cli")
                .arg("dashboard")
                .arg("start")
                .arg("--addr")
                .arg(opts.addr.to_string())
                .arg("--port")
                .arg(opts.port.to_string());

            if opts.verbose {
                cmd.arg("--verbose");
            }

            if let Some(dir) = opts.pid_dir {
                cmd.arg("--pid-dir").arg(&dir);
            }

            if let Some(dir) = opts.log_dir.clone() {
                cmd.arg("--log-dir").arg(&dir);
            }

            let log_path = get_log_filepath(opts.log_dir);
            let log_file = File::create(log_path)
                .unwrap_or_else(|_| panic!("Failed to create log file: {}", log_path.display()));
            cmd.stdin(Stdio::null())
                .stdout(Stdio::from(log_file.try_clone().unwrap()))
                .stderr(Stdio::from(log_file));

            match cmd.spawn() {
                Ok(_) => {
                    println!("🚀 Launched the Daft dashboard in daemon mode!",);
                    println!("The log path is '{}'", log_path.display());
                }
                Err(e) => {
                    eprintln!(
                        "{}",
                        console::style(format!(
                            "❌ Failed to launch Daft Dashboard in daemon mode: {e}"
                        ))
                        .red()
                        .bold()
                    );
                }
            }
            return;
        }
        #[cfg(not(unix))]
        {
            println!(
                "{}",
                console::style("⚠️  Daemon mode isn't supported on this platform. Starting Daft Dashboard in the foreground instead.")
                    .yellow()
                    .bold()
            );
        }
    }

    println!("🚀 Launching the Daft dashboard!");

    let filter = Directive::from_str(if opts.verbose { "INFO" } else { "ERROR" })
        .expect("Failed to parse tracing filter");

    let addr = opts.addr;
    let port = opts.port;

    print_addr_warning(addr);

    let socket_addr = build_socket_addr(addr, port);

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
        daft_dashboard::launch_server(addr, port, async move { shutdown_rx.await.unwrap() })
            .await
            .expect("Failed to launch dashboard server");
    });

    let pid = process::id();
    if fs::write(pid_path, pid.to_string()).is_err() {
        eprintln!(
            "⚠️ Failed to write PID {} to file '{}'",
            pid,
            pid_path.display()
        );
    }

    loop {
        if py.check_signals().is_err() {
            println!("👋 Thanks for using daft dashboard! Shutting down...");
            shutdown_tx
                .send(())
                .expect("Failed to shutdown daft dashboard");
            clear_pid_file_if_matches(pid, opts.pid_dir);
            return;
        }
        py.detach(|| {
            std::thread::sleep(std::time::Duration::from_millis(100));
        });
    }
}

fn stop(opts: StopArgs) {
    let pid_path = get_pid_filepath(opts.pid_dir.clone());
    let pid = match read_pid(opts.pid_dir) {
        Ok(pid) => pid,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            println!(
                "Daft dashboard PID file '{}' not found. Is the dashboard running?",
                pid_path.display()
            );
            return;
        }
        Err(err) => {
            eprintln!(
                "Failed to read daft dashboard PID from file '{}': {err}",
                pid_path.display()
            );
            return;
        }
    };

    match send_sigint(pid) {
        Ok(()) => {
            println!("Sent stop signal to daft dashboard (PID {}).", pid);
            let _ = fs::remove_file(pid_path);
        }
        Err(err) => {
            #[cfg(unix)]
            {
                if matches!(err.raw_os_error(), Some(code) if code == libc::ESRCH) {
                    println!(
                        "No process with PID {} found. Removing stale daft dashboard PID file '{}'",
                        pid,
                        pid_path.display()
                    );
                    let _ = fs::remove_file(pid_path);
                    return;
                }
            }
            eprintln!("Failed to stop daft dashboard (PID {}): {}", pid, err);
        }
    }
}

static PID_FILEPATH: OnceLock<PathBuf> = OnceLock::new();

fn get_pid_filepath(pid_dir: Option<String>) -> &'static PathBuf {
    PID_FILEPATH.get_or_init(|| {
        let pid_dir = pid_dir
            .map(PathBuf::from)
            .or_else(|| std::env::var_os(ENV_DAFT_DASHBOARD_PID_DIR).map(PathBuf::from))
            .unwrap_or_else(std::env::temp_dir);

        if let Err(e) = fs::create_dir_all(&pid_dir) {
            eprintln!("Failed to create PID dir '{}': {e}", pid_dir.display());
        }

        pid_dir.join("daft_dashboard.pid")
    })
}

static LOG_FILEPATH: OnceLock<PathBuf> = OnceLock::new();

fn get_log_filepath(log_dir: Option<String>) -> &'static PathBuf {
    LOG_FILEPATH.get_or_init(|| {
        let log_dir = log_dir
            .map(PathBuf::from)
            .or_else(|| std::env::var_os(ENV_DAFT_DASHBOARD_LOG_DIR).map(PathBuf::from))
            .unwrap_or_else(std::env::temp_dir);

        if let Err(e) = fs::create_dir_all(&log_dir) {
            eprintln!("Failed to create LOG dir '{}': {e}", log_dir.display());
        }

        log_dir.join("daft_dashboard.log")
    })
}

fn clear_pid_file_if_matches(pid: u32, pid_dir: Option<String>) {
    let path = get_pid_filepath(pid_dir.clone());
    match read_pid(pid_dir) {
        Ok(current_pid) if current_pid == pid => match fs::remove_file(path) {
            Ok(_) => {}
            Err(e) if e.kind() != io::ErrorKind::NotFound => {
                eprintln!("Warning: failed to remove daft dashboard PID file: {e}");
            }
            Err(_) => {}
        },
        _ => {}
    }
}

fn print_addr_warning(addr: IpAddr) {
    if addr.is_unspecified() {
        println!(
            "{}",
            console::style(format!(
                "⚠️ Listening on all network interfaces ({})! This is not recommended in production.",
                addr
            ))
                .yellow()
                .bold()
        );
    }
}

fn build_socket_addr(addr: IpAddr, port: u16) -> SocketAddr {
    SocketAddr::from((addr, port))
}

#[cfg(unix)]
fn send_sigint(pid: u32) -> io::Result<()> {
    unsafe {
        if libc::kill(pid as libc::pid_t, libc::SIGINT) == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(not(unix))]
fn send_sigint(_pid: u32) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Other,
        "Stopping daft dashboard is not supported on this platform",
    ))
}

fn read_pid(pid_dir: Option<String>) -> io::Result<u32> {
    let path = get_pid_filepath(pid_dir);
    let contents = fs::read_to_string(path)?;
    let pid: u32 = contents
        .trim()
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(pid)
}
