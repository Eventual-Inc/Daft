use std::{
    fs,
    fs::File,
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    process,
    process::{Command, Stdio},
    str::FromStr,
};

use clap::{Args, Subcommand};
use daft_context::subscribers::dashboard::{
    DASHBOARD_PID_FILE_NAME, DASHBOARD_URL_FILE_NAME, DashboardStateLock, dashboard_state_dir,
    prepare_dashboard_state_dir, write_dashboard_state_file,
};
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
    addr: IpAddr,
    /// The port to launch the dashboard on
    #[arg(short, long, default_value_t = daft_dashboard::DEFAULT_SERVER_PORT)]
    port: u16,
    /// Log HTTP requests and responses from server
    #[arg(short, long)]
    verbose: bool,
    /// Run the dashboard in daemon mode
    #[arg(short, long)]
    daemon: bool,
    /// The directory used to store the PID file
    #[arg(long)]
    pid_dir: Option<String>,
    /// The directory used to store the LOG file
    #[arg(long)]
    log_dir: Option<String>,
    /// (experimental) Path to event log for importing a query into the dashboard
    #[arg(long)]
    event_log_file: Option<String>,
}

#[derive(Args)]
struct StopArgs {
    /// The directory used to store the PID file
    #[arg(long)]
    pid_dir: Option<String>,
}

pub fn run(py: Python, args: DashboardArgs) {
    match args.command {
        DashboardCommand::Start(opts) => start(py, opts),
        DashboardCommand::Stop(opts) => stop(opts),
    }
}

fn start(py: Python, opts: StartArgs) {
    let dir = match state_dir(opts.pid_dir.clone()) {
        Ok(dir) => dir,
        Err(err) => {
            eprintln!("Failed to prepare the daft dashboard state directory: {err}");
            return;
        }
    };

    if dir.join(DASHBOARD_PID_FILE_NAME).exists() {
        println!(
            "⚠️ PID file '{}' already exists. Is a daft dashboard instance already running?",
            dir.join(DASHBOARD_PID_FILE_NAME).display()
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

            if let Some(dir) = opts.pid_dir.as_deref() {
                cmd.arg("--pid-dir").arg(dir);
            }

            if let Some(dir) = opts.log_dir.as_deref() {
                cmd.arg("--log-dir").arg(dir);
            }

            if let Some(path) = opts.event_log_file.as_deref() {
                cmd.arg("--event-log-file").arg(path);
            }

            let log_path = get_log_filepath(opts.log_dir.clone());
            let log_file = File::create(log_path)
                .unwrap_or_else(|_| panic!("Failed to create log file: {}", log_path.display()));
            cmd.stdin(Stdio::null())
                .stdout(Stdio::from(log_file.try_clone().unwrap()))
                .stderr(Stdio::from(log_file));

            match cmd.spawn() {
                Ok(_) => {
                    println!("🚀 Launched the Daft dashboard in daemon mode!");
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

    let server_options = daft_dashboard::ServerOptions::new(opts.addr, opts.port)
        .with_event_log(opts.event_log_file);

    print_addr_warning(server_options.addr());

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

    // Bind before publishing discovery files so that only a live server is
    // advertised, and so a `--port 0` launch publishes the actual port.
    let listener = match runtime.block_on(tokio::net::TcpListener::bind((
        server_options.addr(),
        server_options.port(),
    ))) {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!(
                "{}",
                console::style(format!("❌ Failed to bind the Daft dashboard: {err}"))
                    .red()
                    .bold()
            );
            return;
        }
    };

    let bound_addr = match listener.local_addr() {
        Ok(addr) => addr,
        Err(err) => {
            eprintln!(
                "{}",
                console::style(format!(
                    "❌ Failed to read the Daft dashboard address: {err}"
                ))
                .red()
                .bold()
            );
            return;
        }
    };

    let dashboard_url = dashboard_client_url(bound_addr);
    let remote_url = dashboard_remote_url(bound_addr);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let (exit_tx, mut exit_rx) = tokio::sync::oneshot::channel();
    let view_url = dashboard_url.clone();
    runtime.spawn(async move {
        println!(
            "{}  Daft scripts started on this machine now connect to the dashboard automatically.",
            console::style("█").magenta(),
        );
        println!(
            "   To point a script running elsewhere at it, use `{}`",
            console::style(format!("DAFT_DASHBOARD_URL=\"{remote_url}\" python ...")).bold(),
        );
        println!(
            "✨ View the dashboard at {}. Press Ctrl+C to shutdown",
            console::style(view_url).bold().magenta().underlined(),
        );
        if let Err(err) =
            daft_dashboard::launch_server_with_listener(server_options, listener, async move {
                let _ = shutdown_rx.await;
            })
            .await
        {
            eprintln!("❌ Daft dashboard server stopped with an error: {err}");
        }
        let _ = exit_tx.send(());
    });

    let pid = process::id();
    match DashboardStateLock::acquire(&dir) {
        Ok(_lock) => {
            if let Err(e) =
                write_dashboard_state_file(&dir.join(DASHBOARD_PID_FILE_NAME), &pid.to_string())
            {
                eprintln!(
                    "⚠️ Failed to write PID {} to file '{}': {e}",
                    pid,
                    dir.join(DASHBOARD_PID_FILE_NAME).display()
                );
            }
            if let Err(e) =
                write_dashboard_state_file(&dir.join(DASHBOARD_URL_FILE_NAME), &dashboard_url)
            {
                eprintln!(
                    "⚠️ Failed to write dashboard URL to file '{}': {e}",
                    dir.join(DASHBOARD_URL_FILE_NAME).display()
                );
            }
        }
        Err(e) => {
            eprintln!(
                "⚠️ Failed to lock the daft dashboard state directory '{}': {e}. Clients may not discover this dashboard.",
                dir.display()
            );
        }
    }

    loop {
        if py.check_signals().is_err() {
            println!("👋 Thanks for using daft dashboard! Shutting down...");
            let _ = shutdown_tx.send(());
            clear_dashboard_state_if_matches(&dir, pid);
            return;
        }
        match exit_rx.try_recv() {
            Ok(()) | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                clear_dashboard_state_if_matches(&dir, pid);
                return;
            }
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
        }
        py.detach(|| {
            std::thread::sleep(std::time::Duration::from_millis(100));
        });
    }
}

fn stop(opts: StopArgs) {
    let dir = match state_dir(opts.pid_dir.clone()) {
        Ok(dir) => dir,
        Err(err) => {
            eprintln!("Failed to prepare the daft dashboard state directory: {err}");
            return;
        }
    };

    let _lock = match DashboardStateLock::acquire(&dir) {
        Ok(lock) => lock,
        Err(err) => {
            eprintln!(
                "Failed to lock the daft dashboard state directory '{}': {err}",
                dir.display()
            );
            return;
        }
    };

    let pid = match read_pid(&dir) {
        Ok(pid) => pid,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            println!(
                "Daft dashboard PID file '{}' not found. Is the dashboard running?",
                dir.join(DASHBOARD_PID_FILE_NAME).display()
            );
            return;
        }
        Err(err) => {
            eprintln!(
                "Failed to read daft dashboard PID from file '{}': {err}",
                dir.join(DASHBOARD_PID_FILE_NAME).display()
            );
            return;
        }
    };

    match send_sigint(pid) {
        Ok(()) => {
            println!("Sent stop signal to daft dashboard (PID {}).", pid);
            remove_state_files(&dir);
        }
        Err(err) => {
            #[cfg(unix)]
            {
                if matches!(err.raw_os_error(), Some(code) if code == libc::ESRCH) {
                    println!(
                        "No process with PID {} found. Removing stale daft dashboard state files in '{}'",
                        pid,
                        dir.display()
                    );
                    remove_state_files(&dir);
                    return;
                }
            }
            eprintln!("Failed to stop daft dashboard (PID {}): {}", pid, err);
        }
    }
}

/// Resolve and prepare the directory shared by the dashboard CLI and clients.
///
/// A user-provided directory is trusted as-is; the default directory is
/// per-user and must stay private so other local users cannot publish a URL
/// that redirects query reports.
fn state_dir(pid_dir: Option<String>) -> io::Result<PathBuf> {
    let env_dir = std::env::var_os(ENV_DAFT_DASHBOARD_PID_DIR).filter(|dir| !dir.is_empty());
    let explicitly_configured = pid_dir.is_some() || env_dir.is_some();
    let dir = pid_dir
        .map(PathBuf::from)
        .or_else(|| env_dir.map(PathBuf::from))
        .unwrap_or_else(dashboard_state_dir);

    prepare_dashboard_state_dir(&dir, !explicitly_configured)?;
    Ok(dir)
}

fn read_pid(dir: &Path) -> io::Result<u32> {
    let contents = fs::read_to_string(dir.join(DASHBOARD_PID_FILE_NAME))?;
    contents
        .trim()
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn remove_state_files(dir: &Path) {
    for name in [DASHBOARD_PID_FILE_NAME, DASHBOARD_URL_FILE_NAME] {
        match fs::remove_file(dir.join(name)) {
            Ok(()) => {}
            Err(e) if e.kind() != io::ErrorKind::NotFound => {
                eprintln!("Warning: failed to remove daft dashboard file '{name}': {e}");
            }
            Err(_) => {}
        }
    }
}

fn clear_dashboard_state_if_matches(dir: &Path, pid: u32) {
    let Ok(_lock) = DashboardStateLock::acquire(dir) else {
        return;
    };
    // Another dashboard may have replaced the state files in the meantime.
    if read_pid(dir).ok() != Some(pid) {
        return;
    }
    remove_state_files(dir);
}

static LOG_FILEPATH: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

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

/// URL that clients on this machine should use. A server bound to all
/// interfaces is reachable through loopback, which also renders correctly in
/// browsers.
fn dashboard_client_url(addr: SocketAddr) -> String {
    let ip = if addr.ip().is_unspecified() {
        match addr.ip() {
            IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
        }
    } else {
        addr.ip()
    };
    format!("http://{}", SocketAddr::new(ip, addr.port()))
}

/// URL to advertise for scripts running on other machines. A wildcard bind
/// address is not reachable remotely, so leave the host for the user to fill.
fn dashboard_remote_url(addr: SocketAddr) -> String {
    if addr.ip().is_unspecified() {
        format!("http://<dashboard-host>:{}", addr.port())
    } else {
        dashboard_client_url(addr)
    }
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
