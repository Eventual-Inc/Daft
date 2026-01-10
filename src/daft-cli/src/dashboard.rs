use std::{
    fs, io,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    process,
    process::Command,
    str::FromStr,
};

use clap::Args;
use pyo3::prelude::*;
use tracing_subscriber::{self, filter::Directive, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Args)]
pub(crate) struct DashboardArgs {
    /// The address to launch the dashboard on
    #[arg(short, long, default_value = "0.0.0.0")]
    addr: IpAddr,
    #[arg(short, long, default_value_t = daft_dashboard::DEFAULT_SERVER_PORT)]
    /// The port to launch the dashboard on
    port: u16,
    #[arg(short, long, default_value_t = false)]
    /// Log HTTP requests and responses from server
    verbose: bool,
    /// Run the dashboard in daemon mode (background)
    #[arg(short, long, default_value_t = false, conflicts_with = "stop")]
    daemon: bool,
    /// Stop the currently running dashboard server
    #[arg(short, long, default_value_t = false, conflicts_with = "daemon")]
    stop: bool,
}

struct StartOptions {
    pub addr: IpAddr,
    pub port: u16,
    pub verbose: bool,
    pub daemon: bool,
}

pub fn run(py: Python, args: DashboardArgs) {
    if args.stop {
        stop();
        return;
    }

    let opts = StartOptions {
        addr: args.addr,
        port: args.port,
        verbose: args.verbose,
        daemon: args.daemon,
    };

    start(py, opts);
}

fn clear_pid_file_if_matches(pid: u32) {
    let path = pid_filepath();
    match read_pid() {
        Ok(current_pid) if current_pid == pid => match fs::remove_file(&path) {
            Ok(_) => {}
            Err(e) if e.kind() != io::ErrorKind::NotFound => {
                eprintln!("Warning: failed to remove Daft dashboard PID file: {e}");
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
                "⚠️  Listening on all network interfaces ({})! This is not recommended in production.",
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

fn start_daemon(py: Python, opts: &StartOptions) {
    let addr = opts.addr;
    let port = opts.port;

    print_addr_warning(addr);

    let socket_addr = build_socket_addr(addr, port);

    let sys = match py.import("sys") {
        Ok(sys) => sys,
        Err(err) => {
            eprintln!("Failed to import sys module: {err}");
            return;
        }
    };

    let executable: String = match sys.getattr("executable").and_then(|obj| obj.extract()) {
        Ok(exe) => exe,
        Err(err) => {
            eprintln!("Failed to obtain Python executable: {err}");
            return;
        }
    };

    let mut command = Command::new(executable);
    command
        .arg("-m")
        .arg("daft.cli")
        .arg("dashboard")
        .arg("--addr")
        .arg(addr.to_string())
        .arg("--port")
        .arg(port.to_string());

    if opts.verbose {
        command.arg("--verbose");
    }

    match command.spawn() {
        Ok(child) => {
            let pid = child.id();
            println!("🚀 Launching the Daft dashboard in daemon mode!");
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
                "✨ View the dashboard at {} (daemon PID {})",
                console::style(format!("http://{}", socket_addr))
                    .bold()
                    .magenta()
                    .underlined(),
                pid,
            );
            println!(
                "ℹ️  Dashboard is running in the background. Use `daft dashboard --stop` to stop it."
            );
        }
        Err(err) => {
            eprintln!("Failed to start Daft dashboard in daemon mode: {err}");
        }
    }
}

fn start_foreground(py: Python, opts: StartOptions) {
    println!("🚀 Launching the Daft dashboard!");

    let filter = Directive::from_str(if opts.verbose { "INFO" } else { "ERROR" })
        .expect("Failed to parse tracing filter");

    let addr = opts.addr;
    let port = opts.port;

    print_addr_warning(addr);

    let socket_addr = build_socket_addr(addr, port);
    let pid = process::id();

    if let Err(err) = write_pid(pid) {
        eprintln!("Warning: failed to write Daft dashboard PID file: {err}");
    }

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

    loop {
        if py.check_signals().is_err() {
            println!("👋 Thanks for using Daft dashboard! Shutting down...");
            shutdown_tx
                .send(())
                .expect("Failed to shutdown Daft dashboard");
            clear_pid_file_if_matches(pid);
            return;
        }
        py.detach(|| {
            std::thread::sleep(std::time::Duration::from_millis(100));
        });
    }
}

fn start(py: Python, opts: StartOptions) {
    if opts.daemon {
        start_daemon(py, &opts);
    } else {
        start_foreground(py, opts);
    }
}

fn stop() {
    let path = pid_filepath();
    let pid = match read_pid() {
        Ok(pid) => pid,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            println!(
                "Daft dashboard PID file '{}' not found. Is the dashboard running?",
                path.display()
            );
            return;
        }
        Err(err) => {
            eprintln!(
                "Failed to read Daft dashboard PID file '{}': {err}",
                path.display()
            );
            return;
        }
    };

    match send_sigint(pid) {
        Ok(()) => {
            println!("Sent stop signal to Daft dashboard (PID {}).", pid);
            match fs::remove_file(&path) {
                Ok(_) => {}
                Err(err) if err.kind() != io::ErrorKind::NotFound => {
                    eprintln!(
                        "Warning: failed to remove Daft dashboard PID file '{}': {err}",
                        path.display()
                    );
                }
                Err(_) => {}
            }
        }
        Err(err) => {
            #[cfg(unix)]
            {
                if matches!(err.raw_os_error(), Some(code) if code == libc::ESRCH) {
                    println!(
                        "No process with PID {} found. Removing stale Daft dashboard PID file '{}'.",
                        pid,
                        path.display()
                    );
                    let _ = fs::remove_file(&path);
                    return;
                }
            }
            eprintln!("Failed to stop Daft dashboard (PID {}): {}", pid, err);
        }
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
        "Stopping Daft dashboard is not supported on this platform",
    ))
}

// FIXME by zhenchao tmp dir?
fn pid_filepath() -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push("daft_dashboard.pid");
    path
}

fn write_pid(pid: u32) -> io::Result<()> {
    let path = pid_filepath();
    fs::write(path, pid.to_string())
}

fn read_pid() -> io::Result<u32> {
    let path = pid_filepath();
    let contents = fs::read_to_string(path)?;
    let pid: u32 = contents
        .trim()
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(pid)
}
