use std::process::Command;

fn ci_main(out_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let frontend_dir = std::env::var("CARGO_MANIFEST_DIR")? + "/frontend/out";

    if !std::path::Path::new(&frontend_dir).is_dir() {
        if cfg!(debug_assertions) || std::env::var("DAFT_DASHBOARD_SKIP_BUILD").is_ok() {
            println!("Dashboard assets not found in {frontend_dir}, skipping dashboard build.");
            println!("To build dashboard assets: `bun run build` in src/daft-dashboard/frontend.");
            return Ok(());
        } else {
            panic!("Dashboard assets are required for release builds");
        }
    }

    // if there's anything in the output directory, remove it
    if std::fs::exists(out_dir)? {
        std::fs::remove_dir_all(out_dir)?;
    }

    // move the frontend assets to the output directory
    std::fs::rename(frontend_dir, out_dir)?;
    Ok(())
}

fn default_main(out_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    if cfg!(debug_assertions) && std::env::var("DAFT_DASHBOARD_SKIP_BUILD").is_ok() {
        println!(
            "cargo:warning=Running in debug mode and DAFT_DASHBOARD_SKIP_BUILD is set, skipping dashboard build."
        );
        return Ok(());
    }

    println!("cargo:rerun-if-changed=frontend/src/");
    println!("cargo:rerun-if-changed=frontend/bun.lock");
    println!("cargo:rerun-if-changed=build.rs");

    // Check if bun is installed
    let bun_available = Command::new("bun")
        .arg("--version")
        .output()
        .map(|_| true)
        .unwrap_or(false);

    // if bun is not available, we can't build the frontend assets
    // so we just print a warning and return
    // but if we're in release mode, we panic
    if !bun_available {
        if cfg!(debug_assertions) {
            println!("cargo:warning=Bun not found, skipping dashboard frontend assets");
            return Ok(());
        } else {
            panic!("Bun is required for release builds");
        }
    }

    // Install dependencies
    let install_status = Command::new("bun")
        .current_dir("./frontend")
        .args(["install", "--frozen-lockfile"])
        .status()?;

    if cfg!(debug_assertions) {
        if !install_status.success() {
            println!("cargo:warning=Failed to install frontend dependencies");
        }
    } else {
        assert!(install_status.success(), "Failed to install dependencies");
    }

    // Run `bun run build`
    let mut cmd = Command::new("bun");
    let status = cmd.current_dir("./frontend");

    let status = if cfg!(debug_assertions) {
        status.args(["run", "build", "--no-mangling"])
    } else {
        status.args(["run", "build"])
    };
    let status = status.status()?;

    if cfg!(debug_assertions) {
        if !status.success() {
            println!("cargo:warning=Failed to build frontend assets");
        }
    } else {
        assert!(status.success(), "Failed to build frontend assets");
    }

    let frontend_dir = std::env::var("CARGO_MANIFEST_DIR")? + "/frontend/out";

    // if there's anything in the output directory, remove it
    if std::fs::metadata(out_dir).is_ok() {
        std::fs::remove_dir_all(out_dir)?;
    }

    // move the frontend assets to the output directory
    std::fs::rename(frontend_dir, out_dir)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::env::var("OUT_DIR")?;
    // always set the env var so that the include_dir! macro doesn't panic
    println!("cargo:rustc-env=DASHBOARD_ASSETS_DIR={}", out_dir);

    let is_ci = std::env::var("CI").is_ok() || std::env::var("GITHUB_ACTIONS").is_ok();
    if is_ci {
        ci_main(&out_dir)
    } else {
        default_main(&out_dir)
    }
}
