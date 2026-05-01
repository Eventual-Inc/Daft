use std::process::Command;

fn ci_main(out_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let frontend_dir = std::env::var("CARGO_MANIFEST_DIR")? + "/frontend/out";

    if !std::path::Path::new(&frontend_dir).is_dir() {
        if cfg!(debug_assertions) || std::env::var("DAFT_DASHBOARD_SKIP_BUILD").is_ok() {
            println!("Dashboard assets not found in {frontend_dir}, skipping dashboard build.");
            println!("To build dashboard assets: `npm run build` in src/daft-dashboard/frontend.");
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
    println!("cargo:rerun-if-changed=frontend/package-lock.json");
    println!("cargo:rerun-if-changed=build.rs");

    // Check if npm is installed
    let npm_available = Command::new("npm")
        .arg("--version")
        .output()
        .map(|_| true)
        .unwrap_or(false);

    // if npm is not available, we can't build the frontend assets
    // so we just print a warning and return
    // but if we're in release mode, we panic
    if !npm_available {
        if cfg!(debug_assertions) {
            println!("cargo:warning=npm not found, skipping dashboard frontend assets");
            return Ok(());
        } else {
            panic!("Node/npm is required for release builds");
        }
    }

    // Install dependencies
    let install_status = Command::new("npm")
        .current_dir("./frontend")
        .args(["ci"])
        .status()?;

    if !install_status.success() {
        if cfg!(debug_assertions) {
            println!(
                "cargo:warning=Failed to install dashboard frontend dependencies; skipping (set DAFT_DASHBOARD_SKIP_BUILD=1 to silence)"
            );
            return Ok(());
        } else {
            panic!("Failed to install dependencies");
        }
    }

    // Run `next build`. If it fails, retry with DAFT_DASHBOARD_OFFLINE_FONT=1,
    // which makes next.config.ts replace next/font/google with a system-mono
    // stub. Common cause of first-attempt failure: sandboxed/offline build
    // environments where fetching Geist Mono from fonts.googleapis.com fails.
    // The retry still runs `next build` end-to-end so TS/React code gets
    // typechecked — only the served font is degraded.
    let status = Command::new("npm")
        .current_dir("./frontend")
        .args(["run", "build"])
        .status()?;

    let status = if status.success() {
        status
    } else {
        println!(
            "cargo:warning=Dashboard frontend build failed; retrying with DAFT_DASHBOARD_OFFLINE_FONT=1 (system mono fallback)"
        );
        Command::new("npm")
            .current_dir("./frontend")
            .args(["run", "build"])
            .env("DAFT_DASHBOARD_OFFLINE_FONT", "1")
            .status()?
    };

    if !status.success() {
        if cfg!(debug_assertions) {
            println!(
                "cargo:warning=Failed to build dashboard frontend assets even with offline font fallback; skipping (set DAFT_DASHBOARD_SKIP_BUILD=1 to silence)"
            );
            return Ok(());
        } else {
            panic!("Failed to build frontend assets");
        }
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
