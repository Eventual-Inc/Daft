use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=frontend/src/");
    println!("cargo:rerun-if-changed=frontend/bun.lockb");
    let out_dir = std::env::var("OUT_DIR")?;

    // always set the env var so that the include_dir! macro doesn't panic
    println!("cargo:rustc-env=DASHBOARD_ASSETS_DIR={}", out_dir);

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
        .args(["install"])
        .status()?;

    assert!(install_status.success(), "Failed to install dependencies");

    // Run `bun run build`
    let mut cmd = Command::new("bun");
    let status = cmd.current_dir("./frontend");

    let status = if cfg!(debug_assertions) {
        status.args(["run", "build", "--no-lint", "--no-mangling"])
    } else {
        status.args(["run", "build"])
    };
    let status = status.status()?;

    assert!(status.success(), "Failed to build frontend assets");

    let frontend_dir = std::env::var("CARGO_MANIFEST_DIR")? + "/frontend/out";

    // if there's anything in the output directory, remove it
    if std::fs::metadata(&out_dir).is_ok() {
        std::fs::remove_dir_all(&out_dir)?;
    }

    // move the frontend assets to the output directory
    std::fs::rename(frontend_dir, out_dir)?;
    assert!(status.success(), "Failed to build frontend assets");
    Ok(())
}
