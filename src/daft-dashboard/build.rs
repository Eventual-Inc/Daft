use std::{io, path::Path, process::Command};

/// Recursively copy the contents of `src` into `dst`, resolving symlinks.
///
/// `Path::is_dir` follows symlinks, where `DirEntry::file_type` does not. That
/// matters because `std::fs::copy` also follows them, and refuses a directory:
/// testing the entry type directly would send a symlinked directory down the
/// copy branch and fail.
fn copy_dir_all(src: &Path, dst: &Path) -> io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let dst = dst.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir_all(&entry.path(), &dst)?;
        } else {
            std::fs::copy(entry.path(), &dst)?;
        }
    }
    Ok(())
}

/// Move `src` to `dst`, falling back to a recursive copy across filesystems.
///
/// `OUT_DIR` lives under the cargo target directory, which is routinely on a
/// different filesystem from the source tree: `CARGO_TARGET_DIR` pointed at a
/// separate mount or tmpfs, a container with the source bind-mounted and the
/// target on a volume, a CI cache mount, or a distinct btrfs subvolume. In all
/// of those cases `rename(2)` fails with `EXDEV` rather than moving the
/// directory, so fall back to copy-then-remove.
fn move_dir(src: &Path, dst: &Path) -> io::Result<()> {
    match std::fs::rename(src, dst) {
        Err(e) if e.kind() == io::ErrorKind::CrossesDevices => {
            copy_dir_all(src, dst)?;
            std::fs::remove_dir_all(src)
        }
        result => result,
    }
}

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
    move_dir(Path::new(&frontend_dir), Path::new(out_dir))?;
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

    if cfg!(debug_assertions) {
        if !install_status.success() {
            println!("cargo:warning=Failed to install frontend dependencies");
        }
    } else {
        assert!(install_status.success(), "Failed to install dependencies");
    }

    // Run `npm run build`
    let status = Command::new("npm")
        .current_dir("./frontend")
        .args(["run", "build"])
        .status()?;

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
    move_dir(Path::new(&frontend_dir), Path::new(out_dir))?;
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
