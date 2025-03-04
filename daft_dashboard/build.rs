use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=frontend/src");
    println!("cargo:rerun-if-changed=frontend/package.json");

    // Check if bun is installed
    let bun_available = Command::new("bun")
        .arg("--version")
        .output()
        .map(|_| true)
        .unwrap_or(false);

    if !bun_available {
        if cfg!(debug_assertions) {
            println!("cargo:warning=Bun not found, skipping dashboard frontend assets");
            return;
        } else {
            panic!("Bun is required for release builds");
        }
    }

    // Install dependencies
    let install_status = Command::new("bun")
        .current_dir("./frontend")
        .args(["install"])
        .status()
        .expect("Failed to install dependencies");

    assert!(install_status.success(), "Failed to install dependencies");

    // Run `bun run build`
    let status = Command::new("bun")
        .current_dir("./frontend")
        .args(["run", "build"])
        .status()
        .expect("Failed to build frontend assets");

    assert!(status.success(), "Failed to build frontend assets");
}
