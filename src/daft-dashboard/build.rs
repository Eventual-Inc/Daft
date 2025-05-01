fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::env::var("OUT_DIR")?;

    // always set the env var so that the include_dir! macro doesn't panic
    println!("cargo:rustc-env=DASHBOARD_ASSETS_DIR={}", out_dir);

    let frontend_dir = std::env::var("CARGO_MANIFEST_DIR")? + "/frontend/out";

    if !std::path::Path::new(&frontend_dir).is_dir() {
        if cfg!(debug_assertions) {
            println!("Dashboard assets not found in {frontend_dir}, skipping dashboard build.");
            println!("To build dashboard assets: `bun run build` in src/daft-dashboard/frontend.");
            return Ok(());
        } else {
            panic!("Dashboard assets are required for release builds");
        }
    }

    // if there's anything in the output directory, remove it
    if std::fs::exists(&out_dir)? {
        std::fs::remove_dir_all(&out_dir)?;
    }

    // move the frontend assets to the output directory
    std::fs::rename(frontend_dir, out_dir)?;
    Ok(())
}
