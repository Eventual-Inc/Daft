use std::{
    env,
    path::{Path, PathBuf},
};

/// The build.rs error type.
type Error = Box<dyn std::error::Error>;

/// The build.rs result type.
type Result<T> = std::result::Result<T, Error>;

/// Returns a PathBuf to `proto` dir in the crate root.
fn get_proto_dir() -> Result<PathBuf> {
    let cargo_manifest_dir = env::var("CARGO_MANIFEST_DIR")?;
    let crate_dir = PathBuf::from(cargo_manifest_dir);
    let crate_dir_proto = crate_dir.join("proto");
    Ok(crate_dir_proto)
}

/// Returns a list of `.proto` files in the `proto` dir.
fn get_proto_defs<P: AsRef<Path>>(proto_dir: P) -> Result<Vec<PathBuf>> {
    let mut proto_defs = vec![];
    for entry in walkdir::WalkDir::new(&proto_dir) {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().is_some_and(|ext| ext == "proto") {
            println!("cargo::rerun-if-changed={}", path.to_str().unwrap());
            proto_defs.push(path.to_path_buf());
        }
    }
    Ok(proto_defs)
}

fn main() -> Result<()> {
    // 1. locate proto dir
    let proto_dir = get_proto_dir()?;
    // 2. collect .proto files
    let proto_defs = get_proto_defs(&proto_dir)?;
    // 3. generate code with tonic (and prost)
    tonic_build::configure()
        .build_client(false)
        .build_server(false)
        .compile_protos(proto_defs.as_slice(), &[proto_dir])?;
    Ok(())
}
