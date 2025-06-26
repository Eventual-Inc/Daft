// allows rustc to export symbols for dynamic linking from benchmarks
fn main() {
    println!("cargo:rustc-link-arg-benches=-rdynamic");
    println!("cargo:rerun-if-changed=build.rs");
}
