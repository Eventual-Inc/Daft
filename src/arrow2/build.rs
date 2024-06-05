use rustc_version::{version_meta, Channel};

fn main() {
    if version_meta().unwrap().channel == Channel::Nightly {
        println!("cargo:rustc-cfg=nightly_build");
    }
}
