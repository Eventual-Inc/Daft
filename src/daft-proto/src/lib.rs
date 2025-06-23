//
// !! PLEASE ENSURE THE MODULE STRUCTURE FOLLOWS THE PROTO DIR !!
//

#[allow(
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::doc_markdown,
    clippy::all,
    clippy::pedantic
)]
pub mod protos {
    pub mod daft {
        pub mod v1 {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/generated/daft.v1.rs"
            ));
        }
    }
}
