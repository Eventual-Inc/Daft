//
// !! PLEASE ENSURE THE MODULE STRUCTURE FOLLOWS PROTO DIR !!
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
            // for build.rs gen sources
            // tonic::include_proto!("daft.v1");
            // for checked in gen sources
            include!(concat!(env!("CARGO_MANIFEST_DIR"), "/gen/daft.v1.rs"));
        }
    }
}
