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
            tonic::include_proto!("daft.v1");
        }
    }
}
