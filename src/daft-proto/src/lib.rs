//
// rust sources
//

mod client;

//
// protobuf sources
//

#[allow(
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::doc_markdown,
    clippy::all,
    clippy::pedantic
)]
pub mod echo {
    tonic::include_proto!("echo");
}

//
// python sources
//

#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "python")]
pub use python::register_modules;
