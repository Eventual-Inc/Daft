//
// !! PLEASE ENSURE THE MODULE STRUCTURE FOLLOWS PROTO DIR !!
//

pub mod echo {
    tonic::include_proto!("echo");
}

pub mod schema {
    tonic::include_proto!("schema");
}
