//
// !! PLEASE ENSURE THE MODULE STRUCTURE FOLLOWS PROTO DIR !!
//

pub mod tron {
    tonic::include_proto!("tron");
}

pub mod schema {
    tonic::include_proto!("schema");
}
