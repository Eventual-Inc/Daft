#[macro_export]
macro_rules! invalid_argument_err {
    ($arg: tt) => {{
        let msg = format!($arg);
        Err(::tonic::Status::invalid_argument(msg))
    }};
}

#[macro_export]
macro_rules! unimplemented_err {
    ($arg: tt) => {{
        let msg = format!($arg);
        Err(::tonic::Status::unimplemented(msg))
    }};
}

// not found
#[macro_export]
macro_rules! not_found_err {
    ($arg: tt) => {{
        let msg = format!($arg);
        Err(::tonic::Status::not_found(msg))
    }};
}
