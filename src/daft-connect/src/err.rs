#[macro_export]
macro_rules! invalid_argument_err {
    ($arg: tt) => {{
        let msg = format!($arg);
        Err(::tonic::Status::invalid_argument(msg))
    }};
}

#[macro_export]
macro_rules! nyi {
    ($($arg:tt)*)  => {{
        let msg = format!($($arg)*);
        let msg = format!(r#"Feature: {msg} is not yet implemented, please open an issue at https://github.com/Eventual-Inc/Daft/issues/new?assignees=&labels=enhancement%2Cneeds+triage&projects=&template=feature_request.yaml"#);

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
