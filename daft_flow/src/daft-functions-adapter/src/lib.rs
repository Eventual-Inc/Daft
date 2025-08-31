mod error;
mod scalar_udf_utils;
mod series_iter;

pub use error::{FunctionsAdapterError, FunctionsAdapterResult};
use scalar_udf_utils::ScalarUDFCaller;
use series_iter::SeriesIterExt;

pub fn example_uri_download(url: &str) -> Option<Vec<u8>> {
    let op = daft_functions_uri::download::UrlDownload {};
    op.call_single(url.to_string())
}

/// Example function demonstrating custom error handling
pub fn demonstrate_error_handling() -> FunctionsAdapterResult<()> {
    use daft_core::{datatypes::Int32Array, series::IntoSeries};

    // Create a valid int32 series
    let field = daft_core::datatypes::Field::new("test", daft_core::datatypes::DataType::Int32);
    let int_array =
        Int32Array::from_regular_iter(field, vec![Some(1), None, Some(3)].into_iter()).unwrap();
    let int_series = int_array.into_series();

    // This should work
    let iter = int_series._iter_i32()?;
    let values: Vec<Option<i32>> = iter.collect();
    println!("Successfully iterated over int32 series: {:?}", values);

    // Create a string series
    let string_array = daft_core::datatypes::Utf8Array::from_iter(
        "test",
        vec![Some("hello"), Some("world")].into_iter(),
    );
    let string_series = string_array.into_series();

    // This should return a TypeMismatch error
    match string_series._iter_i32() {
        Ok(_) => {
            return Err(FunctionsAdapterError::IteratorError {
                message: "Expected error but got success".to_string(),
            });
        }
        Err(FunctionsAdapterError::TypeMismatch { expected, actual }) => {
            println!(
                "Correctly caught type mismatch: expected {}, got {}",
                expected, actual
            );
        }
        Err(e) => {
            return Err(FunctionsAdapterError::IteratorError {
                message: format!("Unexpected error type: {:?}", e),
            });
        }
    }

    Ok(())
}
