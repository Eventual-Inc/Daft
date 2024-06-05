use arrow2::array::*;
use arrow2::datatypes::Field;
use arrow2::{error::Error, error::Result, ffi};

fn _test_round_trip(arrays: Vec<Box<dyn Array>>) -> Result<()> {
    let field = Field::new("a", arrays[0].data_type().clone(), true);
    let iter = Box::new(arrays.clone().into_iter().map(Ok)) as _;

    let mut stream = Box::new(ffi::ArrowArrayStream::empty());

    *stream = ffi::export_iterator(iter, field.clone());

    // import
    let mut stream = unsafe { ffi::ArrowArrayStreamReader::try_new(stream)? };

    let mut produced_arrays: Vec<Box<dyn Array>> = vec![];
    while let Some(array) = unsafe { stream.next() } {
        produced_arrays.push(array?);
    }

    assert_eq!(produced_arrays, arrays);
    assert_eq!(stream.field(), &field);
    Ok(())
}

#[test]
fn round_trip() -> Result<()> {
    let array = Int32Array::from(&[Some(2), None, Some(1), None]);
    let array: Box<dyn Array> = Box::new(array);

    _test_round_trip(vec![array.clone(), array.clone(), array])
}

#[test]
fn stream_reader_try_new_invalid_argument_error_on_released_stream() {
    let released_stream = Box::new(ffi::ArrowArrayStream::empty());
    let reader = unsafe { ffi::ArrowArrayStreamReader::try_new(released_stream) };
    // poor man's assert_matches:
    match reader {
        Err(Error::InvalidArgumentError(_)) => {}
        _ => panic!("ArrowArrayStreamReader::try_new did not return an InvalidArgumentError"),
    }
}
