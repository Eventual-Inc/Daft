// Utilities for easily calling Daft scalar UDFs from Rust
use daft_dsl::functions::{FunctionArg, FunctionArgs, scalar::ScalarUDF};

use crate::{SeriesIterExt, error::FunctionsAdapterError};

pub trait ScalarUDFCaller<T, R> {
    fn call_batch<'a, I>(
        &self,
        values: I,
    ) -> Result<Box<dyn Iterator<Item = Option<R>>>, FunctionsAdapterError>
    where
        T: 'a,
        I: IntoIterator<Item = Option<&'a T>>;
    fn call_single(&self, value: T) -> Option<R>;
}

impl<T: ScalarUDF> ScalarUDFCaller<String, Vec<u8>> for T {
    fn call_batch<'a, I>(
        &self,
        values: I,
    ) -> Result<Box<dyn Iterator<Item = Option<Vec<u8>>>>, FunctionsAdapterError>
    where
        I: IntoIterator<Item = Option<&'a String>>,
    {
        use daft_core::{prelude::Utf8Array, series::IntoSeries};

        // PERF: Naively materialize into a vec so that it becomes a trusted length iterator
        let values: Vec<Option<&String>> = values.into_iter().collect();

        let arrow_array = Utf8Array::from_iter("", values.into_iter());
        let series = arrow_array.into_series();
        let result_series = self
            .call(FunctionArgs::try_new(vec![FunctionArg::Unnamed(series)]).unwrap())
            .unwrap();

        let iter = result_series.iter_binary()?;

        // Collect borrowed values into owned values to avoid lifetime issues
        let owned_values: Vec<Option<Vec<u8>>> = iter
            .map(|opt_bytes| opt_bytes.map(|bytes| bytes.to_vec()))
            .collect();

        Ok(Box::new(owned_values.into_iter()))
    }

    fn call_single(&self, value: String) -> Option<Vec<u8>> {
        self.call_batch(std::iter::once(Some(&value)))
            .ok()?
            .next()
            .expect("single output from single input")
    }
}
