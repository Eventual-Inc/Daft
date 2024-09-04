use daft_core::{
    datatypes::{try_mean_supertype, try_sum_supertype, DataType, Field},
    CountMode, IntoSeries,
};

use common_error::DaftError;

use daft_dsl::{make_parameterized_udf_function, make_udf_function};
#[cfg(feature = "python")]
use pyo3::prelude::*;

make_parameterized_udf_function! {
    name: "list_chunk",
    params: (size: usize),
    to_field: (self, input, schema) {
        let input_field = input.to_field(schema)?;
        Ok(input_field
            .to_exploded_field()?
            .to_fixed_size_list_field(self.size)?
            .to_list_field()?)
    },
    evaluate: (self, input) {
        input.list_chunk(self.size)
    }
}

make_parameterized_udf_function! {
    name: "list_count",
    params: (mode: CountMode),
    to_field: (self, input, schema) {
        let input_field = input.to_field(schema)?;

        match input_field.dtype {
            DataType::List(_) | DataType::FixedSizeList(_, _) => {
                Ok(Field::new(input.name(), DataType::UInt64))
            },
            _ => Err(DaftError::TypeError(format!(
                "Expected input to be a list type, received: {}",
                input_field.dtype
            ))),
        }
    },
    evaluate: (self, data) {
        data.list_count(self.mode).map(|count| count.into_series())
    }
}

make_udf_function! {
    name: "explode",
    to_field: (input, schema) {
        let field = input.to_field(schema)?;
        field.to_exploded_field()
    },
    evaluate: (input) {
        input.explode()
    }
}

make_udf_function! {
    name: "list_get",
    to_field: (input, idx, fallback, schema) {
        let input_field = input.to_field(schema)?;
        let idx_field = idx.to_field(schema)?;
        let _default_field = fallback.to_field(schema)?;

        if !idx_field.dtype.is_integer() {
            return Err(DaftError::TypeError(format!(
                "Expected get index to be integer, received: {}",
                idx_field.dtype
            )));
        }

        // TODO(Kevin): Check if default dtype can be cast into input dtype.

        let exploded_field = input_field.to_exploded_field()?;
        Ok(exploded_field)
    },
    evaluate: (input, idx, fallback) {
        input.list_get(idx, fallback)
    }
}

make_udf_function! {
    name: "list_join",
    to_field: (input, delimiter, schema) {
        let input_field = input.to_field(schema)?;
        let delimiter_field = delimiter.to_field(schema)?;
        if delimiter_field.dtype != DataType::Utf8 {
            return Err(DaftError::TypeError(format!(
                "Expected join delimiter to be of type {}, received: {}",
                DataType::Utf8,
                delimiter_field.dtype
            )));
        }

        match input_field.dtype {
            DataType::List(_) | DataType::FixedSizeList(_, _) => {
                let exploded_field = input_field.to_exploded_field()?;
                if exploded_field.dtype != DataType::Utf8 {
                    return Err(DaftError::TypeError(format!("Expected column \"{}\" to be a list type with a Utf8 child, received list type with child dtype {}", exploded_field.name, exploded_field.dtype)));
                }
                Ok(exploded_field)
            }
            _ => Err(DaftError::TypeError(format!(
                "Expected input to be a list type, received: {}",
                input_field.dtype
            ))),
        }
    },
    evaluate: (input, delimiter) {
        let delimiter = delimiter.utf8()?;
        Ok(input.join(delimiter)?.into_series())
    }
}
make_udf_function! {
    name: "list_max",
    to_field: (input, schema) {
        let field = input.to_field(schema)?.to_exploded_field()?;

        if field.dtype.is_numeric() {
            Ok(field)
        } else {
            Err(DaftError::TypeError(format!(
                "Expected input to be numeric, got {}",
                field.dtype
            )))
        }
    },
    evaluate: (input) {
        input.list_max()
    }
}

make_udf_function! {
    name: "list_mean",
    to_field: (input, schema) {
        let inner_field = input.to_field(schema)?.to_exploded_field()?;
        Ok(Field::new(
            inner_field.name.as_str(),
            try_mean_supertype(&inner_field.dtype)?,
        ))
    },
    evaluate: (input) {
        input.list_mean()
    }
}
make_udf_function! {
    name: "list_min",
    to_field: (input, schema) {
        let field = input.to_field(schema)?.to_exploded_field()?;

        if field.dtype.is_numeric() {
            Ok(field)
        } else {
            Err(DaftError::TypeError(format!(
                "Expected input to be numeric, got {}",
                field.dtype
            )))
        }
    },
    evaluate: (input) {
        input.list_min()
    }
}

make_udf_function! {
    name: "list_slice",
    to_field: (input, start, end, schema) {
        let input_field = input.to_field(schema)?;
        let start_field = start.to_field(schema)?;
        let end_field = end.to_field(schema)?;

        if !start_field.dtype.is_integer() {
            return Err(DaftError::TypeError(format!(
                "Expected start index to be integer, received: {}",
                start_field.dtype
            )));
        }

        if !end_field.dtype.is_integer() && !end_field.dtype.is_null() {
            return Err(DaftError::TypeError(format!(
                "Expected end index to be integer or unprovided, received: {}",
                end_field.dtype
            )));
        }
        input_field.to_exploded_field()?.to_list_field()
    },
    evaluate: (input, start, stop) {
        input.list_slice(start, stop)
    }
}

make_udf_function! {
    name: "list_sum",
    to_field: (input, schema) {
        let inner_field = input.to_field(schema)?.to_exploded_field()?;

        Ok(Field::new(
            inner_field.name.as_str(),
            try_sum_supertype(&inner_field.dtype)?,
        ))
    },
    evaluate: (input) {
        input.list_sum()
    }
}

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_wrapped(wrap_pyfunction!(py_explode))?;
    parent.add_wrapped(wrap_pyfunction!(py_list_chunk))?;
    parent.add_wrapped(wrap_pyfunction!(py_list_count))?;
    parent.add_wrapped(wrap_pyfunction!(py_list_get))?;
    parent.add_wrapped(wrap_pyfunction!(py_list_join))?;
    parent.add_wrapped(wrap_pyfunction!(py_list_max))?;
    parent.add_wrapped(wrap_pyfunction!(py_list_mean))?;
    parent.add_wrapped(wrap_pyfunction!(py_list_min))?;
    parent.add_wrapped(wrap_pyfunction!(py_list_slice))?;
    parent.add_wrapped(wrap_pyfunction!(py_list_sum))?;

    Ok(())
}
