use std::cmp::Ordering;

use arrow_buffer::NullBufferBuilder;
use common_error::{DaftError, DaftResult};
use daft_core::{
    array::prelude::UInt64Array,
    lit::Literal,
    prelude::{DataType, Field, Schema, Utf8Array},
    series::{IntoSeries, Series},
};
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, ScalarUDF, UnaryArg, scalar::ScalarFn},
};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Md5Function;

/// Convert a Literal value to bytes for MD5 hashing.
/// For List types, the elements are sorted first to ensure order-insensitive hashing.
fn literal_to_bytes(lit: &Literal) -> DaftResult<Vec<u8>> {
    match lit {
        Literal::Null => {
            // Null is represented as empty bytes
            Ok(b"null".to_vec())
        }
        Literal::Boolean(b) => {
            Ok(b.to_string().into_bytes())
        }
        Literal::Utf8(s) => {
            Ok(s.as_bytes().to_vec())
        }
        Literal::Binary(b) => {
            Ok(b.clone())
        }
        Literal::Int8(n) => {
            Ok(n.to_string().into_bytes())
        }
        Literal::UInt8(n) => {
            Ok(n.to_string().into_bytes())
        }
        Literal::Int16(n) => {
            Ok(n.to_string().into_bytes())
        }
        Literal::UInt16(n) => {
            Ok(n.to_string().into_bytes())
        }
        Literal::Int32(n) => {
            Ok(n.to_string().into_bytes())
        }
        Literal::UInt32(n) => {
            Ok(n.to_string().into_bytes())
        }
        Literal::Int64(n) => {
            Ok(n.to_string().into_bytes())
        }
        Literal::UInt64(n) => {
            Ok(n.to_string().into_bytes())
        }
        Literal::Float32(f) => {
            Ok(f.to_string().into_bytes())
        }
        Literal::Float64(f) => {
            Ok(f.to_string().into_bytes())
        }
        Literal::List(series) => {
            // For lists, we need to sort the elements deterministically
            // to ensure order-insensitive hashing
            let sorted_series = sort_list_series(series)?;
            let mut json_array = Vec::new();
            
            for i in 0..sorted_series.len() {
                let lit = sorted_series.get_lit(i);
                let bytes = literal_to_bytes(&lit)?;
                json_array.push(String::from_utf8_lossy(&bytes).to_string());
            }
            
            // Serialize as JSON for deterministic output
            let json_str = serde_json::to_string(&json_array)?;
            Ok(json_str.into_bytes())
        }
        Literal::Struct(map) => {
            // Sort keys for deterministic output
            let mut sorted_entries: Vec<_> = map.iter().collect();
            sorted_entries.sort_by_key(|(k, _)| k.as_str());
            
            let json_obj = sorted_entries
                .into_iter()
                .map(|(k, v)| {
                    let bytes = literal_to_bytes(v)?;
                    Ok((
                        k.clone(),
                        serde_json::Value::String(String::from_utf8_lossy(&bytes).to_string()),
                    ))
                })
                .collect::<DaftResult<Vec<_>>>()?;
            
            let json_map: serde_json::Map<String, serde_json::Value> =
                json_obj.into_iter().collect();
            let json_str = serde_json::to_string(&json_map)?;
            Ok(json_str.into_bytes())
        }
        Literal::Map { keys, values } => {
            // Serialize map as JSON object with sorted keys
            let mut entries = Vec::new();
            for i in 0..keys.len() {
                let key_lit = keys.get_lit(i);
                let val_lit = values.get_lit(i);
                
                let key_bytes = literal_to_bytes(&key_lit)?;
                let key_str = String::from_utf8_lossy(&key_bytes).to_string();
                
                let val_bytes = literal_to_bytes(&val_lit)?;
                let val_str = String::from_utf8_lossy(&val_bytes).to_string();
                
                entries.push((key_str, val_str));
            }
            
            // Sort by keys for deterministic output
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            
            let json_map: serde_json::Map<String, serde_json::Value> = entries
                .into_iter()
                .map(|(k, v)| (k, serde_json::Value::String(v)))
                .collect();
            
            let json_str = serde_json::to_string(&json_map)?;
            Ok(json_str.into_bytes())
        }
        Literal::Date(d) => {
            Ok(d.to_string().into_bytes())
        }
        Literal::Time(t, tu) => {
            Ok(format!("{:?}_{:?}", t, tu).into_bytes())
        }
        Literal::Timestamp(ts, tu, tz) => {
            let tz_str = tz.as_ref().map(|s| s.as_str()).unwrap_or("");
            Ok(format!("{}_{:?}_{}", ts, tu, tz_str).into_bytes())
        }
        Literal::Duration(d, tu) => {
            Ok(format!("{:?}_{:?}", d, tu).into_bytes())
        }
        Literal::Decimal(n, precision, scale) => {
            Ok(format!("{}_{:?}_{:?}", n, precision, scale).into_bytes())
        }
        Literal::Uuid(uuid) => {
            Ok(uuid.to_string().into_bytes())
        }
        Literal::Tensor { data, shape } => {
            // Serialize tensor metadata and data
            let data_json = json!({
                "shape": shape,
                "type": data.data_type().to_string()
            });
            Ok(data_json.to_string().into_bytes())
        }
        Literal::Embedding(series) => {
            // Serialize embedding type
            let type_str = series.data_type().to_string();
            Ok(format!("embedding_{}", type_str).into_bytes())
        }
        Literal::Image(_) => {
            // For image, use a placeholder (actual image comparison is complex)
            Ok(b"image".to_vec())
        }
        Literal::File(fr) => {
            Ok(format!("{:?}", fr).into_bytes())
        }
        Literal::SparseTensor {
            values,
            indices: _,
            shape,
            indices_offset,
        } => {
            let sparse_json = json!({
                "shape": shape,
                "indices_offset": indices_offset,
                "dtype": values.data_type().to_string()
            });
            Ok(sparse_json.to_string().into_bytes())
        }
        Literal::Interval(iv) => {
            Ok(format!("{:?}", iv).into_bytes())
        }
        Literal::Float16(f) => {
            Ok(f.to_string().into_bytes())
        }
        #[cfg(feature = "python")]
        Literal::Python(_) => {
            Err(DaftError::TypeError(
                "Cannot compute MD5 of Python objects".to_string(),
            ))
        }

        Literal::Extension(_) => {
            Err(DaftError::TypeError(
                "Cannot compute MD5 of extension types".to_string(),
            ))
        }
    }
}

/// Sort list elements for order-insensitive hashing.
/// Elements are sorted by their serialized string representation.
fn sort_list_series(series: &Series) -> DaftResult<Series> {
    let len = series.len();
    let mut indices: Vec<usize> = (0..len).collect();
    
    // Sort indices based on the serialized representation of elements
    indices.sort_by(|&a, &b| {
        let lit_a = series.get_lit(a);
        let lit_b = series.get_lit(b);
        
        match (literal_to_bytes(&lit_a), literal_to_bytes(&lit_b)) {
            (Ok(bytes_a), Ok(bytes_b)) => bytes_a.cmp(&bytes_b),
            _ => Ordering::Equal,
        }
    });
    
    // Create a UInt64Array with sorted indices and use take to reorder
    let indices_array = UInt64Array::from_iter(
        Field::new("indices", DataType::UInt64),
        indices.into_iter().map(|i| Some(i as u64))
    );
    
    series.take(&indices_array)
}

#[typetag::serde]
impl ScalarUDF for Md5Function {
    fn name(&self) -> &'static str {
        "md5"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let UnaryArg { input } = inputs.try_into()?;

        let len = input.len();
        let mut values = Vec::with_capacity(len);
        let mut validity = NullBufferBuilder::new(len);

        for i in 0..len {
            let lit = input.get_lit(i);
            
            match lit {
                Literal::Null => {
                    values.push(None);
                    validity.append_null();
                }
                _ => {
                    let bytes = literal_to_bytes(&lit)?;
                    let md5_hash = format!("{:x}", ::md5::compute(&bytes));
                    values.push(Some(md5_hash));
                    validity.append_non_null();
                }
            }
        }

        let arr = Utf8Array::from_iter(
            input.name(),
            values.into_iter()
        );
        let arr = arr.with_nulls(validity.finish())?;
        Ok(arr.into_series())
    }

    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        let UnaryArg { input } = inputs.try_into()?;
        let field = input.to_field(schema)?;

        // MD5 always returns a UTF8 string, regardless of input type
        Ok(Field::new(field.name, DataType::Utf8))
    }

    fn docstring(&self) -> &'static str {
        "Returns the MD5 digest (hex string) for each input value. \
        Supports all data types. For lists, elements are sorted before hashing to enable order-insensitive comparison."
    }
}

#[must_use]
pub fn md5(input: ExprRef) -> ExprRef {
    ScalarFn::builtin(Md5Function, vec![input]).into()
}
