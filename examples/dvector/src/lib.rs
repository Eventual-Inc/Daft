use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::DataType;
use daft_ext::prelude::*;

mod vectors;

use vectors::{
    map_bool_vectors_f64, map_bool_vectors_u32, map_float_vectors, map_float_vectors_nullable,
};

#[daft_extension]
struct DvectorExtension;

impl DaftExtension for DvectorExtension {
    fn install(session: &mut dyn DaftSession) {
        session.define_function(Arc::new(DvectorL2Distance));
        session.define_function(Arc::new(DvectorInnerProduct));
        session.define_function(Arc::new(DvectorCosineDistance));
        session.define_function(Arc::new(DvectorL1Distance));
        session.define_function(Arc::new(DvectorHammingDistance));
        session.define_function(Arc::new(DvectorJaccardDistance));
    }
}

#[daft_func_batch(return_dtype = DataType::Float64)]
fn dvector_l2_distance(a: ArrayRef, b: ArrayRef) -> DaftResult<ArrayRef> {
    map_float_vectors(&a, &b, |a, b| {
        a.iter()
            .zip(b)
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f64>()
            .sqrt()
    })
}

#[daft_func_batch(return_dtype = DataType::Float64)]
fn dvector_inner_product(a: ArrayRef, b: ArrayRef) -> DaftResult<ArrayRef> {
    map_float_vectors(&a, &b, |a, b| {
        -(a.iter().zip(b).map(|(x, y)| x * y).sum::<f64>())
    })
}

#[daft_func_batch(return_dtype = DataType::Float64)]
fn dvector_cosine_distance(a: ArrayRef, b: ArrayRef) -> DaftResult<ArrayRef> {
    map_float_vectors_nullable(&a, &b, |a, b| {
        let (mut dot, mut na, mut nb) = (0.0, 0.0, 0.0);
        for (x, y) in a.iter().zip(b) {
            dot += x * y;
            na += x * x;
            nb += y * y;
        }
        let denom = na.sqrt() * nb.sqrt();
        if denom == 0.0 {
            None
        } else {
            Some(1.0 - dot / denom)
        }
    })
}

#[daft_func_batch(return_dtype = DataType::Float64)]
fn dvector_l1_distance(a: ArrayRef, b: ArrayRef) -> DaftResult<ArrayRef> {
    map_float_vectors(&a, &b, |a, b| {
        a.iter().zip(b).map(|(x, y)| (x - y).abs()).sum::<f64>()
    })
}

#[daft_func_batch(return_dtype = DataType::UInt32)]
fn dvector_hamming_distance(a: ArrayRef, b: ArrayRef) -> DaftResult<ArrayRef> {
    map_bool_vectors_u32(&a, &b, |a, b| {
        Some(a.iter().zip(b.iter()).filter(|(x, y)| x != y).count() as u32)
    })
}

#[daft_func_batch(return_dtype = DataType::Float64)]
fn dvector_jaccard_distance(a: ArrayRef, b: ArrayRef) -> DaftResult<ArrayRef> {
    map_bool_vectors_f64(&a, &b, |a, b| {
        let union_ct = a
            .iter()
            .zip(b.iter())
            .filter(|(x, y)| x.unwrap_or(false) || y.unwrap_or(false))
            .count();
        if union_ct == 0 {
            return None;
        }
        let inter_ct = a
            .iter()
            .zip(b.iter())
            .filter(|(x, y)| x.unwrap_or(false) && y.unwrap_or(false))
            .count();
        Some(1.0 - inter_ct as f64 / union_ct as f64)
    })
}
