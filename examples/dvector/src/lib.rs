use std::ffi::CStr;

use arrow_array::ArrayRef;
use arrow_schema::Field;
use daft_ext::prelude::*;

mod vectors;

use vectors::{
    map_bool_vectors_f64, map_bool_vectors_u32, map_float_vectors, map_float_vectors_nullable,
    return_field_float, return_field_hamming, return_field_jaccard,
};

#[daft_extension]
struct DvectorExtension;

impl DaftExtension for DvectorExtension {
    fn install(session: &mut dyn DaftSession) {
        session.define_function(Arc::new(L2Distance));
        session.define_function(Arc::new(InnerProduct));
        session.define_function(Arc::new(CosineDistance));
        session.define_function(Arc::new(L1Distance));
        session.define_function(Arc::new(HammingDistance));
        session.define_function(Arc::new(JaccardDistance));
    }
}

struct L2Distance;

impl DaftScalarFunction for L2Distance {
    fn name(&self) -> &CStr {
        c"dvector_l2_distance"
    }

    fn return_field(&self, args: &[Field]) -> DaftResult<Field> {
        return_field_float("dvector_l2_distance", args)
    }

    fn call(&self, args: &[ArrayRef]) -> DaftResult<ArrayRef> {
        map_float_vectors(&args[0], &args[1], |a, b| {
            a.iter()
                .zip(b)
                .map(|(x, y)| (x - y).powi(2))
                .sum::<f64>()
                .sqrt()
        })
    }
}

struct InnerProduct;

impl DaftScalarFunction for InnerProduct {
    fn name(&self) -> &CStr {
        c"dvector_inner_product"
    }

    fn return_field(&self, args: &[Field]) -> DaftResult<Field> {
        return_field_float("dvector_inner_product", args)
    }

    fn call(&self, args: &[ArrayRef]) -> DaftResult<ArrayRef> {
        map_float_vectors(&args[0], &args[1], |a, b| {
            -(a.iter().zip(b).map(|(x, y)| x * y).sum::<f64>())
        })
    }
}

struct CosineDistance;

impl DaftScalarFunction for CosineDistance {
    fn name(&self) -> &CStr {
        c"dvector_cosine_distance"
    }

    fn return_field(&self, args: &[Field]) -> DaftResult<Field> {
        return_field_float("dvector_cosine_distance", args)
    }

    fn call(&self, args: &[ArrayRef]) -> DaftResult<ArrayRef> {
        map_float_vectors_nullable(&args[0], &args[1], |a, b| {
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
}

struct L1Distance;

impl DaftScalarFunction for L1Distance {
    fn name(&self) -> &CStr {
        c"dvector_l1_distance"
    }

    fn return_field(&self, args: &[Field]) -> DaftResult<Field> {
        return_field_float("dvector_l1_distance", args)
    }

    fn call(&self, args: &[ArrayRef]) -> DaftResult<ArrayRef> {
        map_float_vectors(&args[0], &args[1], |a, b| {
            a.iter().zip(b).map(|(x, y)| (x - y).abs()).sum::<f64>()
        })
    }
}

struct HammingDistance;

impl DaftScalarFunction for HammingDistance {
    fn name(&self) -> &CStr {
        c"dvector_hamming_distance"
    }

    fn return_field(&self, args: &[Field]) -> DaftResult<Field> {
        return_field_hamming("dvector_hamming_distance", args)
    }

    fn call(&self, args: &[ArrayRef]) -> DaftResult<ArrayRef> {
        map_bool_vectors_u32(&args[0], &args[1], |a, b| {
            Some(a.iter().zip(b.iter()).filter(|(x, y)| x != y).count() as u32)
        })
    }
}

struct JaccardDistance;

impl DaftScalarFunction for JaccardDistance {
    fn name(&self) -> &CStr {
        c"dvector_jaccard_distance"
    }

    fn return_field(&self, args: &[Field]) -> DaftResult<Field> {
        return_field_jaccard("dvector_jaccard_distance", args)
    }

    fn call(&self, args: &[ArrayRef]) -> DaftResult<ArrayRef> {
        map_bool_vectors_f64(&args[0], &args[1], |a, b| {
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
}
