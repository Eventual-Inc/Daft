use daft_dsl::functions::prelude::*;

use super::hash_method::HashMethod;
use crate::ops::hash_output_bytes;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ImageHash;

#[derive(FunctionArgs)]
struct ImageHashArgs<T> {
    input: T,
    #[arg(optional)]
    method: Option<HashMethod>,
    #[arg(optional)]
    hash_size: Option<u32>,
    #[arg(optional)]
    binbits: Option<u32>,
}

#[typetag::serde]
impl ScalarUDF for ImageHash {
    fn name(&self) -> &'static str {
        "image_hash"
    }

    fn call(
        &self,
        inputs: FunctionArgs<Series>,
        _ctx: &daft_dsl::functions::scalar::EvalContext,
    ) -> DaftResult<Series> {
        let ImageHashArgs {
            input,
            method,
            hash_size,
            binbits,
        } = inputs.try_into()?;
        let method = method.unwrap_or(HashMethod::PHash);
        let hash_size = hash_size.unwrap_or(8);
        let binbits = binbits.unwrap_or(3);
        crate::series::image_hash(&input, method, hash_size, binbits)
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let ImageHashArgs {
            input,
            method,
            hash_size,
            binbits,
        } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        let method = method.unwrap_or(HashMethod::PHash);
        let hash_size = hash_size.unwrap_or(8);
        let binbits = binbits.unwrap_or(3);
        let n_bytes = hash_output_bytes(method, hash_size, binbits);

        match &field.dtype {
            DataType::Image(_) | DataType::FixedShapeImage(..) => {
                Ok(Field::new(field.name, DataType::FixedSizeBinary(n_bytes)))
            }
            _ => Err(common_error::DaftError::TypeError(format!(
                "image_hash requires an Image or FixedShapeImage input, got {field}"
            ))),
        }
    }

    fn docstring(&self) -> &'static str {
        "Computes a perceptual hash of an image for near-duplicate detection.

Returns a ``FixedSizeBinary`` column.

Output size by method:

- Single-segment methods: ``hash_size * hash_size`` bits.
- ``'crop_resistant'``: ``9 * hash_size * hash_size`` bits (3×3 grid).
- ``'colorhash'``: ``14 * binbits`` bits (14 colour/intensity bins).

Supported methods:

- ``'phash'`` (default): Full 2D DCT perceptual hash — most robust.
- ``'phash_simple'``: Row-wise DCT only, compared to mean — faster variant.
- ``'dhash'``: Horizontal difference / gradient hash — fast and accurate.
- ``'dhash_vertical'``: Vertical difference hash — compares top/bottom neighbours.
- ``'ahash'``: Average hash — fastest, least robust.
- ``'whash'``: Multi-level Haar wavelet hash (pywt-compatible, DC-removed).
- ``'crop_resistant'``: Segment-based hash robust against cropping.
- ``'colorhash'``: Color distribution hash in HSV space.

Args:
    input: Image expression to hash.
    method: Hash algorithm (default: ``'phash'``).
    hash_size: Grid size for spatial hash methods (default: 8).
    binbits: Bits per bin for ``'colorhash'`` (default: 3, giving 42-bit hashes)."
    }
}
