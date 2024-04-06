use arrow2::array::Array;
use lazy_static::lazy_static;
use serde_arrow::schema::{SchemaLike, SerdeArrowSchema, TracingOptions};
use sketches_ddsketch::DDSketch;

lazy_static! {
    static ref ARROW2_DDSKETCH_FIELDS: Vec<arrow2::datatypes::Field> =
        SerdeArrowSchema::from_type::<DDSketch>(TracingOptions::default())
            .unwrap()
            .to_arrow2_fields()
            .unwrap();

    /// The corresponding arrow2 DataType of Vec<DDSketch> when serialized as an arrow2 array
    pub static ref ARROW2_DDSKETCH_DTYPE: arrow2::datatypes::DataType = arrow2::datatypes::DataType::Struct(ARROW2_DDSKETCH_FIELDS.clone());
}

/// Converts a Vec<Option<DDSketch>> into an arrow2 Array
pub fn into_arrow2(sketches: Vec<Option<DDSketch>>) -> Box<dyn arrow2::array::Array> {
    if sketches.is_empty() {
        return arrow2::array::StructArray::new_empty(ARROW2_DDSKETCH_DTYPE.clone()).to_boxed();
    }
    let arrow2_arrays =
        serde_arrow::to_arrow2(ARROW2_DDSKETCH_FIELDS.as_slice(), &sketches).unwrap();
    let validity = arrow2_arrays
        .iter()
        .fold(None, |acc, arr| match (&acc, arr.validity()) {
            (_, None) => acc,
            (None, Some(arr)) => Some(arr.clone()),
            (Some(validity), Some(arr)) => Some(arrow2::bitmap::and(validity, arr)),
        });
    arrow2::array::StructArray::new(ARROW2_DDSKETCH_DTYPE.clone(), arrow2_arrays, validity)
        .to_boxed()
}

/// Converts an arrow2 Array into a Vec<Option<DDSketch>>
pub fn from_arrow2(
    arrow_array: Box<dyn arrow2::array::Array>,
) -> serde_arrow::Result<Vec<Option<DDSketch>>> {
    let arrow_struct_arr: &arrow2::array::StructArray =
        arrow_array.as_any().downcast_ref().unwrap();
    serde_arrow::from_arrow2(&ARROW2_DDSKETCH_FIELDS, arrow_struct_arr.values())
}
