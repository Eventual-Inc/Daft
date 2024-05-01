use std::sync::Arc;

use crate::{
    array::{FixedSizeListArray, StructArray},
    datatypes::{Field, Float64Array},
    DataType, IntoSeries, Series,
};

use arrow2::array::{MutablePrimitiveArray, PrimitiveArray};
use common_error::DaftResult;

use super::from_arrow::FromArrow;

impl StructArray {
    pub fn sketch_percentile(
        &self,
        percentiles: &[f64],
        force_list_output: bool,
    ) -> DaftResult<Series> {
        let output_dtype = DataType::FixedSizeList(Box::new(DataType::Float64), percentiles.len());
        let output_field = Field::new(self.field.name.as_str(), output_dtype);

        let mut flat_child =
            MutablePrimitiveArray::<f64>::with_capacity(percentiles.len() * self.len());
        daft_sketch::from_arrow2(self.to_arrow())?
            .iter()
            .for_each(|sketch| match sketch {
                None => {
                    flat_child.extend_trusted_len(
                        std::iter::repeat::<Option<f64>>(None).take(percentiles.len()),
                    );
                }
                Some(sketch) => flat_child
                    .extend_trusted_len(percentiles.iter().map(|&p| sketch.quantile(p).unwrap())),
            });
        let flat_child: PrimitiveArray<f64> = flat_child.into();
        let flat_child = Float64Array::from_arrow(
            Arc::new(Field::new(self.name(), DataType::Float64)),
            flat_child.boxed(),
        )?
        .into_series();

        if percentiles.len() > 1 || force_list_output {
            Ok(
                FixedSizeListArray::new(output_field, flat_child, self.validity().cloned())
                    .into_series(),
            )
        } else {
            Ok(flat_child)
        }
    }
}
