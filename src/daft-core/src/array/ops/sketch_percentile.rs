use std::sync::Arc;

use common_error::DaftResult;

use crate::{
    array::{FixedSizeListArray, StructArray},
    datatypes::{DataType, Field, Float64Array},
    series::{IntoSeries, Series},
};

impl StructArray {
    pub fn sketch_percentile(
        &self,
        percentiles: &[f64],
        force_list_output: bool,
    ) -> DaftResult<Series> {
        let output_dtype = DataType::FixedSizeList(Box::new(DataType::Float64), percentiles.len());
        let output_field = Field::new(self.field.name.as_str(), output_dtype);

        let mut array_builder =
            arrow::array::Float64Builder::with_capacity(percentiles.len() * self.len());
        daft_sketch::from_arrow(self.to_arrow()?)?
            .iter()
            .for_each(|sketch| match sketch {
                None => {
                    array_builder
                        .extend(std::iter::repeat_n::<Option<f64>>(None, percentiles.len()));
                }
                Some(sketch) => {
                    array_builder.extend(percentiles.iter().map(|&p| sketch.quantile(p).unwrap()));
                }
            });
        let arrow_array = array_builder.finish();
        let flat_child = Float64Array::from_arrow(
            Field::new(self.name(), DataType::Float64),
            Arc::new(arrow_array),
        )?
        .into_series();

        if percentiles.len() > 1 || force_list_output {
            Ok(
                FixedSizeListArray::new(output_field, flat_child, self.nulls().cloned())
                    .into_series(),
            )
        } else {
            Ok(flat_child)
        }
    }
}
