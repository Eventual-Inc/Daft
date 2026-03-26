use common_error::{DaftResult, ensure};
use daft_core::{
    datatypes::{try_mean_aggregation_supertype, try_sum_supertype},
    prelude::{DataType, Field, Schema},
    series::Series,
    utils::supertype::try_get_collection_supertype,
};
use daft_dsl::functions::{FunctionArgs, ScalarUDF, scalar::EvalContext};
use serde::{Deserialize, Serialize};

use crate::series::SeriesListExtension;

fn zip_into_list(
    field_name: &str,
    inputs: &[Series],
    inner_dtype: &DataType,
) -> DaftResult<Series> {
    let list_field = Field::new(field_name, DataType::List(Box::new(inner_dtype.clone())));

    let casted = inputs
        .iter()
        .map(|s| s.cast(inner_dtype))
        .collect::<DaftResult<Vec<_>>>()?;
    let casted_refs: Vec<&Series> = casted.iter().collect();
    Series::zip(list_field, casted_refs.as_slice())
}

fn infer_inner_dtype(inputs: &[Series]) -> DaftResult<DataType> {
    let dtypes = inputs.iter().map(|s| s.data_type());
    try_get_collection_supertype(dtypes)
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MinOf;

#[typetag::serde]
impl ScalarUDF for MinOf {
    fn name(&self) -> &'static str {
        "min_of"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["least"]
    }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        ensure!(
            inputs.len() >= 2,
            ValueError: "min_of() requires at least 2 input args, got {}",
            inputs.len()
        );

        let field_name = inputs[0].name();
        let inner_dtype = infer_inner_dtype(&inputs)?;
        ensure!(
            inner_dtype.is_numeric() || inner_dtype.is_boolean(),
            TypeError: "min_of() expects numeric or boolean inputs, got {}",
            inner_dtype
        );

        let list_series = zip_into_list(field_name, &inputs, &inner_dtype)?;
        let out = list_series.list_min()?;
        Ok(out.rename(field_name))
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<daft_dsl::ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let inputs = inputs.into_inner();
        ensure!(
            inputs.len() >= 2,
            SchemaMismatch: "min_of() requires at least 2 input args, got {}",
            inputs.len()
        );

        let first_field = inputs[0].to_field(schema)?;
        let field_types = inputs
            .iter()
            .map(|e| e.get_type(schema))
            .collect::<DaftResult<Vec<_>>>()?;
        let inner_dtype = try_get_collection_supertype(field_types)?;

        ensure!(
            inner_dtype.is_numeric() || inner_dtype.is_boolean(),
            TypeError: "min_of() expects numeric or boolean inputs, got {}",
            inner_dtype
        );

        Ok(Field::new(first_field.name, inner_dtype))
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MaxOf;

#[typetag::serde]
impl ScalarUDF for MaxOf {
    fn name(&self) -> &'static str {
        "max_of"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["greatest"]
    }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        ensure!(
            inputs.len() >= 2,
            ValueError: "max_of() requires at least 2 input args, got {}",
            inputs.len()
        );

        let field_name = inputs[0].name();
        let inner_dtype = infer_inner_dtype(&inputs)?;
        ensure!(
            inner_dtype.is_numeric() || inner_dtype.is_boolean(),
            TypeError: "max_of() expects numeric or boolean inputs, got {}",
            inner_dtype
        );

        let list_series = zip_into_list(field_name, &inputs, &inner_dtype)?;
        let out = list_series.list_max()?;
        Ok(out.rename(field_name))
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<daft_dsl::ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let inputs = inputs.into_inner();
        ensure!(
            inputs.len() >= 2,
            SchemaMismatch: "max_of() requires at least 2 input args, got {}",
            inputs.len()
        );

        let first_field = inputs[0].to_field(schema)?;
        let field_types = inputs
            .iter()
            .map(|e| e.get_type(schema))
            .collect::<DaftResult<Vec<_>>>()?;
        let inner_dtype = try_get_collection_supertype(field_types)?;

        ensure!(
            inner_dtype.is_numeric() || inner_dtype.is_boolean(),
            TypeError: "max_of() expects numeric or boolean inputs, got {}",
            inner_dtype
        );

        Ok(Field::new(first_field.name, inner_dtype))
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct SumOf;

#[typetag::serde]
impl ScalarUDF for SumOf {
    fn name(&self) -> &'static str {
        "sum_of"
    }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        ensure!(
            inputs.len() >= 2,
            ValueError: "sum_of() requires at least 2 input args, got {}",
            inputs.len()
        );

        let field_name = inputs[0].name();
        let inner_dtype = infer_inner_dtype(&inputs)?;
        ensure!(
            inner_dtype.is_numeric(),
            TypeError: "sum_of() expects numeric inputs, got {}",
            inner_dtype
        );

        let list_series = zip_into_list(field_name, &inputs, &inner_dtype)?;
        let out = list_series.list_sum()?;
        Ok(out.rename(field_name))
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<daft_dsl::ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let inputs = inputs.into_inner();
        ensure!(
            inputs.len() >= 2,
            SchemaMismatch: "sum_of() requires at least 2 input args, got {}",
            inputs.len()
        );

        let first_field = inputs[0].to_field(schema)?;
        let field_types = inputs
            .iter()
            .map(|e| e.get_type(schema))
            .collect::<DaftResult<Vec<_>>>()?;
        let inner_dtype = try_get_collection_supertype(field_types)?;

        ensure!(
            inner_dtype.is_numeric(),
            TypeError: "sum_of() expects numeric inputs, got {}",
            inner_dtype
        );

        Ok(Field::new(
            first_field.name,
            try_sum_supertype(&inner_dtype)?,
        ))
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct MeanOf;

#[typetag::serde]
impl ScalarUDF for MeanOf {
    fn name(&self) -> &'static str {
        "mean_of"
    }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        ensure!(
            inputs.len() >= 2,
            ValueError: "mean_of() requires at least 2 input args, got {}",
            inputs.len()
        );

        let field_name = inputs[0].name();
        let inner_dtype = infer_inner_dtype(&inputs)?;
        ensure!(
            inner_dtype.is_numeric(),
            TypeError: "mean_of() expects numeric inputs, got {}",
            inner_dtype
        );

        let list_series = zip_into_list(field_name, &inputs, &inner_dtype)?;
        let out = list_series.list_mean()?;
        Ok(out.rename(field_name))
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<daft_dsl::ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let inputs = inputs.into_inner();
        ensure!(
            inputs.len() >= 2,
            SchemaMismatch: "mean_of() requires at least 2 input args, got {}",
            inputs.len()
        );

        let first_field = inputs[0].to_field(schema)?;
        let field_types = inputs
            .iter()
            .map(|e| e.get_type(schema))
            .collect::<DaftResult<Vec<_>>>()?;
        let inner_dtype = try_get_collection_supertype(field_types)?;

        ensure!(
            inner_dtype.is_numeric(),
            TypeError: "mean_of() expects numeric inputs, got {}",
            inner_dtype
        );

        Ok(Field::new(
            first_field.name,
            try_mean_aggregation_supertype(&inner_dtype)?,
        ))
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AllOf;

#[typetag::serde]
impl ScalarUDF for AllOf {
    fn name(&self) -> &'static str {
        "all_of"
    }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        ensure!(
            inputs.len() >= 2,
            ValueError: "all_of() requires at least 2 input args, got {}",
            inputs.len()
        );

        let field_name = inputs[0].name();
        let inner_dtype = infer_inner_dtype(&inputs)?;
        ensure!(
            inner_dtype.is_boolean(),
            TypeError: "all_of() expects boolean inputs, got {}",
            inner_dtype
        );

        let list_series = zip_into_list(field_name, &inputs, &inner_dtype)?;
        let out = list_series.list_bool_and()?;
        Ok(out.rename(field_name))
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<daft_dsl::ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let inputs = inputs.into_inner();
        ensure!(
            inputs.len() >= 2,
            SchemaMismatch: "all_of() requires at least 2 input args, got {}",
            inputs.len()
        );

        let first_field = inputs[0].to_field(schema)?;
        let field_types = inputs
            .iter()
            .map(|e| e.get_type(schema))
            .collect::<DaftResult<Vec<_>>>()?;
        let inner_dtype = try_get_collection_supertype(field_types)?;

        ensure!(
            inner_dtype.is_boolean(),
            TypeError: "all_of() expects boolean inputs, got {}",
            inner_dtype
        );

        Ok(Field::new(first_field.name, DataType::Boolean))
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AnyOf;

#[typetag::serde]
impl ScalarUDF for AnyOf {
    fn name(&self) -> &'static str {
        "any_of"
    }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &EvalContext) -> DaftResult<Series> {
        let inputs = inputs.into_inner();
        ensure!(
            inputs.len() >= 2,
            ValueError: "any_of() requires at least 2 input args, got {}",
            inputs.len()
        );

        let field_name = inputs[0].name();
        let inner_dtype = infer_inner_dtype(&inputs)?;
        ensure!(
            inner_dtype.is_boolean(),
            TypeError: "any_of() expects boolean inputs, got {}",
            inner_dtype
        );

        let list_series = zip_into_list(field_name, &inputs, &inner_dtype)?;
        let out = list_series.list_bool_or()?;
        Ok(out.rename(field_name))
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<daft_dsl::ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let inputs = inputs.into_inner();
        ensure!(
            inputs.len() >= 2,
            SchemaMismatch: "any_of() requires at least 2 input args, got {}",
            inputs.len()
        );

        let first_field = inputs[0].to_field(schema)?;
        let field_types = inputs
            .iter()
            .map(|e| e.get_type(schema))
            .collect::<DaftResult<Vec<_>>>()?;
        let inner_dtype = try_get_collection_supertype(field_types)?;

        ensure!(
            inner_dtype.is_boolean(),
            TypeError: "any_of() expects boolean inputs, got {}",
            inner_dtype
        );

        Ok(Field::new(first_field.name, DataType::Boolean))
    }
}
