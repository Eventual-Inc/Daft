use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use daft_dsl::{
    AggExpr,
    expr::{
        MapGroupsFn,
        bound_expr::{BoundAggExpr, BoundExpr},
    },
    operator_metrics::NoopMetricsCollector,
    python_udf::PyScalarFn,
};
use daft_groupby::{IntoGroups, IntoUniqueIdxs};

use super::inline_agg::can_inline_agg;
use crate::RecordBatch;

impl RecordBatch {
    pub fn agg(&self, to_agg: &[BoundAggExpr], group_by: &[BoundExpr]) -> DaftResult<Self> {
        // Dispatch depending on whether we're doing groupby or just a global agg.
        match group_by.len() {
            0 => self.agg_global(to_agg),
            _ => self.agg_groupby(to_agg, group_by),
        }
    }

    pub fn agg_global(&self, to_agg: &[BoundAggExpr]) -> DaftResult<Self> {
        self.eval_expression_list(
            &to_agg
                .iter()
                .map(|agg_expr| BoundExpr::new_unchecked(agg_expr.as_ref().into()))
                .collect::<Vec<_>>(),
        )
    }

    pub fn agg_groupby(&self, to_agg: &[BoundAggExpr], group_by: &[BoundExpr]) -> DaftResult<Self> {
        #[cfg(feature = "python")]
        if let [agg_expr] = to_agg
            && let AggExpr::MapGroups { func, inputs } = agg_expr.as_ref()
        {
            return self.map_groups(
                func,
                &inputs
                    .iter()
                    .cloned()
                    .map(BoundExpr::new_unchecked)
                    .collect::<Vec<_>>(),
                group_by,
            );
        }

        // Fast path: inline aggregation for supported agg types (count, sum, min, max).
        if can_inline_agg(to_agg, self) {
            return self.agg_groupby_inline(to_agg, group_by);
        }

        // Table with just the groupby columns.
        let groupby_table = self.eval_expression_list(group_by)?;

        // Get the unique group keys (by indices)
        // and the grouped values (also by indices, one array of indices per group).
        let (groupkey_indices, groupvals_indices) = groupby_table.make_groups()?;

        // Table with the aggregated (deduplicated) group keys.
        let groupkeys_table = {
            let indices_as_arr = UInt64Array::from_vec("", groupkey_indices);
            groupby_table.take(&indices_as_arr)?
        };

        // Take fast path short circuit if there is only 1 group
        let group_idx_input = if groupvals_indices.len() == 1 {
            None
        } else {
            Some(&groupvals_indices)
        };

        let grouped_cols = to_agg
            .iter()
            .map(|e| self.eval_agg_expression(e, group_idx_input))
            .collect::<DaftResult<Vec<_>>>()?;

        // Combine the groupkey columns and the aggregation result columns.
        let groupkeys_series: Vec<Series> = groupkeys_table
            .columns
            .iter()
            .map(|c| c.as_materialized_series().clone())
            .collect();
        Self::from_nonempty_columns([groupkeys_series.as_slice(), &grouped_cols].concat())
    }

    #[cfg(feature = "python")]
    pub fn map_groups(
        &self,
        func: &MapGroupsFn,
        inputs: &[BoundExpr],
        group_by: &[BoundExpr],
    ) -> DaftResult<Self> {
        use common_runtime::get_compute_runtime;
        // Table with just the groupby columns.
        let groupby_table = self.eval_expression_list(group_by)?;

        // Get the unique group keys (by indices)
        // and the grouped values (also by indices, one array of indices per group).
        let (groupkey_indices, groupvals_indices) = groupby_table.make_groups()?;

        let evaluated_inputs = inputs
            .iter()
            .map(|e| self.eval_expression(e))
            .collect::<DaftResult<Vec<_>>>()?;

        let (groupkeys_table, grouped_col) = match func {
            MapGroupsFn::Legacy(udf) => {
                if udf.concurrency.is_some() {
                    return Err(DaftError::ComputeError(
                        "Cannot run actor pool UDF in MapGroups".to_string(),
                    ));
                }

                if groupvals_indices.is_empty() {
                    let empty_groupkeys_table = Self::empty(Some(groupby_table.schema));
                    let empty_udf_output_col = Series::empty(
                        evaluated_inputs
                            .first()
                            .map_or_else(|| "output", |s| s.name()),
                        &udf.return_dtype,
                    );
                    (empty_groupkeys_table, empty_udf_output_col)
                } else if groupvals_indices.len() == 1 {
                    let grouped_col = udf.call_udf(evaluated_inputs.as_slice())?;
                    let groupkeys_table = {
                        let indices_as_arr = UInt64Array::from_vec("", groupkey_indices);
                        groupby_table.take(&indices_as_arr)?
                    };
                    (groupkeys_table, grouped_col)
                } else {
                    let grouped_results = groupkey_indices
                        .iter()
                        .zip(groupvals_indices.iter())
                        .map(|(groupkey_index, groupval_indices)| {
                            let evaluated_grouped_col = {
                                // Convert group indices to Series
                                let indices_as_arr =
                                    UInt64Array::from_vec("", groupval_indices.to_vec());

                                // Take each input Series by the group indices
                                let input_groups = evaluated_inputs
                                    .iter()
                                    .map(|s| s.take(&indices_as_arr))
                                    .collect::<DaftResult<Vec<_>>>()?;

                                // Call the UDF on the grouped inputs
                                udf.call_udf(input_groups.as_slice())?
                            };

                            let broadcasted_groupkeys_table = {
                                // Convert groupkey indices to Series
                                let groupkey_indices_as_arr =
                                    UInt64Array::from_slice("", &[*groupkey_index]);

                                // Take the group keys by the groupkey indices
                                let groupkeys_table =
                                    groupby_table.take(&groupkey_indices_as_arr)?;

                                // Broadcast the group keys to the length of the grouped column,
                                // because output of UDF can be more than one row
                                let broadcasted_groupkeys = groupkeys_table
                                    .columns
                                    .iter()
                                    .map(|c| {
                                        c.broadcast(evaluated_grouped_col.len())
                                            .map(|c| c.take_materialized_series())
                                    })
                                    .collect::<DaftResult<Vec<_>>>()?;

                                Self::from_nonempty_columns(broadcasted_groupkeys)?
                            };

                            Ok((broadcasted_groupkeys_table, evaluated_grouped_col))
                        })
                        .collect::<DaftResult<Vec<_>>>()?;

                    let series_refs = grouped_results.iter().map(|(_, s)| s).collect::<Vec<_>>();
                    let concatenated_grouped_col = Series::concat(series_refs.as_slice())?;

                    let table_refs = grouped_results.iter().map(|(t, _)| t).collect::<Vec<_>>();
                    let concatenated_groupkeys_table = Self::concat(table_refs.as_slice())?;

                    (concatenated_groupkeys_table, concatenated_grouped_col)
                }
            }
            MapGroupsFn::Python(py_scalar_fn) => {
                match py_scalar_fn {
                    PyScalarFn::RowWise(_) => {
                        return Err(DaftError::ComputeError(
                            "Row-wise Python UDFs are not supported in map_groups; use daft.func.batch or @daft.method.batch instead.".to_string(),
                        ));
                    }
                    PyScalarFn::Batch(_) => {}
                }

                if groupvals_indices.is_empty() {
                    let empty_groupkeys_table = Self::empty(Some(groupby_table.schema));
                    let output_name = evaluated_inputs
                        .first()
                        .map_or_else(|| "output", |s| s.name());
                    let empty_udf_output_col = Series::empty(output_name, &py_scalar_fn.dtype());
                    (empty_groupkeys_table, empty_udf_output_col)
                } else if groupvals_indices.len() == 1 {
                    let mut metrics = NoopMetricsCollector;
                    let grouped_col = if py_scalar_fn.is_async() {
                        get_compute_runtime().block_on_current_thread(
                            py_scalar_fn.call_async(evaluated_inputs.as_slice(), &mut metrics),
                        )?
                    } else {
                        py_scalar_fn.call(evaluated_inputs.as_slice(), &mut metrics)?
                    };

                    let groupkeys_table = {
                        let indices_as_arr = UInt64Array::from_vec("", groupkey_indices);
                        groupby_table.take(&indices_as_arr)?
                    };
                    (groupkeys_table, grouped_col)
                } else {
                    let grouped_results = groupkey_indices
                        .iter()
                        .zip(groupvals_indices.iter())
                        .map(|(groupkey_index, groupval_indices)| {
                            // Convert group indices to Series
                            let indices_as_arr =
                                UInt64Array::from_vec("", groupval_indices.to_vec());

                            // Take each input Series by the group indices
                            let input_groups = evaluated_inputs
                                .iter()
                                .map(|s| s.take(&indices_as_arr))
                                .collect::<DaftResult<Vec<_>>>()?;

                            let mut metrics = NoopMetricsCollector;
                            let evaluated_grouped_col = if py_scalar_fn.is_async() {
                                get_compute_runtime().block_on_current_thread(
                                    py_scalar_fn.call_async(input_groups.as_slice(), &mut metrics),
                                )?
                            } else {
                                py_scalar_fn.call(input_groups.as_slice(), &mut metrics)?
                            };

                            let broadcasted_groupkeys_table = {
                                // Convert groupkey indices to Series
                                let groupkey_indices_as_arr =
                                    UInt64Array::from_slice("", &[*groupkey_index]);

                                // Take the group keys by the groupkey indices
                                let groupkeys_table =
                                    groupby_table.take(&groupkey_indices_as_arr)?;

                                // Broadcast the group keys to the length of the grouped column
                                let broadcasted_groupkeys = groupkeys_table
                                    .columns
                                    .iter()
                                    .map(|c| {
                                        c.broadcast(evaluated_grouped_col.len())
                                            .map(|c| c.take_materialized_series())
                                    })
                                    .collect::<DaftResult<Vec<_>>>()?;

                                Self::from_nonempty_columns(broadcasted_groupkeys)?
                            };

                            Ok((broadcasted_groupkeys_table, evaluated_grouped_col))
                        })
                        .collect::<DaftResult<Vec<_>>>()?;

                    let series_refs = grouped_results.iter().map(|(_, s)| s).collect::<Vec<_>>();
                    let concatenated_grouped_col = Series::concat(series_refs.as_slice())?;

                    let table_refs = grouped_results.iter().map(|(t, _)| t).collect::<Vec<_>>();
                    let concatenated_groupkeys_table = Self::concat(table_refs.as_slice())?;

                    (concatenated_groupkeys_table, concatenated_grouped_col)
                }
            }
        };

        // Broadcast either the keys or the grouped_cols, depending on which is unit-length
        let final_len = grouped_col.len();
        let groupkeys_series: Vec<Series> = groupkeys_table
            .columns
            .iter()
            .map(|c| c.as_materialized_series().clone())
            .collect();
        let final_columns = [groupkeys_series.as_slice(), &[grouped_col]].concat();
        let final_schema = Schema::new(final_columns.iter().map(|s| s.field().clone()));
        Self::new_with_broadcast(final_schema, final_columns, final_len)
    }

    pub fn dedup(&self, columns: &[BoundExpr]) -> DaftResult<Self> {
        if columns.is_empty() {
            return Err(DaftError::ValueError(
                "Attempting to dedup RecordBatch on no columns".to_string(),
            ));
        }

        let dedup_table = self.eval_expression_list(columns)?;
        let unique_indices = dedup_table.make_unique_idxs()?;
        let indices_as_arr = UInt64Array::from_vec("", unique_indices);
        self.take(&indices_as_arr)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::prelude::*;
    use daft_dsl::{
        AggExpr,
        expr::bound_expr::{BoundAggExpr, BoundExpr},
        functions::{AggFn, AggFnHandle, State},
        unresolved_col,
    };

    use crate::{RecordBatch, ops::get_column_by_name};

    /// A sum UDAF that exercises the full three-stage pipeline.
    #[derive(serde::Serialize, serde::Deserialize)]
    struct TestSumAgg;

    #[typetag::serde(name = "TestSumAgg")]
    #[allow(clippy::unnecessary_literal_bound)]
    impl AggFn for TestSumAgg {
        fn name(&self) -> &str {
            "test_sum"
        }

        fn return_dtype(&self, input_types: &[DataType]) -> DaftResult<DataType> {
            Ok(input_types[0].clone())
        }

        fn state_fields(&self, _inputs: &[Field]) -> DaftResult<Vec<Field>> {
            Ok(vec![Field::new("sum", DataType::Int64)])
        }

        fn call_agg_block(&self, inputs: Vec<Series>) -> DaftResult<Vec<State>> {
            let sum: i64 = inputs[0].i64()?.into_iter().map(|v| v.unwrap_or(0)).sum();
            Ok(vec![Literal::Int64(sum)])
        }

        fn call_agg_combine(&self, states: Vec<Series>) -> DaftResult<Vec<State>> {
            let sum: i64 = states[0].i64()?.into_iter().map(|v| v.unwrap_or(0)).sum();
            Ok(vec![Literal::Int64(sum)])
        }

        fn call_agg_finalize(&self, state: Vec<State>) -> DaftResult<State> {
            Ok(state.into_iter().next().unwrap_or(Literal::Null))
        }
    }

    fn make_handle() -> AggFnHandle {
        AggFnHandle::new(Arc::new(TestSumAgg))
    }

    fn partial_col_name() -> String {
        format!("{}({})", "test_sum", "x")
    }

    fn bound_block(rb: &RecordBatch) -> BoundAggExpr {
        BoundAggExpr::try_new(
            AggExpr::AggFnMap {
                handle: make_handle(),
                inputs: vec![unresolved_col("x")],
            },
            &rb.schema,
        )
        .unwrap()
    }

    fn bound_combine(schema: &daft_core::prelude::SchemaRef) -> BoundAggExpr {
        let return_field = Field::new("x", DataType::Int64);
        BoundAggExpr::try_new(
            AggExpr::AggFnReduce {
                handle: make_handle(),
                partial: unresolved_col(&*partial_col_name()),
                return_field,
            },
            schema,
        )
        .unwrap()
    }

    fn bound_combine_only(schema: &daft_core::prelude::SchemaRef) -> BoundAggExpr {
        BoundAggExpr::try_new(
            AggExpr::AggFnCombine {
                handle: make_handle(),
                partial: unresolved_col(&*partial_col_name()),
            },
            schema,
        )
        .unwrap()
    }

    // Global: two batches (one with a null). Null=0 per TestSumAgg, so 1+null+2+3 = 6.
    #[test]
    fn test_agg_fn_global() -> DaftResult<()> {
        let rb1 = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_iter(
                Arc::new(Field::new("x", DataType::Int64)),
                [Some(1i64), None, Some(2i64)],
            )
            .into_series(),
        ])?;
        let rb2 = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_vec("x", vec![3i64]).into_series(),
        ])?;

        let partial1 = rb1.agg_global(&[bound_block(&rb1)])?;
        let partial2 = rb2.agg_global(&[bound_block(&rb2)])?;
        let merged = RecordBatch::concat(&[partial1, partial2])?;
        assert_eq!(merged.len(), 2);

        let result = merged.agg_global(&[bound_combine(&merged.schema)])?;
        assert_eq!(result.len(), 1);
        let col = get_column_by_name(&result, "x")?;
        assert_eq!(col.i64()?.get(0), Some(6i64));
        Ok(())
    }

    // Three shards (shard3 has null in group "a"). Null=0, so a=1+2+3+null=6, b=10+20+5=35.
    #[test]
    fn test_agg_fn_grouped() -> DaftResult<()> {
        let shard1 = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_vec("x", vec![1i64, 2, 10]).into_series(),
            Utf8Array::from_slice("g", &["a", "a", "b"]).into_series(),
        ])?;
        let shard2 = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_vec("x", vec![3i64, 20]).into_series(),
            Utf8Array::from_slice("g", &["a", "b"]).into_series(),
        ])?;
        let shard3 = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_iter(
                Arc::new(Field::new("x", DataType::Int64)),
                [None::<i64>, Some(5i64)],
            )
            .into_series(),
            Utf8Array::from_slice("g", &["a", "b"]).into_series(),
        ])?;

        let bound_g_s1 = BoundExpr::try_new(unresolved_col("g"), &shard1.schema)?;
        let bound_g_s2 = BoundExpr::try_new(unresolved_col("g"), &shard2.schema)?;
        let bound_g_s3 = BoundExpr::try_new(unresolved_col("g"), &shard3.schema)?;

        let partial1 = shard1.agg(&[bound_block(&shard1)], &[bound_g_s1])?;
        let partial2 = shard2.agg(&[bound_block(&shard2)], &[bound_g_s2])?;
        let partial3 = shard3.agg(&[bound_block(&shard3)], &[bound_g_s3])?;

        // Simulates shuffle merge: all partial states land in one reduce step.
        let merged = RecordBatch::concat(&[partial1, partial2, partial3])?;

        let bound_g_m = BoundExpr::try_new(unresolved_col("g"), &merged.schema)?;
        let result = merged.agg(&[bound_combine(&merged.schema)], &[bound_g_m])?;
        assert_eq!(result.len(), 2);

        let g_col = get_column_by_name(&result, "g")?;
        let x_col = get_column_by_name(&result, "x")?;
        let mut pairs: Vec<(String, i64)> = (0..result.len())
            .map(|i| {
                Ok::<_, common_error::DaftError>((
                    g_col.utf8()?.get(i).unwrap().to_string(),
                    x_col.i64()?.get(i).unwrap(),
                ))
            })
            .collect::<DaftResult<_>>()?;
        pairs.sort();

        assert_eq!(pairs, vec![("a".into(), 6i64), ("b".into(), 35i64)]);
        Ok(())
    }

    // Verifies the deserialized handle produces the same pipeline results as the original.
    #[test]
    fn test_agg_fn_handle_serde_roundtrip() -> DaftResult<()> {
        let original = make_handle();
        let json = serde_json::to_string(&original).unwrap();
        let restored: AggFnHandle = serde_json::from_str(&json).unwrap();
        assert_eq!(original.name(), restored.name());

        let rb = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_vec("x", vec![4i64, 5, 6]).into_series(),
        ])?;
        let block_expr = BoundAggExpr::try_new(
            AggExpr::AggFnMap {
                handle: restored.clone(),
                inputs: vec![unresolved_col("x")],
            },
            &rb.schema,
        )
        .unwrap();
        let partial = rb.agg_global(&[block_expr])?;

        let return_field = Field::new("x", DataType::Int64);
        let reduce_expr = BoundAggExpr::try_new(
            AggExpr::AggFnReduce {
                handle: restored,
                partial: unresolved_col(&*partial_col_name()),
                return_field,
            },
            &partial.schema,
        )
        .unwrap();
        let result = partial.agg_global(&[reduce_expr])?;
        let col = get_column_by_name(&result, "x")?;
        assert_eq!(col.i64()?.get(0), Some(15i64)); // 4+5+6
        Ok(())
    }

    #[test]
    fn test_agg_combine_only_preserves_struct() -> DaftResult<()> {
        let rb1 = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_vec("x", vec![1i64, 2]).into_series(),
        ])?;
        let rb2 = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_vec("x", vec![3i64]).into_series(),
        ])?;
        let partial1 = rb1.agg_global(&[bound_block(&rb1)])?;
        let partial2 = rb2.agg_global(&[bound_block(&rb2)])?;
        let merged = RecordBatch::concat(&[partial1, partial2])?;
        assert_eq!(merged.len(), 2);

        let combined = merged.agg_global(&[bound_combine_only(&merged.schema)])?;
        assert_eq!(combined.len(), 1);
        let col = get_column_by_name(&combined, &partial_col_name())?;
        assert!(
            matches!(col.data_type(), DataType::Struct(_)),
            "should remain Struct after combine-only, got {}",
            col.data_type()
        );

        // Now finalize — result should be 1+2+3 = 6 typed as Int64.
        let final_result = combined.agg_global(&[bound_combine(&combined.schema)])?;
        let x = get_column_by_name(&final_result, "x")?;
        assert_eq!(x.i64()?.get(0), Some(6i64));
        Ok(())
    }

    #[test]
    fn test_agg_combine_only_grouped() -> DaftResult<()> {
        let shard1 = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_vec("x", vec![10i64, 1]).into_series(),
            Utf8Array::from_slice("g", &["a", "b"]).into_series(),
        ])?;
        let shard2 = RecordBatch::from_nonempty_columns(vec![
            Int64Array::from_vec("x", vec![20i64, 2]).into_series(),
            Utf8Array::from_slice("g", &["a", "b"]).into_series(),
        ])?;

        let bound_g = |rb: &RecordBatch| BoundExpr::try_new(unresolved_col("g"), &rb.schema);
        let partial1 = shard1.agg(&[bound_block(&shard1)], &[bound_g(&shard1)?])?;
        let partial2 = shard2.agg(&[bound_block(&shard2)], &[bound_g(&shard2)?])?;
        let merged = RecordBatch::concat(&[partial1, partial2])?;

        let bound_g_m = BoundExpr::try_new(unresolved_col("g"), &merged.schema)?;
        let combined = merged.agg(
            &[bound_combine_only(&merged.schema)],
            std::slice::from_ref(&bound_g_m),
        )?;
        assert_eq!(combined.len(), 2);
        let partial_col = get_column_by_name(&combined, &partial_col_name())?;
        assert!(matches!(partial_col.data_type(), DataType::Struct(_)));

        // Finalize the combined state — a: 10+20=30, b: 1+2=3.
        let bound_g_c = BoundExpr::try_new(unresolved_col("g"), &combined.schema)?;
        let final_result = combined.agg(&[bound_combine(&combined.schema)], &[bound_g_c])?;
        let g_col = get_column_by_name(&final_result, "g")?;
        let x_col = get_column_by_name(&final_result, "x")?;
        let mut pairs: Vec<(String, i64)> = (0..final_result.len())
            .map(|i| {
                Ok::<_, common_error::DaftError>((
                    g_col.utf8()?.get(i).unwrap().to_string(),
                    x_col.i64()?.get(i).unwrap(),
                ))
            })
            .collect::<DaftResult<_>>()?;
        pairs.sort();
        assert_eq!(pairs, vec![("a".into(), 30i64), ("b".into(), 3i64)]);
        Ok(())
    }
}
