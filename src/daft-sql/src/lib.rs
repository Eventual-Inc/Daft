#![feature(let_chains)]

pub mod error;
pub mod functions;

mod exec;
mod modules;
mod planner;
mod schema;
mod statement;
mod table_provider;

pub use planner::*;
#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<python::PySqlCatalog>()?;
    parent.add_function(wrap_pyfunction!(python::sql_exec, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::sql_expr, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::sql_datatype, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::list_sql_functions, parent)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, LazyLock};

    use common_error::DaftError;
    use daft_core::prelude::*;
    use daft_dsl::{lit, unresolved_col, Expr, ExprRef, PlanRef, Subquery, UnresolvedColumn};
    use daft_logical_plan::{
        logical_plan::Source, source_info::PlaceHolderInfo, ClusteringSpec, JoinOptions,
        LogicalPlan, LogicalPlanBuilder, LogicalPlanRef, SourceInfo,
    };
    use daft_session::Session;
    use error::SQLPlannerResult;
    use rstest::{fixture, rstest};

    use super::*;
    use crate::{error::PlannerError, planner::SQLPlanner};

    #[fixture]
    fn tbl_1() -> LogicalPlanRef {
        let schema = Arc::new(Schema::new(vec![
            Field::new("test", DataType::Utf8),
            Field::new("utf8", DataType::Utf8),
            Field::new("i32", DataType::Int32),
            Field::new("i64", DataType::Int64),
            Field::new("f32", DataType::Float32),
            Field::new("f64", DataType::Float64),
            Field::new("bool", DataType::Boolean),
            Field::new("date", DataType::Date),
            Field::new("time", DataType::Time(TimeUnit::Microseconds)),
            Field::new("list_utf8", DataType::new_list(DataType::Utf8)),
        ]));
        LogicalPlan::Source(Source::new(
            schema.clone(),
            Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
                source_schema: schema,
                clustering_spec: Arc::new(ClusteringSpec::unknown()),
            })),
        ))
        .arced()
    }

    #[fixture]
    fn tbl_2() -> LogicalPlanRef {
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8),
            Field::new("id", DataType::Int32),
            Field::new("val", DataType::Int32),
        ]));
        LogicalPlan::Source(Source::new(
            schema.clone(),
            Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
                source_schema: schema,
                clustering_spec: Arc::new(ClusteringSpec::unknown()),
            })),
        ))
        .arced()
    }

    #[fixture]
    fn tbl_3() -> LogicalPlanRef {
        let schema = Arc::new(Schema::new(vec![
            Field::new("first_name", DataType::Utf8),
            Field::new("last_name", DataType::Utf8),
            Field::new("id", DataType::Int32),
        ]));
        LogicalPlan::Source(Source::new(
            schema.clone(),
            Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
                source_schema: schema,
                clustering_spec: Arc::new(ClusteringSpec::unknown()),
            })),
        ))
        .arced()
    }

    #[fixture]
    fn planner() -> SQLPlanner<'static> {
        static SESSION: LazyLock<Session> = LazyLock::new(|| {
            let session = Session::default();
            // construct views from the tables and attach to the session
            session
                .create_temp_table("tbl1", &tbl_1().into(), false)
                .unwrap();
            session
                .create_temp_table("tbl2", &tbl_2().into(), false)
                .unwrap();
            session
                .create_temp_table("tbl3", &tbl_3().into(), false)
                .unwrap();

            session
        });

        SQLPlanner::new(&SESSION)
    }

    #[rstest]
    #[case::basic("select * from tbl1")]
    #[case::select_with_limit("select * from tbl1 limit 1")]
    #[case::exclude("select * exclude utf8 from tbl1")]
    #[case::exclude2("select * exclude (utf8, i32) from tbl1")]
    #[case("select utf8 from tbl1")]
    #[case("select i32 from tbl1")]
    #[case("select i64 from tbl1")]
    #[case("select f32 from tbl1")]
    #[case("select f64 from tbl1")]
    #[case("select bool from tbl1")]
    #[case("select date from tbl1")]
    #[case("select bool::text from tbl1")]
    #[case("select cast(bool as text) from tbl1")]
    #[case("select list_utf8 as a, utf8 as b, utf8 as c from tbl1")]
    #[case("select list_utf8::text[] from tbl1")]
    #[case("select list_utf8[0] from tbl1")]
    #[case::slice("select list_utf8[0:2] from tbl1")]
    #[case::join("select * from tbl2 join tbl3 on tbl2.id = tbl3.id")]
    #[case::null_safe_join("select * from tbl2 left join tbl3 on tbl2.id <=> tbl3.id")]
    #[case::join_with_filter("select * from tbl2 join tbl3 on tbl2.id = tbl3.id and tbl2.val > 0")]
    #[case::from("select tbl2.text from tbl2")]
    #[case::using("select tbl2.text from tbl2 join tbl3 using (id)")]
    #[case::orderby("select * from tbl1 order by i32")]
    #[case::orderby("select * from tbl1 order by i32 desc")]
    #[case::orderby("select * from tbl1 order by i32 asc")]
    #[case::orderby_multi("select * from tbl1 order by i32 desc, f32 asc")]
    #[case::whenthen("select case when i32 = 1 then 'a' else 'b' end from tbl1")]
    #[case::cte("with cte as (select * from tbl1) select * from cte")]
    #[case::double_alias("select * from tbl1 as tbl2, tbl2 as tbl1")]
    #[case::double_alias_qualified("select tbl1.val from tbl1 as tbl2, tbl2 as tbl1")]
    #[case::interval_arithmetic("select interval '1 day' * 3 from tbl1")]
    fn test_compiles(mut planner: SQLPlanner, #[case] query: &str) -> SQLPlannerResult<()> {
        let plan = planner.plan_sql(query);
        assert!(&plan.is_ok(), "query: {query}\nerror: {plan:?}");

        Ok(())
    }

    #[rstest]
    fn test_compile_from_read_parquet(mut planner: SQLPlanner) -> SQLPlannerResult<()> {
        let query = "select * from read_parquet('../../tests/assets/parquet-data/mvp.parquet')";
        let plan = planner.plan_sql(query);
        assert!(&plan.is_ok(), "query: {query}\nerror: {plan:?}");

        Ok(())
    }

    #[rstest]
    fn test_compile_from_read_csv(mut planner: SQLPlanner) -> SQLPlannerResult<()> {
        let query =
            "select * from read_csv('../../tests/assets/sampled-tpch.csv', delimiter => ',')";
        let plan = planner.plan_sql(query);
        assert!(&plan.is_ok(), "query: {query}\nerror: {plan:?}");

        Ok(())
    }

    #[rstest]
    fn test_parse_sql(mut planner: SQLPlanner, tbl_1: LogicalPlanRef) {
        let sql = "select test as a from tbl1";
        let plan = planner.plan_sql(sql).unwrap();

        let expected = LogicalPlanBuilder::from(tbl_1)
            .alias("tbl1")
            .select(vec![unresolved_col("test").alias("a")])
            .unwrap()
            .build();
        assert_eq!(plan, expected);
    }

    #[rstest]
    fn test_where_clause(mut planner: SQLPlanner, tbl_1: LogicalPlanRef) -> SQLPlannerResult<()> {
        let sql = "select test as a from tbl1 where test = 'a'";
        let plan = planner.plan_sql(sql)?;

        let expected = LogicalPlanBuilder::from(tbl_1)
            .alias("tbl1")
            .filter(unresolved_col("test").eq(lit("a")))?
            .select(vec![unresolved_col("test").alias("a")])?
            .build();

        assert_eq!(plan, expected);
        Ok(())
    }

    #[rstest]
    fn test_limit(mut planner: SQLPlanner, tbl_1: LogicalPlanRef) -> SQLPlannerResult<()> {
        let sql = "select test as a from tbl1 limit 10";
        let plan = planner.plan_sql(sql)?;

        let expected = LogicalPlanBuilder::from(tbl_1)
            .alias("tbl1")
            .select(vec![unresolved_col("test").alias("a")])?
            .limit(10, true)?
            .build();

        assert_eq!(plan, expected);
        Ok(())
    }

    #[rstest]
    fn test_negative_limit(mut planner: SQLPlanner) -> SQLPlannerResult<()> {
        let sql = "select test as a from tbl1 limit -1";
        let plan = planner.plan_sql(sql);
        match plan {
            Err(PlannerError::InvalidOperation { message }) => {
                assert_eq!(
                    message,
                    "LIMIT <n> must be greater than or equal to 0, instead got: -1"
                );
            }
            _ => panic!("Unexpected result"),
        }

        Ok(())
    }

    #[rstest]
    fn test_orderby(mut planner: SQLPlanner, tbl_1: LogicalPlanRef) -> SQLPlannerResult<()> {
        let sql = "select utf8 from tbl1 order by utf8 desc";
        let plan = planner.plan_sql(sql)?;

        let expected = LogicalPlanBuilder::from(tbl_1)
            .alias("tbl1")
            .sort(vec![unresolved_col("utf8")], vec![true], vec![true])?
            .select(vec![unresolved_col("utf8")])?
            .build();

        assert_eq!(plan, expected);
        Ok(())
    }

    #[rstest(
        null_equals_null => [false, true]
    )]
    fn test_join(
        mut planner: SQLPlanner,
        tbl_2: LogicalPlanRef,
        tbl_3: LogicalPlanRef,
        null_equals_null: bool,
    ) -> SQLPlannerResult<()> {
        let sql = format!(
            "select * from tbl2 join tbl3 on tbl2.id {} tbl3.id",
            if null_equals_null { "<=>" } else { "=" }
        );
        let plan = planner.plan_sql(&sql)?;

        let left_on: ExprRef = UnresolvedColumn {
            name: "id".into(),
            plan_ref: PlanRef::Alias("tbl2".into()),
            plan_schema: None,
        }
        .into();
        let right_on: ExprRef = UnresolvedColumn {
            name: "id".into(),
            plan_ref: PlanRef::Alias("tbl3".into()),
            plan_schema: None,
        }
        .into();

        let on = if null_equals_null {
            left_on.eq_null_safe(right_on)
        } else {
            left_on.eq(right_on)
        };

        let expected = LogicalPlanBuilder::from(tbl_2)
            .alias("tbl2")
            .join(
                LogicalPlanBuilder::from(tbl_3).alias("tbl3"),
                on.into(),
                vec![],
                JoinType::Inner,
                None,
                JoinOptions::default().prefix("tbl3."),
            )?
            .select(vec![unresolved_col("*")])?
            .build();
        assert_eq!(plan, expected);
        Ok(())
    }

    #[rstest]
    fn test_join_with_filter(
        mut planner: SQLPlanner,
        tbl_2: LogicalPlanRef,
        tbl_3: LogicalPlanRef,
    ) -> SQLPlannerResult<()> {
        let sql = "select * from tbl2 join tbl3 on tbl2.id = tbl3.id and tbl2.val > 0";
        let plan = planner.plan_sql(sql)?;

        let tbl2_id: ExprRef = UnresolvedColumn {
            name: "id".into(),
            plan_ref: PlanRef::Alias("tbl2".into()),
            plan_schema: None,
        }
        .into();
        let tbl3_id: ExprRef = UnresolvedColumn {
            name: "id".into(),
            plan_ref: PlanRef::Alias("tbl3".into()),
            plan_schema: None,
        }
        .into();
        let tbl2_val: ExprRef = UnresolvedColumn {
            name: "val".into(),
            plan_ref: PlanRef::Alias("tbl2".into()),
            plan_schema: None,
        }
        .into();

        let expected = LogicalPlanBuilder::from(tbl_2)
            .alias("tbl2")
            .join(
                LogicalPlanBuilder::from(tbl_3).alias("tbl3"),
                (tbl2_id.eq(tbl3_id)).and(tbl2_val.gt(lit(0 as i64))).into(),
                vec![],
                JoinType::Inner,
                None,
                JoinOptions::default().prefix("tbl3."),
            )?
            .select(vec![unresolved_col("*")])?
            .build();
        assert_eq!(plan, expected);
        Ok(())
    }

    #[rstest]
    fn test_global_agg(mut planner: SQLPlanner, tbl_1: LogicalPlanRef) -> SQLPlannerResult<()> {
        let sql = "select max(i32) from tbl1";
        let plan = planner.plan_sql(sql)?;

        let expected = LogicalPlanBuilder::from(tbl_1)
            .alias("tbl1")
            .aggregate(vec![unresolved_col("i32").max()], vec![])?
            .select(vec![unresolved_col("i32")])?
            .build();

        assert_eq!(plan, expected);
        Ok(())
    }

    #[rstest]
    #[case::basic("select utf8 from tbl1 order by utf8")]
    #[case::asc("select utf8 from tbl1 order by utf8 asc")]
    #[case::desc("select utf8 from tbl1 order by utf8 desc")]
    #[case::with_alias("select utf8 as a from tbl1 order by a")]
    #[case::with_alias_in_projection_only("select utf8 as a from tbl1 order by utf8")]
    #[case::with_groupby("select utf8, sum(i32) from tbl1 group by utf8 order by utf8")]
    #[case::with_groupby_and_alias(
        "select utf8 as a, sum(i32) from tbl1 group by utf8 order by utf8"
    )]
    #[case::with_groupby_and_alias_mixed("select utf8 as a from tbl1 group by a order by utf8")]
    #[case::with_groupby_and_alias_mixed_2("select utf8 as a from tbl1 group by utf8 order by a")]
    #[case::with_groupby_and_alias_mixed_asc(
        "select utf8 as a from tbl1 group by utf8 order by a asc"
    )]
    fn test_compiles_orderby(mut planner: SQLPlanner, #[case] query: &str) -> SQLPlannerResult<()> {
        let plan = planner.plan_sql(query);
        if let Err(e) = plan {
            panic!("query: {query}\nerror: {e:?}");
        }
        assert!(plan.is_ok(), "query: {query}\nerror: {plan:?}");

        Ok(())
    }

    #[rstest]
    #[case::with_second_select("select i32 as a from tbl1 where i32 > 0")]
    #[case::with_where("select i32 as a from tbl1 where i32 > 0")]
    #[case::with_where_aliased("select i32 as a from tbl1 where a > 0")]
    #[case::with_groupby("select i32 as a from tbl1 group by i32")]
    #[case::with_groupby_aliased("select i32 as a from tbl1 group by a")]
    #[case::with_orderby("select i32 as a from tbl1 order by i32")]
    #[case::with_orderby_aliased("select i32 as a from tbl1 order by a")]
    #[case::with_many("select i32 as a from tbl1 where i32 > 0 group by i32 order by i32")]
    #[case::with_many_aliased("select i32 as a from tbl1 where a > 0 group by a order by a")]
    #[case::second_select("select i32 as a, a + 1 from tbl1")]
    fn test_compiles_select_alias(
        mut planner: SQLPlanner,
        #[case] query: &str,
    ) -> SQLPlannerResult<()> {
        let plan = planner.plan_sql(query);
        if let Err(e) = plan {
            panic!("query: {query}\nerror: {e:?}");
        }
        assert!(plan.is_ok(), "query: {query}\nerror: {plan:?}");

        Ok(())
    }

    #[rstest]
    #[case::basic(
        "select utf8 from tbl1 where i64 > (select max(id) from tbl2 where id = i32)",
        PlanRef::Unqualified
    )]
    #[case::compound(
        "select utf8 from tbl1 where i64 > (select max(id) from tbl2 where id = tbl1.i32)",
        PlanRef::Alias("tbl1".into())
    )]
    fn test_correlated_subquery(
        mut planner: SQLPlanner,
        #[case] query: &str,
        #[case] plan_ref: PlanRef,
        tbl_1: LogicalPlanRef,
        tbl_2: LogicalPlanRef,
    ) -> SQLPlannerResult<()> {
        use daft_dsl::{Column, ResolvedColumn};

        let plan = planner.plan_sql(query)?;

        let outer_col = Arc::new(Expr::Column(Column::Resolved(ResolvedColumn::OuterRef(
            Field::new("i32", DataType::Int32),
            plan_ref,
        ))));
        let subquery = LogicalPlanBuilder::from(tbl_2)
            .alias("tbl2")
            .filter(unresolved_col("id").eq(outer_col))?
            .aggregate(vec![unresolved_col("id").max()], vec![])?
            .select(vec![unresolved_col("id")])?
            .build();

        let subquery = Arc::new(Expr::Subquery(Subquery { plan: subquery }));

        let expected = LogicalPlanBuilder::from(tbl_1)
            .alias("tbl1")
            .filter(unresolved_col("i64").gt(subquery))?
            .select(vec![unresolved_col("utf8")])?
            .build();

        assert_eq!(plan, expected);

        Ok(())
    }

    #[rstest]
    fn test_multiple_from_with_join(
        mut planner: SQLPlanner,
        tbl_1: LogicalPlanRef,
        tbl_2: LogicalPlanRef,
        tbl_3: LogicalPlanRef,
    ) -> SQLPlannerResult<()> {
        let sql = "select tbl2.val from tbl1 left join tbl2 on tbl1.utf8 = tbl2.text, (tbl1 as tbl4) right join tbl3 on tbl4.i32 = tbl3.id";
        let plan = planner.plan_sql(sql)?;

        let first_from = LogicalPlanBuilder::from(tbl_1.clone()).alias("tbl1").join(
            LogicalPlanBuilder::from(tbl_2).alias("tbl2"),
            unresolved_col("utf8").eq(unresolved_col("text")).into(),
            vec![],
            JoinType::Left,
            None,
            JoinOptions::default().prefix("tbl2."),
        )?;

        let second_from = LogicalPlanBuilder::from(tbl_1)
            .alias("tbl1")
            .alias("tbl4")
            .join(
                LogicalPlanBuilder::from(tbl_3).alias("tbl3"),
                unresolved_col("i32").eq(unresolved_col("id")).into(),
                vec![],
                JoinType::Right,
                None,
                JoinOptions::default().prefix("tbl3."),
            )?;

        let expected = first_from
            .cross_join(second_from, JoinOptions::default())?
            .select(vec![unresolved_col("val")])?
            .build();

        assert_eq!(plan, expected);

        Ok(())
    }

    #[rstest]
    #[case::basic("select tbl1.test from tbl1 as tbl2")]
    #[case::subquery("select tbl1.test from (select * from tbl1) as tbl2")]
    fn test_subquery_alias_bad_scope(
        mut planner: SQLPlanner,
        #[case] query: &str,
    ) -> SQLPlannerResult<()> {
        let result = planner.plan_sql(query);

        assert!(result.is_err_and(|e| { matches!(e, PlannerError::ColumnNotFound { .. }) }));

        Ok(())
    }
}
