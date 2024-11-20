#![feature(let_chains)]

pub mod catalog;
pub mod error;
pub mod functions;
mod modules;
mod planner;
#[cfg(feature = "python")]
pub mod python;
mod table_provider;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<python::PyCatalog>()?;
    parent.add_function(wrap_pyfunction_bound!(python::sql, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(python::sql_expr, parent)?)?;
    parent.add_function(wrap_pyfunction_bound!(python::list_sql_functions, parent)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use catalog::SQLCatalog;
    use daft_core::prelude::*;
    use daft_dsl::{col, lit, Expr, OuterReferenceColumn, Subquery};
    use daft_logical_plan::{
        logical_plan::Source, source_info::PlaceHolderInfo, ClusteringSpec, LogicalPlan,
        LogicalPlanBuilder, LogicalPlanRef, SourceInfo,
    };
    use error::SQLPlannerResult;
    use rstest::{fixture, rstest};

    use super::*;
    use crate::planner::SQLPlanner;

    #[fixture]
    fn tbl_1() -> LogicalPlanRef {
        let schema = Arc::new(
            Schema::new(vec![
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
            ])
            .unwrap(),
        );
        LogicalPlan::Source(Source {
            output_schema: schema.clone(),
            source_info: Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
                source_schema: schema,
                clustering_spec: Arc::new(ClusteringSpec::unknown()),
                source_id: 0,
            })),
        })
        .arced()
    }

    #[fixture]
    fn tbl_2() -> LogicalPlanRef {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("text", DataType::Utf8),
                Field::new("id", DataType::Int32),
            ])
            .unwrap(),
        );
        LogicalPlan::Source(Source {
            output_schema: schema.clone(),
            source_info: Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
                source_schema: schema,
                clustering_spec: Arc::new(ClusteringSpec::unknown()),
                source_id: 0,
            })),
        })
        .arced()
    }

    #[fixture]
    fn tbl_3() -> LogicalPlanRef {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("first_name", DataType::Utf8),
                Field::new("last_name", DataType::Utf8),
                Field::new("id", DataType::Int32),
            ])
            .unwrap(),
        );
        LogicalPlan::Source(Source {
            output_schema: schema.clone(),
            source_info: Arc::new(SourceInfo::PlaceHolder(PlaceHolderInfo {
                source_schema: schema,
                clustering_spec: Arc::new(ClusteringSpec::unknown()),
                source_id: 0,
            })),
        })
        .arced()
    }

    #[fixture]
    fn planner() -> SQLPlanner<'static> {
        let mut catalog = SQLCatalog::new();

        catalog.register_table("tbl1", tbl_1());
        catalog.register_table("tbl2", tbl_2());
        catalog.register_table("tbl3", tbl_3());

        SQLPlanner::new(catalog)
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
    #[case::from("select tbl2.text from tbl2")]
    #[case::using("select tbl2.text from tbl2 join tbl3 using (id)")]
    #[case(
        r"
    select
        abs(i32) as abs,
        ceil(i32) as ceil,
        floor(i32) as floor,
        sign(i32) as sign
    from tbl1"
    )]
    #[case("select round(i32, 1) from tbl1")]
    #[case::groupby("select max(i32) from tbl1 group by utf8")]
    #[case::orderby("select * from tbl1 order by i32")]
    #[case::orderby("select * from tbl1 order by i32 desc")]
    #[case::orderby("select * from tbl1 order by i32 asc")]
    #[case::orderby_multi("select * from tbl1 order by i32 desc, f32 asc")]
    #[case::whenthen("select case when i32 = 1 then 'a' else 'b' end from tbl1")]
    #[case::globalagg("select max(i32) from tbl1")]
    #[case::cte("with cte as (select * from tbl1) select * from cte")]
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

        let expected = LogicalPlanBuilder::new(tbl_1, None)
            .select(vec![col("test").alias("a")])
            .unwrap()
            .build();
        assert_eq!(plan, expected);
    }

    #[rstest]
    fn test_where_clause(mut planner: SQLPlanner, tbl_1: LogicalPlanRef) -> SQLPlannerResult<()> {
        let sql = "select test as a from tbl1 where test = 'a'";
        let plan = planner.plan_sql(sql)?;

        let expected = LogicalPlanBuilder::new(tbl_1, None)
            .filter(col("test").eq(lit("a")))?
            .select(vec![col("test").alias("a")])?
            .build();

        assert_eq!(plan, expected);
        Ok(())
    }
    #[rstest]
    fn test_limit(mut planner: SQLPlanner, tbl_1: LogicalPlanRef) -> SQLPlannerResult<()> {
        let sql = "select test as a from tbl1 limit 10";
        let plan = planner.plan_sql(sql)?;

        let expected = LogicalPlanBuilder::new(tbl_1, None)
            .select(vec![col("test").alias("a")])?
            .limit(10, true)?
            .build();

        assert_eq!(plan, expected);
        Ok(())
    }

    #[rstest]
    fn test_orderby(mut planner: SQLPlanner, tbl_1: LogicalPlanRef) -> SQLPlannerResult<()> {
        let sql = "select utf8 from tbl1 order by utf8 desc";
        let plan = planner.plan_sql(sql)?;

        let expected = LogicalPlanBuilder::new(tbl_1, None)
            .select(vec![col("utf8")])?
            .sort(vec![col("utf8")], vec![true], vec![true])?
            .build();

        assert_eq!(plan, expected);
        Ok(())
    }

    #[rstest]
    fn test_cast(mut planner: SQLPlanner, tbl_1: LogicalPlanRef) -> SQLPlannerResult<()> {
        let builder = LogicalPlanBuilder::new(tbl_1, None);
        let cases = vec![
            (
                "select bool::text from tbl1",
                vec![col("bool").cast(&DataType::Utf8)],
            ),
            (
                "select utf8::bytes from tbl1",
                vec![col("utf8").cast(&DataType::Binary)],
            ),
            (
                r#"select CAST("bool" as text) from tbl1"#,
                vec![col("bool").cast(&DataType::Utf8)],
            ),
        ];
        for (sql, expected) in cases {
            let actual = planner.plan_sql(sql)?;
            let expected = builder.clone().select(expected)?.build();
            assert_eq!(
                actual,
                expected,
                "query: {}\n expected:{}",
                sql,
                expected.repr_ascii(false)
            );
        }

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
        let expected = LogicalPlanBuilder::new(tbl_2, None)
            .join_with_null_safe_equal(
                tbl_3,
                vec![col("id")],
                vec![col("id")],
                Some(vec![null_equals_null]),
                JoinType::Inner,
                None,
                None,
                Some("tbl3."),
                true,
            )?
            .select(vec![col("*")])?
            .build();
        assert_eq!(plan, expected);
        Ok(())
    }

    #[rstest]
    #[case::abs("select abs(i32) as abs from tbl1")]
    #[case::ceil("select ceil(i32) as ceil from tbl1")]
    #[case::floor("select floor(i32) as floor from tbl1")]
    #[case::sign("select sign(i32) as sign from tbl1")]
    #[case::round("select round(i32, 1) as round from tbl1")]
    #[case::sqrt("select sqrt(i32) as sqrt from tbl1")]
    #[case::sin("select sin(i32) as sin from tbl1")]
    #[case::cos("select cos(i32) as cos from tbl1")]
    #[case::tan("select tan(i32) as tan from tbl1")]
    #[case::asin("select asin(i32) as asin from tbl1")]
    #[case::acos("select acos(i32) as acos from tbl1")]
    #[case::atan("select atan(i32) as atan from tbl1")]
    #[case::atan2("select atan2(i32, 1) as atan2 from tbl1")]
    #[case::radians("select radians(i32) as radians from tbl1")]
    #[case::degrees("select degrees(i32) as degrees from tbl1")]
    #[case::log2("select log2(i32) as log2 from tbl1")]
    #[case::log10("select log10(i32) as log10 from tbl1")]
    #[case::ln("select ln(i32) as ln from tbl1")]
    #[case::exp("select exp(i32) as exp from tbl1")]
    #[case::atanh("select atanh(i32) as atanh from tbl1")]
    #[case::acosh("select acosh(i32) as acosh from tbl1")]
    #[case::asinh("select asinh(i32) as asinh from tbl1")]
    #[case::ends_with("select ends_with(utf8, 'a') as ends_with from tbl1")]
    #[case::starts_with("select starts_with(utf8, 'a') as starts_with from tbl1")]
    #[case::contains("select contains(utf8, 'a') as contains from tbl1")]
    #[case::split("select split(utf8, '.') as split from tbl1")]
    #[case::replace("select regexp_replace(utf8, 'a', 'b') as replace from tbl1")]
    #[case::length("select length(utf8) as length from tbl1")]
    #[case::lower("select lower(utf8) as lower from tbl1")]
    #[case::upper("select upper(utf8) as upper from tbl1")]
    #[case::lstrip("select lstrip(utf8) as lstrip from tbl1")]
    #[case::rstrip("select rstrip(utf8) as rstrip from tbl1")]
    #[case::reverse("select reverse(utf8) as reverse from tbl1")]
    #[case::capitalize("select capitalize(utf8) as capitalize from tbl1")]
    #[case::left("select left(utf8, 1) as left from tbl1")]
    #[case::right("select right(utf8, 1) as right from tbl1")]
    #[case::find("select find(utf8, 'a') as find from tbl1")]
    #[case::rpad("select rpad(utf8, 1, 'a') as rpad from tbl1")]
    #[case::lpad("select lpad(utf8, 1, 'a') as lpad from tbl1")]
    #[case::repeat("select repeat(utf8, 1) as repeat from tbl1")]
    #[case::to_date("select to_date(utf8, 'YYYY-MM-DD') as to_date from tbl1")]
    #[case::like("select utf8 like 'a' as like from tbl1")]
    #[case::ilike("select utf8 ilike 'a' as ilike from tbl1")]
    #[case::datestring("select DATE '2021-08-01' as dt from tbl1")]
    #[case::datetime("select DATETIME '2021-08-01 00:00:00' as dt from tbl1")]
    #[case::countstar("select COUNT(*) as count from tbl1")]
    #[case::countstarlower("select COUNT(*) as count from tbl1")]
    #[case::count("select COUNT(i32) as count from tbl1")]
    #[case::countcasing("select CoUnT(i32) as count from tbl1")]
    // #[case::to_datetime("select to_datetime(utf8, 'YYYY-MM-DD') as to_datetime from tbl1")]
    fn test_compiles_funcs(mut planner: SQLPlanner, #[case] query: &str) -> SQLPlannerResult<()> {
        let plan = planner.plan_sql(query);
        assert!(plan.is_ok(), "query: {query}\nerror: {plan:?}");

        Ok(())
    }

    #[rstest]
    fn test_global_agg(mut planner: SQLPlanner, tbl_1: LogicalPlanRef) -> SQLPlannerResult<()> {
        let sql = "select max(i32) from tbl1";
        let plan = planner.plan_sql(sql)?;

        let expected = LogicalPlanBuilder::new(tbl_1, None)
            .aggregate(vec![col("i32").max()], vec![])?
            .select(vec![col("i32")])?
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
    #[case::basic("select utf8 from tbl1 where i64 > (select max(id) from tbl2 where id = i32)")]
    #[case::compound(
        "select utf8 from tbl1 where i64 > (select max(id) from tbl2 where id = tbl1.i32)"
    )]
    fn test_correlated_subquery(
        mut planner: SQLPlanner,
        #[case] query: &str,
        tbl_1: LogicalPlanRef,
        tbl_2: LogicalPlanRef,
    ) -> SQLPlannerResult<()> {
        let plan = planner.plan_sql(query)?;

        let outer_col = Arc::new(Expr::OuterReferenceColumn(OuterReferenceColumn {
            field: Field::new("i32", DataType::Int32),
            depth: 1,
        }));
        let subquery = LogicalPlanBuilder::new(tbl_2, None)
            .filter(col("id").eq(outer_col))?
            .aggregate(vec![col("id").max()], vec![])?
            .select(vec![col("id")])?
            .build();

        let subquery = Arc::new(Expr::Subquery(Subquery { plan: subquery }));

        let expected = LogicalPlanBuilder::new(tbl_1, None)
            .filter(col("i64").gt(subquery))?
            .select(vec![col("utf8")])?
            .build();

        assert_eq!(plan, expected);

        Ok(())
    }
}
