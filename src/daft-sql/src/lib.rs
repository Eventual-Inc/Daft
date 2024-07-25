pub mod catalog;
pub mod error;
pub mod functions;
mod planner;

#[cfg(feature = "python")]
pub mod python;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<python::PyCatalog>()?;
    parent.add_wrapped(wrap_pyfunction!(python::sql))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use catalog::SQLCatalog;
    use daft_core::{
        datatypes::{Field, TimeUnit},
        schema::Schema,
        DataType,
    };
    use daft_dsl::{col, lit};
    use daft_plan::{
        logical_plan::Source, source_info::PlaceHolderInfo, ClusteringSpec, LogicalPlan,
        LogicalPlanBuilder, LogicalPlanRef, SourceInfo,
    };
    use error::SQLPlannerResult;
    use planner::SQLPlanner;
    use rstest::rstest;

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

    fn setup() -> SQLPlanner {
        let mut catalog = SQLCatalog::new();

        catalog.register_table("tbl1", tbl_1());
        catalog.register_table("tbl2", tbl_2());
        catalog.register_table("tbl3", tbl_3());

        SQLPlanner::new(catalog)
    }

    #[rstest]
    #[case("select * from tbl1")]
    #[case("select * from tbl1 limit 1")]
    #[case("select * exclude utf8 from tbl1")]
    #[case("select * exclude (utf8, i32) from tbl1")]
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
    #[case("select list_utf8[0:2] from tbl1")]
    #[case("select * from tbl2 join tbl3 on tbl2.id = tbl3.id")]
    #[case("select tbl2.text from tbl2")]
    #[case("select tbl2.text from tbl2 join tbl3 using (id)")]
    #[case(
        r#"
    select
        abs(i32) as abs,
        ceil(i32) as ceil,
        floor(i32) as floor,
        sign(i32) as sign
    from tbl1"#
    )]
    #[case("select round(i32, 1) from tbl1")]
    #[case::groupby("select max(i32) from tbl1 group by utf8")]
    #[case::orderby("select * from tbl1 order by i32")]
    #[case::orderby("select * from tbl1 order by i32 desc")]
    #[case::orderby("select * from tbl1 order by i32 asc")]
    #[case::orderby_multi("select * from tbl1 order by i32 desc, f32 asc")]
    fn test_compiles(#[case] query: &str) -> SQLPlannerResult<()> {
        let planner = setup();

        let plan = planner.plan_sql(query);
        assert!(plan.is_ok(), "query: {}\nerror: {:?}", query, plan);

        Ok(())
    }

    #[test]
    fn test_parse_sql() {
        let planner = setup();
        let sql = "select test as a from tbl1";
        let plan = planner.plan_sql(sql).unwrap();

        let expected = LogicalPlanBuilder::new(tbl_1())
            .select(vec![col("test").alias("a")])
            .unwrap()
            .build();
        assert_eq!(plan, expected);
    }

    #[test]
    fn test_where_clause() -> SQLPlannerResult<()> {
        let planner = setup();
        let sql = "select test as a from tbl1 where test = 'a'";
        let plan = planner.plan_sql(sql)?;

        let expected = LogicalPlanBuilder::new(tbl_1())
            .filter(col("test").eq(lit("a")))?
            .select(vec![col("test").alias("a")])?
            .build();

        assert_eq!(plan, expected);
        Ok(())
    }
    #[test]
    fn test_limit() -> SQLPlannerResult<()> {
        let planner = setup();
        let sql = "select test as a from tbl1 limit 10";
        let plan = planner.plan_sql(sql)?;

        let expected = LogicalPlanBuilder::new(tbl_1())
            .select(vec![col("test").alias("a")])?
            .limit(10, true)?
            .build();

        assert_eq!(plan, expected);
        Ok(())
    }

    #[test]
    fn test_orderby() -> SQLPlannerResult<()> {
        let planner = setup();
        let sql = "select utf8 from tbl1 order by utf8 desc";
        let plan = planner.plan_sql(sql)?;

        let expected = LogicalPlanBuilder::new(tbl_1())
            .select(vec![col("utf8")])?
            .sort(vec![col("utf8")], vec![true])?
            .build();

        assert_eq!(plan, expected);
        Ok(())
    }

    #[test]
    fn test_cast() -> SQLPlannerResult<()> {
        let planner = setup();
        let builder = LogicalPlanBuilder::new(tbl_1());
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

    #[test]
    fn test_join() -> SQLPlannerResult<()> {
        let planner = setup();
        let sql = "select * from tbl2 join tbl3 on tbl2.id = tbl3.id";
        let plan = planner.plan_sql(sql)?;
        let expected = LogicalPlanBuilder::new(tbl_2())
            .join(
                tbl_3(),
                vec![col("id")],
                vec![col("id")],
                daft_core::JoinType::Inner,
                None,
            )?
            .build();
        assert_eq!(plan, expected);
        Ok(())
    }
}
