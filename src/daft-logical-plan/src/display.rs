use std::fmt::{self, Display};

use common_display::{tree::TreeDisplay, DisplayLevel};

impl TreeDisplay for crate::LogicalPlan {
    fn display_as(&self, level: DisplayLevel) -> String {
        match level {
            DisplayLevel::Compact => self.name().to_string(),
            DisplayLevel::Default | DisplayLevel::Verbose => self.multiline_display().join("\n"),
        }
    }

    fn get_name(&self) -> String {
        self.name().to_string()
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.children().into_iter().map(|x| x as _).collect()
    }
}

// Single node display.
impl Display for crate::LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.multiline_display().join(", "))?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common_display::mermaid::{MermaidDisplay, MermaidDisplayOptions, SubgraphOptions};
    use common_error::DaftResult;
    use daft_core::prelude::*;
    use daft_dsl::{col, lit};
    use daft_functions::utf8::{endswith, startswith};
    use pretty_assertions::assert_eq;

    use crate::{
        ops::Source, source_info::PlaceHolderInfo, ClusteringSpec, LogicalPlan, LogicalPlanBuilder,
        LogicalPlanRef, SourceInfo,
    };

    fn plan_1() -> LogicalPlanRef {
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

    fn plan_2() -> LogicalPlanRef {
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

    #[test]
    // create a random, complex plan and check if it can be displayed as expected
    fn test_mermaid_display() -> DaftResult<()> {
        let subplan = LogicalPlanBuilder::new(plan_1(), None)
            .filter(col("id").eq(lit(1)))?
            .build();

        let subplan2 = LogicalPlanBuilder::new(plan_2(), None)
            .filter(
                startswith(col("last_name"), lit("S")).and(endswith(col("last_name"), lit("n"))),
            )?
            .limit(1000, false)?
            .add_monotonically_increasing_id(None)?
            .distinct()?
            .sort(vec![col("last_name")], vec![false], vec![false])?
            .build();

        let plan = LogicalPlanBuilder::new(subplan, None)
            .join(
                subplan2,
                vec![col("id")],
                vec![col("id")],
                JoinType::Inner,
                None,
                None,
                None,
                false,
            )?
            .filter(col("first_name").eq(lit("hello")))?
            .select(vec![col("first_name")])?
            .limit(10, false)?
            .build();

        let mermaid_repr = plan.repr_mermaid(Default::default());

        let expected = r#"flowchart TD
Limit0["Limit: 10"]
Project1["Project: col(first_name)"]
Filter2["Filter: col(first_name) == lit('hello')"]
Join3["Join: Type = Inner
Strategy = Auto
On = col(id)
Output schema = id#Int32, text#Utf8, first_name#Utf8, last_name#Utf8"]
Filter4["Filter: col(id) == lit(1)"]
Source5["PlaceHolder:
Source ID = 0
Num partitions = 0
Output schema = text#Utf8, id#Int32"]
Source5 --> Filter4
Filter4 --> Join3
Sort6["Sort: Sort by = (col(last_name), ascending, nulls last)"]
Distinct7["Distinct"]
MonotonicallyIncreasingId8["MonotonicallyIncreasingId"]
Limit9["Limit: 1000"]
Filter10["Filter: startswith(col(last_name), lit('S')) & endswith(col(last_name),
lit('n'))"]
Source11["PlaceHolder:
Source ID = 0
Num partitions = 0
Output schema = first_name#Utf8, last_name#Utf8, id#Int32"]
Source11 --> Filter10
Filter10 --> Limit9
Limit9 --> MonotonicallyIncreasingId8
MonotonicallyIncreasingId8 --> Distinct7
Distinct7 --> Sort6
Sort6 --> Join3
Join3 --> Filter2
Filter2 --> Project1
Project1 --> Limit0
"#;

        assert_eq!(mermaid_repr, expected);
        Ok(())
    }
    #[test]
    // create a random, complex plan and check if it can be displayed as expected
    fn test_mermaid_display_simple() -> DaftResult<()> {
        let subplan = LogicalPlanBuilder::new(plan_1(), None)
            .filter(col("id").eq(lit(1)))?
            .build();

        let subplan2 = LogicalPlanBuilder::new(plan_2(), None)
            .filter(
                startswith(col("last_name"), lit("S")).and(endswith(col("last_name"), lit("n"))),
            )?
            .limit(1000, false)?
            .add_monotonically_increasing_id(None)?
            .distinct()?
            .sort(vec![col("last_name")], vec![false], vec![false])?
            .build();

        let plan = LogicalPlanBuilder::new(subplan, None)
            .join_with_null_safe_equal(
                subplan2,
                vec![col("id")],
                vec![col("id")],
                Some(vec![true]),
                JoinType::Inner,
                None,
                None,
                None,
                false,
            )?
            .filter(col("first_name").eq(lit("hello")))?
            .select(vec![col("first_name")])?
            .limit(10, false)?
            .build();

        let mermaid_repr = plan.repr_mermaid(MermaidDisplayOptions {
            simple: true,
            ..Default::default()
        });
        let expected = r#"flowchart TD
Limit0["Limit"]
Project1["Project"]
Filter2["Filter"]
Join3["Join"]
Filter4["Filter"]
Source5["Source"]
Source5 --> Filter4
Filter4 --> Join3
Sort6["Sort"]
Distinct7["Distinct"]
MonotonicallyIncreasingId8["MonotonicallyIncreasingId"]
Limit9["Limit"]
Filter10["Filter"]
Source11["Source"]
Source11 --> Filter10
Filter10 --> Limit9
Limit9 --> MonotonicallyIncreasingId8
MonotonicallyIncreasingId8 --> Distinct7
Distinct7 --> Sort6
Sort6 --> Join3
Join3 --> Filter2
Filter2 --> Project1
Project1 --> Limit0
"#;

        assert_eq!(mermaid_repr, expected);
        Ok(())
    }

    #[test]
    fn test_subgraph_opts() -> DaftResult<()> {
        let plan = plan_1();
        let opts = MermaidDisplayOptions {
            simple: false,
            bottom_up: false,
            subgraph_options: Some(SubgraphOptions {
                name: "Optimized Logical Plan".to_string(),
                subgraph_id: "optimized".to_string(),
            }),
        };

        let mermaid_repr = plan.repr_mermaid(opts);
        let expected = r#"subgraph optimized["Optimized Logical Plan"]
optimizedSource0["PlaceHolder:
Source ID = 0
Num partitions = 0
Output schema = text#Utf8, id#Int32"]
end
"#;
        assert_eq!(mermaid_repr, expected);
        Ok(())
    }
}
