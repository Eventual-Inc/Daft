use std::{
    collections::HashMap,
    fmt::{self, Display, Write},
};

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::{DaftError, DaftResult};
use common_treenode::TreeNodeVisitor;
use daft_dsl::{resolved_col, Literal};
use serde_json::json;

use crate::{LogicalPlan, LogicalPlanRef};

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

pub struct JsonVisitor<'a, W>
where
    W: std::fmt::Write,
{
    f: &'a mut W,
    objects: HashMap<u32, serde_json::Value>,
    /// if true, include the summarized schema information
    with_schema: bool,
    next_id: u32,
    parent_ids: Vec<u32>,
    output_nodes: Vec<serde_json::Value>,
}

impl<'a, W> JsonVisitor<'a, W>
where
    W: std::fmt::Write,
{
    pub fn new(f: &'a mut W) -> Self {
        Self {
            f,
            objects: HashMap::new(),
            with_schema: false,
            next_id: 0,
            parent_ids: Vec::new(),
            output_nodes: Vec::new(),
        }
    }
    pub fn with_schema(&mut self, with_schema: bool) {
        self.with_schema = with_schema;
    }

    fn to_json_value(node: &LogicalPlan) -> serde_json::Value {
        match node {
            LogicalPlan::Source(source) => json!({}),
            LogicalPlan::Project(project) => json!({
                "projection": project.projection.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
            }),
            LogicalPlan::ActorPoolProject(actor_pool_project) => todo!(),
            LogicalPlan::Filter(filter) => json!({
                "predicate": vec![&filter.predicate.to_string()],
            }),
            LogicalPlan::Limit(limit) => json!({
                "limit": vec![&limit.limit.lit().to_string()],
            }),
            LogicalPlan::Explode(explode) => json!({
                "to_explode": explode.to_explode.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
            }),
            LogicalPlan::Unpivot(unpivot) => json!({}),
            LogicalPlan::Sort(sort) => json!({
                "sort_by": sort.sort_by.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
                "nulls_first": sort.nulls_first,
                "descending": sort.descending,

            }),
            LogicalPlan::Repartition(repartition) => json!({}),
            LogicalPlan::Distinct(distinct) => json!({}),
            LogicalPlan::Aggregate(aggregate) => json!({
                "aggregations": aggregate.aggregations.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
                "groupby": aggregate.groupby.iter().map(|e| e.to_string()).collect::<Vec<_>>(),

            }),
            LogicalPlan::Pivot(pivot) => json!({
                "pivot_column": pivot.pivot_column.to_string(),
                "value_column": pivot.value_column.to_string(),
                "aggregation": pivot.aggregation,
                "group_by": pivot.group_by.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
                "names": pivot.names.iter().map(|e| resolved_col(e.clone()).to_string()).collect::<Vec<_>>(),
            }),
            LogicalPlan::Concat(concat) => json!({}),
            LogicalPlan::Intersect(intersect) => json!({}),
            LogicalPlan::Union(union) => json!({}),
            LogicalPlan::Join(join) => json!({
                "on": vec![&join.on.inner().map(|e| e.to_string())],
                "join_type": join.join_type,
                "join_strategy": join.join_strategy,

            }),
            LogicalPlan::Sink(sink) => json!({}),
            LogicalPlan::Sample(sample) => json!({}),
            LogicalPlan::MonotonicallyIncreasingId(monotonically_increasing_id) => json!({
                "column_name": vec![resolved_col(monotonically_increasing_id.column_name.clone()).to_string()]
            }),
            LogicalPlan::SubqueryAlias(subquery_alias) => json!({}),
            LogicalPlan::Window(window) => json!({}),
            LogicalPlan::TopN(top_n) => json!({}),
        }
    }

    pub fn finish(&mut self) {
        let value = self.objects.values().collect::<Vec<_>>();

        self.f
            .write_str(&serde_json::to_string_pretty(&value).unwrap())
            .unwrap();
    }
}

impl<W> TreeNodeVisitor for JsonVisitor<'_, W>
where
    W: Write,
{
    type Node = LogicalPlanRef;

    fn f_down(&mut self, node: &Self::Node) -> DaftResult<common_treenode::TreeNodeRecursion> {
        println!("f_down {}", node.name());
        let id = self.next_id;
        self.next_id += 1;
        let mut object = Self::to_json_value(node.as_ref());
        // handle all common properties here
        object["type"] = json!(node.name());
        object["stats"] = json!(node.stats_state());
        object["plan_id"] = json!(node.plan_id());
        object["children"] = serde_json::Value::Array(vec![]);
        
        if self.with_schema {
            let schema = node.schema();

            object["schema_string"] = json!(schema.short_string());
            // object["schema"] = json!(schema.fields());
        }
        self.objects.insert(id, object);
        self.parent_ids.push(id);
        Ok(common_treenode::TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, _node: &Self::Node) -> DaftResult<common_treenode::TreeNodeRecursion> {
        let id = self.parent_ids.pop().unwrap();

        let current_node = self
            .objects
            .remove(&id)
            .ok_or_else(|| DaftError::ValueError("Missing current node!".to_string()))?;

        if let Some(parent_id) = self.parent_ids.last() {
            let parent_node = self
                .objects
                .get_mut(parent_id)
                .expect("Missing parent node!");

            let plans = parent_node
                .get_mut("children")
                .and_then(|p| p.as_array_mut())
                .expect("Children should be an array");

            plans.push(current_node);
        } else {
            // This is the root node
            let plan = serde_json::json!(&current_node);
            write!(
                self.f,
                "{}",
                serde_json::to_string_pretty(&plan).map_err(|e| DaftError::SerdeJsonError(e))?
            )?;
        }
        Ok(common_treenode::TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common_display::mermaid::{MermaidDisplay, MermaidDisplayOptions, SubgraphOptions};
    use common_error::DaftResult;
    use common_treenode::TreeNode;
    use daft_core::prelude::*;
    use daft_dsl::{lit, resolved_col};
    use daft_functions::utf8::{endswith, startswith};
    use pretty_assertions::assert_eq;

    use crate::{
        ops::Source, source_info::PlaceHolderInfo, ClusteringSpec, LogicalPlan, LogicalPlanBuilder,
        LogicalPlanRef, SourceInfo,
    };

    fn plan_1() -> LogicalPlanRef {
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8),
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

    fn plan_2() -> LogicalPlanRef {
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

    #[test]
    // create a random, complex plan and check if it can be displayed as expected
    fn test_mermaid_display() -> DaftResult<()> {
        let subplan = LogicalPlanBuilder::from(plan_1())
            .filter(resolved_col("id").eq(lit(1)))?
            .build();

        let subplan2 = LogicalPlanBuilder::from(plan_2())
            .filter(
                startswith(resolved_col("last_name"), lit("S"))
                    .and(endswith(resolved_col("last_name"), lit("n"))),
            )?
            .limit(1000, false)?
            .add_monotonically_increasing_id(Some("id2"))?
            .distinct()?
            .sort(vec![resolved_col("last_name")], vec![false], vec![false])?
            .build();

        let plan = LogicalPlanBuilder::from(subplan)
            .join(
                subplan2,
                None,
                vec!["id".to_string()],
                JoinType::Inner,
                None,
                Default::default(),
            )?
            .filter(resolved_col("first_name").eq(lit("hello")))?
            .select(vec![resolved_col("first_name")])?
            .limit(10, false)?
            .build();

        let mermaid_repr = plan.repr_mermaid(Default::default());

        let expected = r#"flowchart TD
Limit0["Limit: 10"]
Project1["Project: col(first_name)"]
Filter2["Filter: col(first_name) == lit('hello')"]
Join3["Join: Type = Inner
Strategy = Auto
On = col(left.id#Int32) == col(right.id#Int32)
Output schema = id#Int32, text#Utf8, id2#UInt64, first_name#Utf8, last_name#Utf8"]
Filter4["Filter: col(id) == lit(1)"]
Source5["PlaceHolder:
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
        let subplan = LogicalPlanBuilder::from(plan_1())
            .filter(resolved_col("id").eq(lit(1)))?
            .build();

        let subplan2 = LogicalPlanBuilder::from(plan_2())
            .filter(
                startswith(resolved_col("last_name"), lit("S"))
                    .and(endswith(resolved_col("last_name"), lit("n"))),
            )?
            .limit(1000, false)?
            .add_monotonically_increasing_id(Some("id2"))?
            .distinct()?
            .sort(vec![resolved_col("last_name")], vec![false], vec![false])?
            .build();

        let plan = LogicalPlanBuilder::from(subplan)
            .join(
                subplan2,
                None,
                vec!["id".to_string()],
                JoinType::Inner,
                None,
                Default::default(),
            )?
            .filter(resolved_col("first_name").eq(lit("hello")))?
            .select(vec![resolved_col("first_name")])?
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
                metadata: None,
            }),
        };

        let mermaid_repr = plan.repr_mermaid(opts);
        let expected = r#"subgraph optimized["Optimized Logical Plan"]
direction TB
optimizedSource0["PlaceHolder:
Num partitions = 0
Output schema = text#Utf8, id#Int32"]
end
"#;
        assert_eq!(mermaid_repr, expected);
        Ok(())
    }
    #[test]
    fn test_repr_json() -> DaftResult<()> {
        let subplan = LogicalPlanBuilder::from(plan_1())
            .filter(resolved_col("id").eq(lit(1)))?
            .build();

        let subplan2 = LogicalPlanBuilder::from(plan_2())
            .filter(
                startswith(resolved_col("last_name"), lit("S"))
                    .and(endswith(resolved_col("last_name"), lit("n"))),
            )?
            .limit(1000, false)?
            .add_monotonically_increasing_id(Some("id2"))?
            .distinct()?
            .sort(vec![resolved_col("last_name")], vec![false], vec![false])?
            .build();

        let plan = LogicalPlanBuilder::from(subplan)
            .join(
                subplan2,
                None,
                vec!["id".to_string()],
                JoinType::Inner,
                None,
                Default::default(),
            )?
            .filter(resolved_col("first_name").eq(lit("hello")))?
            .select(vec![resolved_col("first_name")])?
            .limit(10, false)?
            .build();

        let mut output = String::new();

        let mut json_vis = super::JsonVisitor::new(&mut output);
        json_vis.with_schema(true);

        plan.visit(&mut json_vis).unwrap();
        println!("{}", output);
        // json_vis.finish();
        assert!(false);
        Ok(())
    }
}
