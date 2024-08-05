use std::marker::PhantomData;

use common_error::DaftResult;
use common_treenode::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use indexmap::IndexMap;

use crate::{LogicalPlan, PhysicalPlan};

use super::TreeDisplay;

pub trait MermaidDisplay {
    fn repr_mermaid(&self, options: MermaidDisplayOptions) -> String;
}

#[derive(Debug, Clone, Default)]
pub struct MermaidDisplayOptions {
    // simple mode doesn't show the full display string of each node.
    // This is useful for large trees.
    // In simple mode, the display string is just the node's name.
    pub simple: bool,
    // subgraph_options is used to configure the subgraph.
    pub subgraph_options: Option<SubgraphOptions>,
}

#[derive(Debug, Clone)]
pub struct SubgraphOptions {
    pub name: String,
    pub subgraph_id: String,
}

struct MermaidDisplayVisitor<T> {
    phantom: PhantomData<T>,
    // each node should only appear once in the tree.
    // the key is the node's `multiline_display` string, and the value is the node's id.
    // This is necessary because the same kind of node can appear multiple times in the tree. (such as multiple filters)
    nodes: IndexMap<String, String>,
    // node_count is used to generate unique ids for each node.
    node_count: usize,
    output: Vec<String>,
    options: MermaidDisplayOptions,
}

impl<T> MermaidDisplayVisitor<T> {
    pub fn new(options: MermaidDisplayOptions) -> MermaidDisplayVisitor<T> {
        let mut output = Vec::new();
        match &options.subgraph_options {
            Some(SubgraphOptions { name, subgraph_id }) => {
                output.push(format!(r#"subgraph {subgraph_id}["{name}"]"#));
            }
            None => {
                output.push("flowchart TD".to_string());
            }
        }

        MermaidDisplayVisitor {
            phantom: PhantomData,
            nodes: IndexMap::new(),
            node_count: 0,
            output,
            options,
        }
    }

    /// Build the mermaid chart.
    /// Example:
    /// ```mermaid
    /// flowchart TD
    /// Limit0["Limit: 10"]
    /// Filter1["Filter: col(first_name) == lit('hello')"]
    /// Source2["Source: ..."]
    /// Limit0 --> Filter1
    /// Filter1 --> Source2
    /// ```
    fn build(mut self) -> String {
        if self.options.subgraph_options.is_some() {
            self.output.push("end".to_string());
        }
        self.output.join("\n")
    }
}

impl<T> MermaidDisplayVisitor<T>
where
    T: TreeDisplay,
{
    fn add_node(&mut self, node: &T) {
        let name = node.get_name();
        let display = self.display_for_node(node);
        let node_id = self.node_count;
        self.node_count += 1;

        let id = match &self.options.subgraph_options {
            Some(SubgraphOptions { subgraph_id, .. }) => format!("{subgraph_id}{name}{node_id}"),
            None => format!("{name}{node_id}"),
        };

        self.nodes.insert(display.clone(), id.clone());

        if self.options.simple {
            self.output.push(format!(r#"{}["{}"]"#, id, name));
        } else {
            self.output.push(format!(r#"{}["{}"]"#, id, display));
        }
    }

    fn display_for_node(&self, node: &T) -> String {
        // Ideally, a node should be able to uniquely identify itself.
        // For now, we'll just use the disaplay string.
        let mut display = Vec::new();
        let lines = node.get_multiline_representation();
        let max_chars = 80;

        for line in lines {
            let sublines = textwrap::wrap(&line, max_chars)
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();

            display.extend(sublines);
        }

        display.join("\n").replace('\"', "'")
    }

    // Get the id of a node that has already been added.
    fn get_node_id(&self, node: &T) -> String {
        let display = self.display_for_node(node);
        // SAFETY: Since this is only called after all nodes have been added, we can safely unwrap.
        self.nodes.get(&display).cloned().unwrap()
    }

    fn add_edge(&mut self, parent: String, child: String) {
        self.output.push(format!(r#"{child} --> {parent}"#));
    }
}

impl<T> TreeNodeVisitor for MermaidDisplayVisitor<T>
where
    T: TreeDisplay + TreeNode,
{
    type Node = T;

    fn f_down(&mut self, node: &Self::Node) -> DaftResult<TreeNodeRecursion> {
        // go from top to bottom, and add all nodes first.
        // order doesn't matter here, because mermaid uses the edges to determine the order.
        self.add_node(node);
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &Self::Node) -> DaftResult<TreeNodeRecursion> {
        // now that all nodes are added, we can add edges between them.
        // We want to do this from bottom to top so that the ordering of the mermaid chart is correct.
        let node_id = self.get_node_id(node);
        let children = node.get_children();
        if children.is_empty() {
            return Ok(TreeNodeRecursion::Continue);
        }
        for child in children {
            let child_id = self.get_node_id(&child);
            self.add_edge(node_id.clone(), child_id);
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

impl MermaidDisplay for LogicalPlan {
    /// Convert the logical plan to a mermaid diagram.
    /// If `simple` is true, the diagram will be simplified. and details will be omitted.
    fn repr_mermaid(&self, options: MermaidDisplayOptions) -> String {
        let mut visitor = MermaidDisplayVisitor::new(options);
        // TODO: implement TreeNode for LogicalPlan so that we can use `visit` directly.
        let _ = self.visit(&mut visitor);

        visitor.build()
    }
}

impl MermaidDisplay for PhysicalPlan {
    fn repr_mermaid(&self, options: MermaidDisplayOptions) -> String {
        let mut visitor = MermaidDisplayVisitor::new(options);
        let _ = self.visit(&mut visitor);

        visitor.build()
    }
}

#[cfg(test)]
mod test {
    use super::{MermaidDisplay, MermaidDisplayOptions, SubgraphOptions};
    use std::sync::Arc;

    use common_error::DaftResult;
    use daft_core::{datatypes::Field, schema::Schema, DataType};
    use daft_dsl::{
        col,
        functions::utf8::{endswith, startswith},
        lit,
    };

    use crate::{
        logical_ops::Source, source_info::PlaceHolderInfo, ClusteringSpec, LogicalPlan,
        LogicalPlanBuilder, LogicalPlanRef, SourceInfo,
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
        let subplan = LogicalPlanBuilder::new(plan_1())
            .filter(col("id").eq(lit(1)))?
            .build();

        let subplan2 = LogicalPlanBuilder::new(plan_2())
            .filter(
                startswith(col("last_name"), lit("S")).and(endswith(col("last_name"), lit("n"))),
            )?
            .limit(1000, false)?
            .add_monotonically_increasing_id(None)?
            .distinct()?
            .sort(vec![col("last_name")], vec![false])?
            .build();

        let plan = LogicalPlanBuilder::new(subplan)
            .join(
                subplan2,
                vec![col("id")],
                vec![col("id")],
                daft_core::JoinType::Inner,
                None,
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
Output schema = text#Utf8, id#Int32, first_name#Utf8, last_name#Utf8"]
Filter4["Filter: col(id) == lit(1)"]
Source5["PlaceHolder:
Source ID = 0
Num partitions = 0
Output schema = text#Utf8, id#Int32"]
Source5 --> Filter4
Sort6["Sort: Sort by = (col(last_name), ascending)"]
Distinct7["Distinct"]
MonotonicallyIncreasingId8["MonotonicallyIncreasingId"]
Limit9["Limit: 1000"]
Filter10["Filter: startswith(col(last_name), lit('S')) & endswith(col(last_name), lit('n'))"]
Source11["PlaceHolder:
Source ID = 0
Num partitions = 0
Output schema = first_name#Utf8, last_name#Utf8, id#Int32"]
Source11 --> Filter10
Filter10 --> Limit9
Limit9 --> MonotonicallyIncreasingId8
MonotonicallyIncreasingId8 --> Distinct7
Distinct7 --> Sort6
Filter4 --> Join3
Sort6 --> Join3
Join3 --> Filter2
Filter2 --> Project1
Project1 --> Limit0"#;

        assert_eq!(mermaid_repr, expected);
        Ok(())
    }
    #[test]
    // create a random, complex plan and check if it can be displayed as expected
    fn test_mermaid_display_simple() -> DaftResult<()> {
        let subplan = LogicalPlanBuilder::new(plan_1())
            .filter(col("id").eq(lit(1)))?
            .build();

        let subplan2 = LogicalPlanBuilder::new(plan_2())
            .filter(
                startswith(col("last_name"), lit("S")).and(endswith(col("last_name"), lit("n"))),
            )?
            .limit(1000, false)?
            .add_monotonically_increasing_id(None)?
            .distinct()?
            .sort(vec![col("last_name")], vec![false])?
            .build();

        let plan = LogicalPlanBuilder::new(subplan)
            .join(
                subplan2,
                vec![col("id")],
                vec![col("id")],
                daft_core::JoinType::Inner,
                None,
            )?
            .filter(col("first_name").eq(lit("hello")))?
            .select(vec![col("first_name")])?
            .limit(10, false)?
            .build();

        let mermaid_repr = plan.repr_mermaid(Default::default());
        let expected = r#"flowchart TD
Limit0["Limit"]
Project1["Project"]
Filter2["Filter"]
Join3["Join"]
Filter4["Filter"]
Source5["Source"]
Source5 --> Filter4
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
Filter4 --> Join3
Sort6 --> Join3
Join3 --> Filter2
Filter2 --> Project1
Project1 --> Limit0"#;

        assert_eq!(mermaid_repr, expected);
        Ok(())
    }

    #[test]
    fn test_subgraph_opts() -> DaftResult<()> {
        let plan = plan_1();
        let opts = MermaidDisplayOptions {
            simple: false,
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
end"#;
        assert_eq!(mermaid_repr, expected);
        Ok(())
    }
}
