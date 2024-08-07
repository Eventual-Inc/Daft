use std::{marker::PhantomData, sync::Arc};

use common_error::DaftResult;
use common_treenode::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use indexmap::IndexMap;

use crate::tree::TreeDisplay;

pub trait MermaidDisplay {
    fn repr_mermaid(&self, options: MermaidDisplayOptions) -> String;
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "python", derive(pyo3::FromPyObject))]
pub struct MermaidDisplayOptions {
    /// simple mode doesn't show the full display string of each node.
    /// This is useful for large trees.
    /// In simple mode, the display string is just the node's name.
    pub simple: bool,
    /// subgraph_options is used to configure the subgraph.
    /// Since some common displays (jupyter) don't support multiple mermaid graphs in a single cell, we need to use subgraphs.
    /// The subgraph_options is used to both indicate that a subgraph should be used, and to configure the subgraph.
    pub subgraph_options: Option<SubgraphOptions>,
}

/// subrgaph <subgraph_id>["<name>"]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", derive(pyo3::FromPyObject))]
pub struct SubgraphOptions {
    /// The display text for the subgraph name.
    pub name: String,
    /// The unique id for the subgraph.
    pub subgraph_id: String,
}

struct MermaidDisplayVisitor<T> {
    phantom: PhantomData<T>,
    /// each node should only appear once in the tree.
    /// the key is the node's `multiline_display` string, and the value is the node's id.
    /// This is necessary because the same kind of node can appear multiple times in the tree. (such as multiple filters)
    nodes: IndexMap<String, String>,
    /// node_count is used to generate unique ids for each node.
    node_count: usize,
    output: Vec<String>,
    options: MermaidDisplayOptions,
}

impl<T> MermaidDisplayVisitor<T> {
    pub fn new(options: MermaidDisplayOptions) -> MermaidDisplayVisitor<T> {
        let mut output = Vec::new();
        // if it's not a subgraph, we render the entire thing: `flowchart TD`
        // otherwise we just build out the subgraph componenend `subgraph <subgraph_id>["<name>"]`
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

        let lines = node.get_multiline_representation();
        let mut display: Vec<String> = Vec::with_capacity(lines.len());
        let max_chars = 80;

        for line in lines {
            let sublines = textwrap::wrap(&line, max_chars);
            let sublines = sublines.iter().map(|s| s.to_string());

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

impl<T> MermaidDisplay for Arc<T>
where
    T: TreeDisplay,
    Arc<T>: TreeNode,
{
    fn repr_mermaid(&self, options: MermaidDisplayOptions) -> String {
        let mut visitor = MermaidDisplayVisitor::new(options);
        let _ = self.visit(&mut visitor);
        visitor.build()
    }
}
