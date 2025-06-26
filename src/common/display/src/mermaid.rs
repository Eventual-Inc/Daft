use std::fmt;

use indexmap::IndexMap;

use crate::{tree::TreeDisplay, DisplayLevel};

pub trait MermaidDisplay: TreeDisplay {
    fn repr_mermaid(&self, options: MermaidDisplayOptions) -> String;
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "python", derive(pyo3::FromPyObject))]
pub struct MermaidDisplayOptions {
    /// simple mode doesn't show the full display string of each node.
    /// This is useful for large trees.
    /// In simple mode, the display string is just the node's name.
    pub simple: bool,
    /// Display the root node at the bottom of the diagram or at the top
    pub bottom_up: bool,
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
    /// metadata
    pub metadata: Option<String>,
}

impl<T: TreeDisplay> MermaidDisplay for T {
    fn repr_mermaid(&self, options: MermaidDisplayOptions) -> String {
        let mut s = String::new();
        let display_type = match options.simple {
            true => DisplayLevel::Compact,
            false => DisplayLevel::Default,
        };

        let mut visitor = MermaidDisplayVisitor::new(
            &mut s,
            display_type,
            options.bottom_up,
            options.subgraph_options,
        );

        let _ = visitor.fmt(self);
        s
    }
}

pub struct MermaidDisplayVisitor<'a, W> {
    output: &'a mut W,
    t: DisplayLevel,
    /// Should the root node be at the bottom or the top of the diagram
    bottom_up: bool,
    /// each node should only appear once in the tree.
    /// the key is the node's `multiline_display` string, and the value is the node's id.
    /// This is necessary because the same kind of node can appear multiple times in the tree. (such as multiple filters)
    nodes: IndexMap<String, String>,
    /// node_count is used to generate unique ids for each node.
    node_count: usize,
    subgraph_options: Option<SubgraphOptions>,
}

impl<'a, W> MermaidDisplayVisitor<'a, W> {
    pub fn new(
        w: &'a mut W,
        t: DisplayLevel,
        bottom_up: bool,
        subgraph_options: Option<SubgraphOptions>,
    ) -> Self {
        Self {
            output: w,
            t,
            bottom_up,
            nodes: IndexMap::new(),
            node_count: 0,
            subgraph_options,
        }
    }
}

impl<W> MermaidDisplayVisitor<'_, W>
where
    W: fmt::Write,
{
    fn add_node(&mut self, node: &dyn TreeDisplay) -> fmt::Result {
        let name = node.get_name();
        let display = self.display_for_node(node)?;
        let node_id = self.node_count;
        self.node_count += 1;

        let id = match &self.subgraph_options {
            Some(SubgraphOptions { subgraph_id, .. }) => format!("{subgraph_id}{name}{node_id}"),
            None => format!("{name}{node_id}"),
        };
        if display.is_empty() {
            return Err(fmt::Error);
        }
        writeln!(self.output, r#"{id}["{display}"]"#)?;

        self.nodes.insert(node.id(), id);
        Ok(())
    }

    fn display_for_node(&self, node: &dyn TreeDisplay) -> Result<String, fmt::Error> {
        // Ideally, a node should be able to uniquely identify itself.
        // For now, we'll just use the display string.
        let line = node.display_as(self.t);
        let max_chars = 80;

        let sublines = textwrap::wrap(&line, max_chars);

        Ok(sublines.join("\n").replace('\"', "'"))
    }

    // Get the id of a node that has already been added.
    fn get_node_id(&self, node: &dyn TreeDisplay) -> Result<String, fmt::Error> {
        let id = node.id();
        // SAFETY: Since this is only called after the parent node have been added, we can safely unwrap.
        Ok(self.nodes.get(&id).cloned().unwrap())
    }

    fn add_edge(&mut self, parent: String, child: String) -> fmt::Result {
        writeln!(self.output, r"{child} --> {parent}")
    }

    fn fmt_node(&mut self, node: &dyn TreeDisplay) -> fmt::Result {
        self.add_node(node)?;
        let children = node.get_children();
        if children.is_empty() {
            return Ok(());
        }

        for child in children {
            self.fmt_node(child)?;
            self.add_edge(self.get_node_id(node)?, self.get_node_id(child)?)?;
        }

        Ok(())
    }

    pub fn fmt(&mut self, node: &dyn TreeDisplay) -> fmt::Result {
        if let Some(SubgraphOptions {
            name,
            subgraph_id,
            metadata,
        }) = &self.subgraph_options
        {
            writeln!(self.output, r#"subgraph {subgraph_id}["{name}"]"#)?;
            if self.bottom_up {
                writeln!(self.output, r"direction BT")?;
            } else {
                writeln!(self.output, r"direction TB")?;
            }
            // add metadata to the subgraph
            let metadata_id = if let Some(metadata) = metadata {
                let id = format!("{subgraph_id}_metadata");
                writeln!(self.output, r#"{id}["{metadata}"]"#)?;
                Some(id)
            } else {
                None
            };

            self.fmt_node(node)?;

            // stack metadata on top of first node with an invisible edge
            if let Some(metadata_id) = metadata_id {
                if self.bottom_up {
                    let first_node_id = self.nodes.values().next().unwrap();
                    writeln!(self.output, r"{first_node_id} ~~~ {metadata_id}")?;
                } else {
                    let last_node_id = self.nodes.values().last().unwrap();
                    writeln!(self.output, r"{metadata_id} ~~~ {last_node_id}")?;
                }
            }

            writeln!(self.output, "end")?;
        } else {
            if self.bottom_up {
                writeln!(self.output, "flowchart BT")?;
            } else {
                writeln!(self.output, "flowchart TD")?;
            }

            self.fmt_node(node)?;
        }
        Ok(())
    }
}
