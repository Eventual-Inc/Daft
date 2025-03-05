use std::fmt;

use common_display::mermaid::{MermaidDisplay, MermaidDisplayOptions, SubgraphOptions};

use super::planner::EmittedStage;

pub struct StageDisplayMermaidVisitor<'a, W> {
    output: &'a mut W,
    options: MermaidDisplayOptions,
}

impl<'a, W> StageDisplayMermaidVisitor<'a, W> {
    pub fn new(w: &'a mut W, options: MermaidDisplayOptions) -> Self {
        Self { output: w, options }
    }
}

impl<W> StageDisplayMermaidVisitor<'_, W>
where
    W: fmt::Write,
{
    fn add_node(&mut self, node: &EmittedStage) -> fmt::Result {
        let display = self.display_for_node(node)?;
        write!(self.output, "{}", display)?;
        Ok(())
    }

    fn display_for_node(&self, node: &EmittedStage) -> Result<String, fmt::Error> {
        let simple = self.options.simple;
        let bottom_up = self.options.bottom_up;
        let stage_id = self.get_node_id(node);
        let name = self.get_node_name(node);
        let subgraph_options = SubgraphOptions {
            name,
            subgraph_id: stage_id,
            metadata: Some(node.stats.as_ref().unwrap().to_string()),
        };
        let display = node.physical_plan.repr_mermaid(MermaidDisplayOptions {
            simple,
            bottom_up,
            subgraph_options: Some(subgraph_options),
        });
        Ok(display)
    }

    // Get the id of a node that has already been added.
    fn get_node_id(&self, node: &EmittedStage) -> String {
        let id = match node.stage_id {
            Some(id) => id.to_string(),
            None => "final".to_string(),
        };
        format!("stage_{}", id)
    }

    fn get_node_name(&self, node: &EmittedStage) -> String {
        let id = match node.stage_id {
            Some(id) => id.to_string(),
            None => "final".to_string(),
        };
        format!("Stage {}", id)
    }

    fn add_edge(&mut self, parent: String, child: String) -> fmt::Result {
        writeln!(self.output, r"{child} --> {parent}")
    }

    fn fmt_node(&mut self, node: &EmittedStage) -> fmt::Result {
        self.add_node(node)?;
        let children = &node.input_stages;
        if children.is_empty() {
            return Ok(());
        }

        for child in children {
            self.fmt_node(child)?;
            self.add_edge(self.get_node_id(node), self.get_node_id(child))?;
        }

        Ok(())
    }

    pub fn fmt(&mut self, node: &EmittedStage) -> fmt::Result {
        if let Some(SubgraphOptions {
            name, subgraph_id, ..
        }) = &self.options.subgraph_options
        {
            writeln!(self.output, r#"subgraph {subgraph_id}["{name}"]"#)?;
            if self.options.bottom_up {
                writeln!(self.output, r"direction BT")?;
            } else {
                writeln!(self.output, r"direction TB")?;
            }
            self.fmt_node(node)?;
            writeln!(self.output, "end")?;
        } else {
            if self.options.bottom_up {
                writeln!(self.output, "flowchart BT")?;
            } else {
                writeln!(self.output, "flowchart TD")?;
            }

            self.fmt_node(node)?;
        }
        Ok(())
    }
}
