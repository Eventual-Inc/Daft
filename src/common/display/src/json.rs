use serde_json::json;

use crate::{tree::TreeDisplay, DisplayLevel};

pub trait JsonDisplay: TreeDisplay {
    fn repr_json(&self, options: JsonDisplayOptions) -> String;
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "python", derive(pyo3::FromPyObject))]
pub struct JsonDisplayOptions {
    /// simple mode doesn't show the full display string of each node.
    /// This is useful for large trees.
    /// In simple mode, the display string is just the node's name.
    pub simple: bool,
}

impl<T: TreeDisplay> JsonDisplay for T {
    fn repr_json(&self, options: JsonDisplayOptions) -> String {
        let mut s = String::new();
        let display_type = match options.simple {
            true => DisplayLevel::Compact,
            false => DisplayLevel::Default,
        };

        let mut visitor = JsonDisplayVisitor::new(&mut s, display_type);

        let _ = visitor.fmt(self);
        json!(visitor.out).to_string()
    }
}
pub struct JsonDisplayVisitor<'a, W> {
    output: &'a mut W,
    t: DisplayLevel,
    /// Should the root node be at the bottom or the top of the diagram
    /// each node should only appear once in the tree.
    /// the key is the node's `multiline_display` string, and the value is the node's id.
    /// This is necessary because the same kind of node can appear multiple times in the tree. (such as multiple filters)
    out: Vec<serde_json::Value>,
}

impl<'a, W> JsonDisplayVisitor<'a, W> {
    pub fn new(output: &'a mut W, t: DisplayLevel) -> Self {
        Self {
            output,
            t,
            out: Vec::new(),
        }
    }
}

impl<W> JsonDisplayVisitor<'_, W>
where
    W: std::fmt::Write,
{
    pub fn fmt(&mut self, node: &dyn TreeDisplay) -> std::fmt::Result {
        let children = node.get_children();
        let children = children
            .iter()
            .map(|c| {
                let mut visitor = JsonDisplayVisitor::new(self.output, self.t);
                visitor.fmt(*c).unwrap();
                visitor.out
            })
            .collect::<Vec<_>>();
        let obj = json!({
            "name": node.get_name(),
            "info": node.display_as(self.t),
            "children": children
        });
        self.out.push(obj);

        Ok(())
    }
}
