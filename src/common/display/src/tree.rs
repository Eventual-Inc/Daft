use std::sync::Arc;

pub trait TreeDisplay {
    /// Describe the node in a human-readable way.
    /// The `level` parameter is used to determine how verbose the description should be.
    /// It is up to the implementer to decide how to use this parameter.
    ///
    /// For example, a `level` of `DisplayLevel::Compact` might only show the name of the node,
    /// while a `level` of `DisplayLevel::Default` might show all available details.
    ///
    /// **Important**. Implementers do not need to worry about the formatting of the output.
    fn description(&self, level: crate::DisplayLevel) -> String;

    /// Get a unique identifier for this node.
    /// No two nodes should have the same semantic id.
    /// The default implementation uses the node's name and memory address.
    fn semantic_id(&self) -> String {
        let mut s = String::new();
        s.push_str(&self.get_name());
        s.push_str(&format!("{:p}", self as *const Self as *const ()));
        s
    }

    /// Get the human-readable name of this node.
    fn get_name(&self) -> String {
        std::any::type_name::<Self>().to_string()
    }

    /// Required method: Get the children of the self node.
    fn get_children(&self) -> Vec<Arc<dyn TreeDisplay>>;
}

impl TreeDisplay for Arc<dyn TreeDisplay> {
    fn description(&self, level: crate::DisplayLevel) -> String {
        self.as_ref().description(level)
    }

    fn get_name(&self) -> String {
        self.as_ref().get_name()
    }

    fn semantic_id(&self) -> String {
        self.as_ref().semantic_id()
    }

    fn get_children(&self) -> Vec<Arc<dyn TreeDisplay>> {
        self.as_ref().get_children()
    }
}
