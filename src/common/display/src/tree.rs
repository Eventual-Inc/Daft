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
    fn display_as(&self, level: crate::DisplayLevel) -> String;

    /// Describe the node in a JSON format.
    /// This is useful for special visualization tools to parse the tree.
    /// Generally expected to include an `id`, `type`, `name`, and `schema` field, but may vary.
    /// Children will be auto-embedded.
    fn repr_json(&self) -> serde_json::Value;

    /// Get a unique identifier for this node.
    /// No two nodes should have the same id.
    /// The default implementation uses the node's name and memory address.
    fn id(&self) -> String {
        format!(
            "{}{:p}",
            self.get_name(),
            std::ptr::from_ref::<Self>(self).cast::<()>()
        )
    }

    /// Get the human-readable name of this node.
    fn get_name(&self) -> String {
        std::any::type_name::<Self>().to_string()
    }

    /// Required method: Get the children of the self node.
    fn get_children(&self) -> Vec<&dyn TreeDisplay>;
}

impl TreeDisplay for Arc<dyn TreeDisplay> {
    fn display_as(&self, level: crate::DisplayLevel) -> String {
        self.as_ref().display_as(level)
    }

    fn repr_json(&self) -> serde_json::Value {
        self.as_ref().repr_json()
    }

    fn get_name(&self) -> String {
        self.as_ref().get_name()
    }

    fn id(&self) -> String {
        self.as_ref().id()
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.as_ref().get_children()
    }
}
