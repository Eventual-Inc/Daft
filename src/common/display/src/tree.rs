use std::sync::Arc;

pub trait TreeDisplay {
    /// Describe the node in a human-readable way.
    /// The `level` parameter is used to determine how verbose the description should be.
    /// It is up to the implementer to decide how to use this parameter.
    ///
    ///
    /// If not implemented, it will default to the `get_name` method in `Compact` mode.
    /// and `get_multiline_representation` in `Default` mode.
    fn node_description(&self, level: crate::DisplayLevel) -> String {
        match level {
            // If in `Compact` mode, only display the name of the node.
            crate::DisplayLevel::Compact => self.get_name(),
            crate::DisplayLevel::Default => self.get_multiline_representation().join(", "),
        }
    }

    /// Required method: Get a list of lines representing this node. No trailing newlines.
    /// The multiline representation should contain information about the node itself,
    /// **but not its children.**
    fn get_multiline_representation(&self) -> Vec<String>;

    // Required method: Get the human-readable name of this node.
    fn get_name(&self) -> String;

    // Required method: Get the children of the self node.
    fn get_children(&self) -> Vec<Arc<dyn TreeDisplay>>;
}

impl TreeDisplay for Arc<dyn TreeDisplay> {
    fn get_multiline_representation(&self) -> Vec<String> {
        self.as_ref().get_multiline_representation()
    }

    fn get_name(&self) -> String {
        self.as_ref().get_name()
    }

    fn get_children(&self) -> Vec<Arc<dyn TreeDisplay>> {
        self.as_ref().get_children()
    }
}
