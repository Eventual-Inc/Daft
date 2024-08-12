use std::{fmt, sync::Arc};

pub trait TreeDisplay {
    /// Display the current node in the tree.
    /// If not implemented, it will default to the `get_name` method in `Compact` mode.
    /// and `get_multiline_representation` in `Default` mode.
    fn node_display(&self, t: crate::DisplayLevel) -> String {
        match t {
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

pub struct AsciiTreeBuilder {
    pub simple: bool,
}

impl AsciiTreeBuilder {
    pub fn new(simple: bool) -> Self {
        Self { simple }
    }

    // Print the whole tree represented by this node.
    fn fmt_tree<'a, W: fmt::Write + 'a, T: TreeDisplay>(
        &self,
        node: &T,
        s: &'a mut W,
        simple: bool,
    ) -> fmt::Result {
        let t = if simple {
            crate::DisplayLevel::Compact
        } else {
            crate::DisplayLevel::Default
        };
        self.fmt_tree_gitstyle(node, 0, s, t)
    }

    // Print the tree recursively, and illustrate the tree structure with a single line per node + indentation.
    fn fmt_tree_indent_style<'a, W: fmt::Write + 'a, T: TreeDisplay>(
        &self,
        node: &T,
        indent: usize,
        s: &'a mut W,
    ) -> fmt::Result {
        // Print the current node.
        if indent > 0 {
            writeln!(s)?;
            write!(s, "{:indent$}", "", indent = 2 * indent)?;
        }
        let node_str = node.get_multiline_representation().join(", ");
        write!(s, "{node_str}")?;

        // Recursively handle children.
        let children = node.get_children();
        match &children[..] {
            // No children - stop printing.
            [] => Ok(()),
            // One child.
            [child] => {
                // Child tree.
                self.fmt_tree_indent_style(child, indent + 1, s)
            }
            // Two children.
            [left, right] => {
                self.fmt_tree_indent_style(left, indent + 1, s)?;
                self.fmt_tree_indent_style(right, indent + 1, s)
            }
            _ => unreachable!("Max two child nodes expected, got {}", children.len()),
        }
    }

    // Print the tree recursively, and illustrate the tree structure in the same style as `git log --graph`.
    // `depth` is the number of forks in this node's ancestors.
    fn fmt_tree_gitstyle<'a, W: fmt::Write + 'a, T: TreeDisplay>(
        &self,
        node: &T,
        depth: usize,
        s: &'a mut W,
        t: crate::DisplayLevel,
    ) -> fmt::Result {
        // Print the current node.
        // e.g. | | * <node contents line 1>
        //      | | | <node contents line 2>

        let lines = match t {
            crate::DisplayLevel::Compact => vec![node.get_name()],
            crate::DisplayLevel::Default => node.get_multiline_representation(),
        };
        use terminal_size::{terminal_size, Width};
        let size = terminal_size();
        let term_width = if let Some((Width(w), _)) = size {
            w as usize
        } else {
            88usize
        };

        let mut counter = 0;
        for val in lines.iter() {
            let base_characters = depth * 2;
            let expected_chars = (term_width - base_characters - 8).max(8);
            let sublines = textwrap::wrap(val, expected_chars);

            for (i, sb) in sublines.iter().enumerate() {
                self.fmt_depth(depth, s)?;
                match counter {
                    0 => write!(s, "* ")?,
                    _ => write!(s, "|   ")?,
                }
                counter += 1;
                match i {
                    0 => writeln!(s, "{sb}")?,
                    _ => writeln!(s, "  {sb}")?,
                }
            }
        }

        // Recursively handle children.
        let children = node.get_children();
        match &children[..] {
            // No children - stop printing.
            [] => Ok(()),
            // One child - print leg, then print the child tree.
            [child] => {
                // Leg: e.g. | | |
                self.fmt_depth(depth, s)?;
                writeln!(s, "|")?;

                // Child tree.
                self.fmt_tree_gitstyle(child, depth, s, t)
            }
            // Two children - print legs, print right child indented, print left child.
            [left, right] => {
                // Legs: e.g. | | |\
                self.fmt_depth(depth, s)?;
                writeln!(s, "|\\")?;

                // Right child tree, indented.
                self.fmt_tree_gitstyle(right, depth + 1, s, t)?;

                // Legs, e.g. | | |
                self.fmt_depth(depth, s)?;
                writeln!(s, "|")?;

                // Left child tree.
                self.fmt_tree_gitstyle(left, depth, s, t)
            }
            _ => unreachable!("Max two child nodes expected, got {}", children.len()),
        }
    }

    fn fmt_depth<'a, W: fmt::Write + 'a>(&self, depth: usize, s: &'a mut W) -> fmt::Result {
        // Print leading pipes for forks in ancestors that will be printed later.
        // e.g. "| | "
        for _ in 0..depth {
            write!(s, "| ")?;
        }
        Ok(())
    }
}

pub trait AsciiTreeDisplay: TreeDisplay {
    // // Print the whole tree represented by this node.
    fn fmt_tree<'a, W: fmt::Write + 'a>(&self, w: &'a mut W, simple: bool) -> fmt::Result
    where
        Self: Sized,
    {
        let builder = AsciiTreeBuilder::new(simple);
        builder.fmt_tree(self, w, simple)
    }
    fn fmt_tree_indent_style<'a, W: fmt::Write + 'a>(
        &self,
        indent: usize,
        s: &'a mut W,
    ) -> fmt::Result
    where
        Self: Sized,
    {
        let builder = AsciiTreeBuilder::new(false);
        builder.fmt_tree_indent_style(self, indent, s)
    }
}

impl<T: TreeDisplay> AsciiTreeDisplay for T {}
