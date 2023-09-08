use std::{
    fmt::{self, Display, Write},
    sync::Arc,
};

pub(crate) trait TreeDisplay {
    // Required method: Get a list of lines representing this node. No trailing newlines.
    fn get_multiline_representation(&self) -> Vec<String>;

    // Required method: Get the human-readable name of this node.
    fn get_name(&self) -> String;

    // Required method: Get the children of the self node.
    fn get_children(&self) -> Vec<&Arc<Self>>;

    // Print the whole tree represented by this node.
    fn fmt_tree(&self, s: &mut String, simple: bool) -> fmt::Result {
        self.fmt_tree_gitstyle(0, s, simple)
    }

    // Print the tree recursively, and illustrate the tree structure with a single line per node + indentation.
    fn fmt_tree_indent_style(&self, indent: usize, s: &mut String) -> fmt::Result {
        // Print the current node.
        if indent > 0 {
            writeln!(s)?;
            write!(s, "{:indent$}", "", indent = 2 * indent)?;
        }
        let node_str = self.get_multiline_representation().join(", ");
        write!(s, "{node_str}")?;

        // Recursively handle children.
        let children = self.get_children();
        match children[..] {
            // No children - stop printing.
            [] => Ok(()),
            // One child.
            [child] => {
                // Child tree.
                child.fmt_tree_indent_style(indent + 1, s)
            }
            // Two children.
            [left, right] => {
                left.fmt_tree_indent_style(indent + 1, s)?;
                right.fmt_tree_indent_style(indent + 1, s)
            }
            _ => unreachable!("Max two child nodes expected, got {}", children.len()),
        }
    }

    // Print the tree recursively, and illustrate the tree structure in the same style as `git log --graph`.
    // `depth` is the number of forks in this node's ancestors.
    fn fmt_tree_gitstyle(&self, depth: usize, s: &mut String, simple: bool) -> fmt::Result {
        // Print the current node.
        // e.g. | | * <node contents line 1>
        //      | | | <node contents line 2>
        let lines = if simple {
            vec![self.get_name()]
        } else {
            self.get_multiline_representation()
        };
        for (i, val) in lines.iter().enumerate() {
            self.fmt_depth(depth, s)?;
            match i {
                0 => write!(s, "* ")?,
                _ => write!(s, "|   ")?,
            }
            writeln!(s, "{val}")?;
        }

        // Recursively handle children.
        let children = self.get_children();
        match children[..] {
            // No children - stop printing.
            [] => Ok(()),
            // One child - print leg, then print the child tree.
            [child] => {
                // Leg: e.g. | | |
                self.fmt_depth(depth, s)?;
                writeln!(s, "|")?;

                // Child tree.
                child.fmt_tree_gitstyle(depth, s, simple)
            }
            // Two children - print legs, print right child indented, print left child.
            [left, right] => {
                // Legs: e.g. | | |\
                self.fmt_depth(depth, s)?;
                writeln!(s, "|\\")?;

                // Right child tree, indented.
                right.fmt_tree_gitstyle(depth + 1, s, simple)?;

                // Legs, e.g. | | |
                self.fmt_depth(depth, s)?;
                writeln!(s, "|")?;

                // Left child tree.
                left.fmt_tree_gitstyle(depth, s, simple)
            }
            _ => unreachable!("Max two child nodes expected, got {}", children.len()),
        }
    }

    fn fmt_depth(&self, depth: usize, s: &mut String) -> fmt::Result {
        // Print leading pipes for forks in ancestors that will be printed later.
        // e.g. "| | "
        for _ in 0..depth {
            write!(s, "| ")?;
        }
        Ok(())
    }
}

impl TreeDisplay for crate::LogicalPlan {
    fn get_multiline_representation(&self) -> Vec<String> {
        self.multiline_display()
    }

    fn get_name(&self) -> String {
        self.name()
    }

    fn get_children(&self) -> Vec<&Arc<Self>> {
        self.children()
    }
}

// Single node display.
impl Display for crate::LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.get_multiline_representation().join(", "))?;
        Ok(())
    }
}
