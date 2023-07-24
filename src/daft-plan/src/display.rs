use std::fmt;

trait TreeDisplay {
    // Required method: Print just the self node in a single line. No trailing newline.
    fn fmt_oneline(&self, f: &mut fmt::Formatter) -> fmt::Result;

    // Required method: Get the children of the self node.
    fn get_children(&self) -> Vec<&Self>;

    // Print the whole tree represented by this node.
    fn fmt_tree(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_tree_gitstyle(0, f)
    }

    // Print the tree recursively, and illustrate the tree structure in the same style as `git log --graph`.
    // `depth` is the number of forks in this node's ancestors.
    fn fmt_tree_gitstyle(&self, depth: usize, f: &mut fmt::Formatter) -> fmt::Result {
        // Print the current node.
        // e.g. | | * <node contents>
        self.fmt_depth(depth, f)?;
        write!(f, "* ")?;
        self.fmt_oneline(f)?;
        writeln!(f)?;

        // Recursively handle children.
        let children = self.get_children();
        match children[..] {
            // No children - stop printing.
            [] => Ok(()),
            // One child - print leg, then print the child tree.
            [child] => {
                // Leg: e.g. | | |
                self.fmt_depth(depth, f)?;
                writeln!(f, "|")?;

                // Child tree.
                child.fmt_tree_gitstyle(depth, f)
            }
            // Two children - print legs, print right child indented, print left child.
            [left, right] => {
                // Legs: e.g. | | |\
                self.fmt_depth(depth, f)?;
                writeln!(f, "|\\")?;

                // Right child tree, indented.
                right.fmt_tree_gitstyle(depth + 1, f)?;

                // Legs, e.g. | | |
                self.fmt_depth(depth, f)?;
                writeln!(f, "|")?;

                // Left child tree.
                left.fmt_tree_gitstyle(depth, f)
            }
            _ => unreachable!("Max two child nodes expected, got {}", children.len()),
        }
    }

    fn fmt_depth(&self, depth: usize, f: &mut fmt::Formatter) -> fmt::Result {
        // Print leading pipes for forks in ancestors that will be printed later.
        // e.g. "| | "
        for _ in 0..depth {
            write!(f, "| ")?;
        }
        Ok(())
    }
}
