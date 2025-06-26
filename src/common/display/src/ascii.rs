use std::fmt;

use crate::{tree::TreeDisplay, DisplayLevel};

// Print the tree recursively, and illustrate the tree structure with a single line per node + indentation.
fn fmt_tree_indent_style<'a, W: fmt::Write + 'a>(
    node: &dyn TreeDisplay,
    indent: usize,
    s: &'a mut W,
) -> fmt::Result {
    // Print the current node.
    if indent > 0 {
        writeln!(s)?;
        write!(s, "{:indent$}", "", indent = 2 * indent)?;
    }

    let node_str = node.display_as(DisplayLevel::Default);
    s.write_str(&node_str)?;

    // Recursively handle children.
    let children = node.get_children();

    match children[..] {
        // No children - stop printing.
        [] => Ok(()),
        // One child.
        [child] => {
            // Child tree.
            fmt_tree_indent_style(child, indent + 1, s)
        }
        // Two children.
        [left, right] => {
            fmt_tree_indent_style(left, indent + 1, s)?;
            fmt_tree_indent_style(right, indent + 1, s)
        }
        _ => unreachable!("Max two child nodes expected, got {}", children.len()),
    }
}

// Print the tree recursively, and illustrate the tree structure in the same style as `git log --graph`.
// `depth` is the number of forks in this node's ancestors.
pub fn fmt_tree_gitstyle<'a, W: fmt::Write + 'a>(
    node: &dyn TreeDisplay,
    depth: usize,
    s: &'a mut W,
    level: crate::DisplayLevel,
) -> fmt::Result {
    use terminal_size::{terminal_size, Width};

    // Print the current node.
    // e.g. | | * <node contents line 1>
    //      | | | <node contents line 2>

    let desc = node.display_as(level);
    let lines = desc.lines();

    let size = terminal_size();
    let term_width = if let Some((Width(w), _)) = size {
        w as usize
    } else {
        88usize
    };

    let mut counter = 0;
    let base_characters = depth * 2;
    let expected_chars = (term_width - base_characters - 8).max(8);

    for val in lines {
        let sublines = textwrap::wrap(val, expected_chars);

        for (i, sb) in sublines.iter().enumerate() {
            fmt_depth(depth, s)?;
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
    match children[..] {
        // No children - stop printing.
        [] => Ok(()),
        // One child - print leg, then print the child tree.
        [child] => {
            // Leg: e.g. | | |
            fmt_depth(depth, s)?;
            writeln!(s, "|")?;

            // Child tree.
            fmt_tree_gitstyle(child, depth, s, level)
        }
        // Two children - print legs, print right child indented, print left child.
        [left, right] => {
            // Legs: e.g. | | |\
            fmt_depth(depth, s)?;
            writeln!(s, "|\\")?;

            // Right child tree, indented.
            fmt_tree_gitstyle(right, depth + 1, s, level)?;

            // Legs, e.g. | | |
            fmt_depth(depth, s)?;
            writeln!(s, "|")?;

            // Left child tree.
            fmt_tree_gitstyle(left, depth, s, level)
        }
        _ => unreachable!("Max two child nodes expected, got {}", children.len()),
    }
}

fn fmt_depth<'a, W: fmt::Write + 'a>(depth: usize, s: &'a mut W) -> fmt::Result {
    // Print leading pipes for forks in ancestors that will be printed later.
    // e.g. "| | "
    for _ in 0..depth {
        write!(s, "| ")?;
    }
    Ok(())
}

pub trait AsciiTreeDisplay: TreeDisplay {
    // // Print the whole tree represented by this node.
    fn fmt_tree<'a, W: fmt::Write + 'a>(&self, w: &'a mut W, simple: bool) -> fmt::Result
    where
        Self: Sized,
    {
        let level = if simple {
            crate::DisplayLevel::Compact
        } else {
            crate::DisplayLevel::Default
        };

        fmt_tree_gitstyle(self, 0, w, level)
    }

    fn fmt_tree_indent_style<'a, W: fmt::Write + 'a>(
        &self,
        indent: usize,
        s: &'a mut W,
    ) -> fmt::Result
    where
        Self: Sized,
    {
        fmt_tree_indent_style(self, indent, s)
    }
}

impl<T: TreeDisplay> AsciiTreeDisplay for T {}
