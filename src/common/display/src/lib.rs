use mermaid::MermaidDisplayOptions;

pub mod mermaid;
pub mod tree;

pub trait Displayable {
    fn multiline_display(&self) -> Vec<String>;
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "python", derive(pyo3::FromPyObject))]
// do not change the order of the variants, pyo3 matches from top to bottom,
// If any variants have a union, it will always match the first one,
// So we need to order them from most specific to least specific.
pub enum DisplayFormat {
    // Display the tree in Mermaid format.
    Mermaid(MermaidDisplayOptions),
    // Display the tree in ASCII format.
    Ascii { simple: bool },
}
