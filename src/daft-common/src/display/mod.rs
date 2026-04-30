pub mod ascii;
pub mod mermaid;
pub mod table_display;
pub mod tree;
pub mod utils;

pub trait DisplayAs {
    fn display_as(&self, level: DisplayLevel) -> String;
}

#[derive(Clone, Copy)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum DisplayLevel {
    /// A compact display, showing only the most important details.
    Compact,
    /// The default display, showing common details.
    Default,
    /// A verbose display, showing all available details.
    Verbose,
}
