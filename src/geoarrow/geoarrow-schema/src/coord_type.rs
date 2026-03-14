/// The permitted GeoArrow coordinate representations.
///
/// GeoArrow permits coordinate types to either be "Interleaved", where the X and Y coordinates are
/// in a single buffer as `XYXYXY` or "Separated", where the X and Y coordinates are in multiple
/// buffers as `XXXX` and `YYYY`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoordType {
    /// Interleaved coordinates.
    ///
    /// This stores coordinates in an Arrow
    /// [fixed-size-list-typed][arrow_schema::DataType::FixedSizeList] array.
    ///
    /// The size of the internal fixed-size list depends on the [dimension][crate::Dimension] of
    /// the array.
    ///
    /// ```notest
    /// FixedSizeList<double>[n_dim]
    /// ```
    Interleaved,

    /// Separated coordinates.
    ///
    /// This stores coordinates in an Arrow [struct-typed][arrow_schema::DataType::Struct] array:
    ///
    /// ```notest
    /// Struct<x: double, y: double, [z: double, [m: double>]]
    /// ```
    #[default]
    Separated,
}
