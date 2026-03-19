use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

pub(crate) fn offsets_buffer_i32_to_i64(offsets: &OffsetBuffer<i32>) -> OffsetBuffer<i64> {
    let i64_offsets = offsets.iter().map(|x| *x as i64).collect::<Vec<_>>();
    unsafe { OffsetBuffer::new_unchecked(i64_offsets.into()) }
}

pub(crate) fn offsets_buffer_i64_to_i32(
    offsets: &OffsetBuffer<i64>,
) -> GeoArrowResult<OffsetBuffer<i32>> {
    i32::try_from(*offsets.last()).map_err(|_| GeoArrowError::Overflow)?;

    let i32_offsets = offsets.iter().map(|x| *x as i32).collect::<Vec<_>>();
    Ok(unsafe { OffsetBuffer::new_unchecked(i32_offsets.into()) })
}

/// Offsets utils that I miss from arrow2
pub(crate) trait OffsetBufferUtils<O: OffsetSizeTrait> {
    /// Returns the length an array with these offsets would be.
    fn len_proxy(&self) -> usize;

    /// Returns a range (start, end) corresponding to the position `index`
    /// # Panic
    /// This function panics iff `index >= self.len_proxy()`
    fn start_end(&self, index: usize) -> (usize, usize);

    /// Returns the last offset.
    fn last(&self) -> &O;
}

impl<O: OffsetSizeTrait> OffsetBufferUtils<O> for OffsetBuffer<O> {
    /// Returns the length an array with these offsets would be.
    #[inline]
    fn len_proxy(&self) -> usize {
        self.len() - 1
    }

    /// Returns a range (start, end) corresponding to the position `index`
    ///
    /// # Panic
    ///
    /// Panics iff `index >= self.len_proxy()`
    #[inline]
    fn start_end(&self, index: usize) -> (usize, usize) {
        assert!(index < self.len_proxy());
        let start = self[index].to_usize().unwrap();
        let end = self[index + 1].to_usize().unwrap();
        (start, end)
    }

    /// Returns the last offset.
    #[inline]
    fn last(&self) -> &O {
        self.as_ref().last().unwrap()
    }
}

pub(crate) trait GeometryTypeName {
    /// Returns the name of the geometry type.
    fn name(&self) -> String;
}

impl<P, LS, Y, MP, ML, MY, GC, R, T, L> GeometryTypeName
    for geo_traits::GeometryType<'_, P, LS, Y, MP, ML, MY, GC, R, T, L>
where
    P: geo_traits::PointTrait,
    LS: geo_traits::LineStringTrait,
    Y: geo_traits::PolygonTrait,
    MP: geo_traits::MultiPointTrait,
    ML: geo_traits::MultiLineStringTrait,
    MY: geo_traits::MultiPolygonTrait,
    GC: geo_traits::GeometryCollectionTrait,
    R: geo_traits::RectTrait,
    T: geo_traits::TriangleTrait,
    L: geo_traits::LineTrait,
{
    fn name(&self) -> String {
        match self {
            Self::Point(_) => "Point".to_string(),
            Self::LineString(_) => "LineString".to_string(),
            Self::Polygon(_) => "Polygon".to_string(),
            Self::MultiPoint(_) => "MultiPoint".to_string(),
            Self::MultiLineString(_) => "MultiLineString".to_string(),
            Self::MultiPolygon(_) => "MultiPolygon".to_string(),
            Self::GeometryCollection(_) => "GeometryCollection".to_string(),
            Self::Rect(_) => "Rect".to_string(),
            Self::Triangle(_) => "Triangle".to_string(),
            Self::Line(_) => "Line".to_string(),
        }
    }
}
