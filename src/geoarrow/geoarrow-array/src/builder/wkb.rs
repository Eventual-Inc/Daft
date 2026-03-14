use arrow_array::{OffsetSizeTrait, builder::GenericBinaryBuilder};
use geo_traits::GeometryTrait;
use geoarrow_schema::{
    WkbType,
    error::{GeoArrowError, GeoArrowResult},
};
use wkb::{
    Endianness,
    reader::Wkb,
    writer::{WriteOptions, write_geometry},
};

use crate::{array::GenericWkbArray, capacity::WkbCapacity};

/// The GeoArrow equivalent to `Vec<Option<Wkb>>`: a mutable collection of Wkb buffers.
///
/// Converting a [`WkbBuilder`] into a [`GenericWkbArray`] is `O(1)`.
#[derive(Debug)]
pub struct WkbBuilder<O: OffsetSizeTrait>(GenericBinaryBuilder<O>, WkbType);

impl<O: OffsetSizeTrait> WkbBuilder<O> {
    /// Creates a new empty [`WkbBuilder`].
    pub fn new(typ: WkbType) -> Self {
        Self::with_capacity(typ, Default::default())
    }

    /// Initializes a new [`WkbBuilder`] with a pre-allocated capacity of slots and values.
    pub fn with_capacity(typ: WkbType, capacity: WkbCapacity) -> Self {
        Self(
            GenericBinaryBuilder::with_capacity(
                capacity.offsets_capacity,
                capacity.buffer_capacity,
            ),
            typ,
        )
    }

    // Upstream APIs don't exist for this yet. To implement this without upstream changes, we could
    // change to using manual `Vec`'s ourselves
    // pub fn reserve(&mut self, capacity: WkbCapacity) {
    // }

    /// Push a Geometry onto the end of this builder
    #[inline]
    pub fn push_geometry(
        &mut self,
        geom: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(geom) = geom {
            let wkb_options = WriteOptions {
                endianness: Endianness::LittleEndian,
            };
            write_geometry(&mut self.0, geom, &wkb_options)
                .map_err(|err| GeoArrowError::Wkb(err.to_string()))?;
            self.0.append_value("")
        } else {
            self.0.append_null()
        };
        Ok(())
    }

    /// Extend this builder from an iterator of Geometries.
    pub fn extend_from_iter<'a>(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait<T = f64> + 'a)>>,
    ) -> GeoArrowResult<()> {
        geoms
            .into_iter()
            .try_for_each(|maybe_geom| self.push_geometry(maybe_geom))?;
        Ok(())
    }

    /// Create this builder from a slice of nullable Geometries.
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        typ: WkbType,
    ) -> GeoArrowResult<Self> {
        let capacity = WkbCapacity::from_geometries(geoms.iter().map(|x| x.as_ref()));
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()))?;
        Ok(array)
    }

    /// Push raw WKB bytes onto the end of this builder.
    ///
    /// This method validates that the input bytes represent valid WKB before appending.
    /// If the bytes are `None`, a null value is appended.
    ///
    /// # Errors
    ///
    /// Returns an error if the input bytes are not valid WKB format.
    ///
    /// # Example
    ///
    /// ```
    /// use geoarrow_array::builder::WkbBuilder;
    /// use geoarrow_array::GeoArrowArray;
    /// use geoarrow_schema::WkbType;
    ///
    /// let mut builder = WkbBuilder::<i32>::new(WkbType::default());
    ///
    /// // Valid WKB for a Point(1.0, 2.0) in little-endian
    /// let wkb_bytes = vec![
    ///     0x01, // Little-endian
    ///     0x01, 0x00, 0x00, 0x00, // Point type
    ///     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // x = 1.0
    ///     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
    /// ];
    ///
    /// builder.push_wkb(Some(&wkb_bytes)).unwrap();
    /// builder.push_wkb(None).unwrap(); // Append null
    ///
    /// let array = builder.finish();
    /// assert_eq!(array.len(), 2);
    /// ```
    #[inline]
    pub fn push_wkb(&mut self, wkb: Option<&[u8]>) -> GeoArrowResult<()> {
        if let Some(bytes) = wkb {
            // Validate that the bytes are valid WKB
            Wkb::try_new(bytes).map_err(|err| GeoArrowError::Wkb(err.to_string()))?;
            self.0.append_value(bytes);
        } else {
            self.0.append_null();
        }
        Ok(())
    }

    /// Push raw WKB bytes onto the end of this builder without validation.
    ///
    /// This method directly appends the input bytes to the underlying buffer without
    /// validating that they represent valid WKB. If the bytes are `None`, a null value
    /// is appended.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it does not validate that the input bytes are
    /// valid WKB format. Calling this with invalid WKB data may result in undefined
    /// behavior when the resulting array is used with operations that assume valid WKB.
    ///
    /// The caller must ensure that:
    /// - The bytes represent valid WKB according to the OGC WKB specification
    /// - The byte order (endianness) is correctly specified in the WKB header
    /// - The geometry type and coordinates are properly encoded
    ///
    /// # Example
    ///
    /// ```
    /// use geoarrow_array::builder::WkbBuilder;
    /// use geoarrow_array::GeoArrowArray;
    /// use geoarrow_schema::WkbType;
    ///
    /// let mut builder = WkbBuilder::<i32>::new(WkbType::default());
    ///
    /// // Valid WKB for a Point(1.0, 2.0) in little-endian
    /// let wkb_bytes = vec![
    ///     0x01, // Little-endian
    ///     0x01, 0x00, 0x00, 0x00, // Point type
    ///     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // x = 1.0
    ///     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
    /// ];
    ///
    /// unsafe {
    ///     builder.push_wkb_unchecked(Some(&wkb_bytes));
    ///     builder.push_wkb_unchecked(None); // Append null
    /// }
    ///
    /// let array = builder.finish();
    /// assert_eq!(array.len(), 2);
    /// ```
    #[inline]
    pub unsafe fn push_wkb_unchecked(&mut self, wkb: Option<&[u8]>) {
        if let Some(bytes) = wkb {
            self.0.append_value(bytes);
        } else {
            self.0.append_null();
        }
    }

    /// Consume this builder and convert to a [GenericWkbArray].
    ///
    /// This is `O(1)`.
    pub fn finish(mut self) -> GenericWkbArray<O> {
        GenericWkbArray::new(self.0.finish(), self.1.metadata().clone())
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::trait_::GeoArrowArray;

//     /// Valid WKB for Point(1.0, 2.0) in little-endian format
//     fn point_wkb() -> Vec<u8> {
//         let point = geo::Point::new(1.0, 2.0);
//         let mut buf = Vec::new();
//         wkb::writer::write_point(&mut buf, &point, &Default::default()).unwrap();
//         buf
//     }

//     /// Valid WKB for Point(3.0, 4.0) in little-endian format
//     fn point_wkb_2() -> Vec<u8> {
//         let point = geo::Point::new(3.0, 4.0);
//         let mut buf = Vec::new();
//         wkb::writer::write_point(&mut buf, &point, &Default::default()).unwrap();
//         buf
//     }

//     /// Invalid WKB (too short)
//     fn invalid_wkb() -> Vec<u8> {
//         vec![0x01, 0x01]
//     }

//     #[test]
//     fn test_push_raw_valid() {
//         let mut builder = WkbBuilder::<i32>::new(WkbType::default());
//         let wkb = point_wkb();

//         // Should succeed with valid WKB
//         builder.push_wkb(Some(&wkb)).unwrap();

//         let array = builder.finish();
//         assert_eq!(array.len(), 1);
//         assert!(!array.is_null(0));
//     }

//     #[test]
//     fn test_push_raw_multiple() {
//         let mut builder = WkbBuilder::<i32>::new(WkbType::default());
//         let wkb1 = point_wkb();
//         let wkb2 = point_wkb_2();

//         builder.push_wkb(Some(&wkb1)).unwrap();
//         builder.push_wkb(Some(&wkb2)).unwrap();

//         let array = builder.finish();
//         assert_eq!(array.len(), 2);
//         assert!(!array.is_null(0));
//         assert!(!array.is_null(1));
//     }

//     #[test]
//     fn test_push_raw_null() {
//         let mut builder = WkbBuilder::<i32>::new(WkbType::default());

//         // Push null value
//         builder.push_wkb(None).unwrap();

//         let array = builder.finish();
//         assert_eq!(array.len(), 1);
//         assert!(array.is_null(0));
//     }

//     #[test]
//     fn test_push_raw_mixed_with_nulls() {
//         let mut builder = WkbBuilder::<i32>::new(WkbType::default());
//         let wkb = point_wkb();

//         builder.push_wkb(Some(&wkb)).unwrap();
//         builder.push_wkb(None).unwrap();
//         builder.push_wkb(Some(&wkb)).unwrap();

//         let array = builder.finish();
//         assert_eq!(array.len(), 3);
//         assert!(!array.is_null(0));
//         assert!(array.is_null(1));
//         assert!(!array.is_null(2));
//     }

//     #[test]
//     fn test_push_raw_invalid() {
//         let mut builder = WkbBuilder::<i32>::new(WkbType::default());
//         let invalid = invalid_wkb();

//         // Should fail with invalid WKB
//         let result = builder.push_wkb(Some(&invalid));
//         assert!(result.is_err());
//     }

//     #[test]
//     fn test_push_raw_unchecked_valid() {
//         let mut builder = WkbBuilder::<i32>::new(WkbType::default());
//         let wkb = point_wkb();

//         unsafe {
//             builder.push_wkb_unchecked(Some(&wkb));
//         }

//         let array = builder.finish();
//         assert_eq!(array.len(), 1);
//         assert!(!array.is_null(0));
//     }

//     #[test]
//     fn test_push_raw_unchecked_null() {
//         let mut builder = WkbBuilder::<i32>::new(WkbType::default());

//         unsafe {
//             builder.push_wkb_unchecked(None);
//         }

//         let array = builder.finish();
//         assert_eq!(array.len(), 1);
//         assert!(array.is_null(0));
//     }

//     #[test]
//     fn test_push_raw_unchecked_multiple() {
//         let mut builder = WkbBuilder::<i32>::new(WkbType::default());
//         let wkb1 = point_wkb();
//         let wkb2 = point_wkb_2();

//         unsafe {
//             builder.push_wkb_unchecked(Some(&wkb1));
//             builder.push_wkb_unchecked(None);
//             builder.push_wkb_unchecked(Some(&wkb2));
//         }

//         let array = builder.finish();
//         assert_eq!(array.len(), 3);
//         assert!(!array.is_null(0));
//         assert!(array.is_null(1));
//         assert!(!array.is_null(2));
//     }

//     #[test]
//     fn test_push_raw_with_i64_offset() {
//         let mut builder = WkbBuilder::<i64>::new(WkbType::default());
//         let wkb = point_wkb();

//         builder.push_wkb(Some(&wkb)).unwrap();
//         builder.push_wkb(None).unwrap();

//         let array = builder.finish();
//         assert_eq!(array.len(), 2);
//         assert!(!array.is_null(0));
//         assert!(array.is_null(1));
//     }
// }
