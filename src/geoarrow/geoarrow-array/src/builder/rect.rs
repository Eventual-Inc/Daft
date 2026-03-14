use arrow_buffer::NullBufferBuilder;
use geo_traits::{CoordTrait, RectTrait};
use geoarrow_schema::{
    BoxType,
    error::{GeoArrowError, GeoArrowResult},
};

use crate::{array::RectArray, builder::SeparatedCoordBufferBuilder, scalar::Rect};

/// The GeoArrow equivalent to `Vec<Option<Rect>>`: a mutable collection of Rects.
///
/// Converting an [`RectBuilder`] into a [`RectArray`] is `O(1)`.
#[derive(Debug)]
pub struct RectBuilder {
    pub(crate) data_type: BoxType,
    pub(crate) lower: SeparatedCoordBufferBuilder,
    pub(crate) upper: SeparatedCoordBufferBuilder,
    pub(crate) validity: NullBufferBuilder,
}

impl RectBuilder {
    /// Creates a new empty [`RectBuilder`].
    pub fn new(typ: BoxType) -> Self {
        Self::with_capacity(typ, Default::default())
    }

    /// Creates a new [`RectBuilder`] with a capacity.
    pub fn with_capacity(typ: BoxType, capacity: usize) -> Self {
        Self {
            lower: SeparatedCoordBufferBuilder::with_capacity(capacity, typ.dimension()),
            upper: SeparatedCoordBufferBuilder::with_capacity(capacity, typ.dimension()),
            validity: NullBufferBuilder::new(capacity),
            data_type: typ,
        }
    }

    /// Reserves capacity for at least `additional` more Rects.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, additional: usize) {
        self.lower.reserve(additional);
        self.upper.reserve(additional);
    }

    /// Reserves the minimum capacity for at least `additional` more Rects.
    ///
    /// Unlike [`reserve`], this will not deliberately over-allocate to speculatively avoid
    /// frequent allocations. After calling `reserve_exact`, capacity will be greater than or equal
    /// to `self.len() + additional`. Does nothing if the capacity is already sufficient.
    ///
    /// Note that the allocator may give the collection more space than it
    /// requests. Therefore, capacity can not be relied upon to be precisely
    /// minimal. Prefer [`reserve`] if future insertions are expected.
    ///
    /// [`reserve`]: Self::reserve
    pub fn reserve_exact(&mut self, additional: usize) {
        self.lower.reserve_exact(additional);
        self.upper.reserve_exact(additional);
    }

    /// Shrinks the capacity of self to fit.
    pub fn shrink_to_fit(&mut self) {
        self.lower.shrink_to_fit();
        self.upper.shrink_to_fit();
        // self.validity.shrink_to_fit();
    }

    /// The canonical method to create a [`RectBuilder`] out of its internal components.
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// This function errors iff:
    ///
    /// - The validity is not `None` and its length is different from the number of geometries
    pub fn try_new(
        lower: SeparatedCoordBufferBuilder,
        upper: SeparatedCoordBufferBuilder,
        validity: NullBufferBuilder,
        data_type: BoxType,
    ) -> GeoArrowResult<Self> {
        if lower.len() != upper.len() {
            return Err(GeoArrowError::InvalidGeoArrow(
                "Lower and upper lengths must match".to_string(),
            ));
        }
        Ok(Self {
            lower,
            upper,
            validity,
            data_type,
        })
    }

    /// Consume the builder and convert to an immutable [`RectArray`]
    pub fn finish(mut self) -> RectArray {
        RectArray::new(
            self.lower.finish(),
            self.upper.finish(),
            self.validity.finish(),
            self.data_type.metadata().clone(),
        )
    }

    /// Add a new Rect to the end of this builder.
    #[inline]
    pub fn push_rect(&mut self, value: Option<&impl RectTrait<T = f64>>) {
        if let Some(value) = value {
            let min_coord = value.min();
            let max_coord = value.max();

            self.lower.push_coord(&min_coord);
            self.upper.push_coord(&max_coord);
            self.validity.append_non_null()
        } else {
            // Since it's a struct, we still need to push coords when null
            self.lower.push_constant(f64::NAN);
            self.upper.push_constant(f64::NAN);
            self.validity.append_null();
        }
    }

    /// Add a new null value to the end of this builder.
    #[inline]
    pub fn push_null(&mut self) {
        self.push_rect(None::<&Rect>);
    }

    /// Push min and max coordinates of a rect to the builder.
    #[inline]
    pub fn push_min_max(&mut self, min: &impl CoordTrait<T = f64>, max: &impl CoordTrait<T = f64>) {
        self.lower.push_coord(min);
        self.upper.push_coord(max);
        self.validity.append_non_null()
    }

    /// Create this builder from a iterator of Rects.
    pub fn from_rects<'a>(
        geoms: impl ExactSizeIterator<Item = &'a (impl RectTrait<T = f64> + 'a)>,
        typ: BoxType,
    ) -> Self {
        let mut mutable_array = Self::with_capacity(typ, geoms.len());
        geoms
            .into_iter()
            .for_each(|rect| mutable_array.push_rect(Some(rect)));
        mutable_array
    }

    /// Create this builder from a iterator of nullable Rects.
    pub fn from_nullable_rects<'a>(
        geoms: impl ExactSizeIterator<Item = Option<&'a (impl RectTrait<T = f64> + 'a)>>,
        typ: BoxType,
    ) -> Self {
        let mut mutable_array = Self::with_capacity(typ, geoms.len());
        geoms
            .into_iter()
            .for_each(|maybe_rect| mutable_array.push_rect(maybe_rect));
        mutable_array
    }
}
