use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use geo_traits::*;
use geoarrow_schema::{
    Dimension, GeometryCollectionType, GeometryType, LineStringType, Metadata, MultiLineStringType,
    MultiPointType, MultiPolygonType, PointType, PolygonType,
    error::{GeoArrowError, GeoArrowResult},
    type_id::GeometryTypeId,
};

use crate::{
    GeoArrowArray,
    array::{DimensionIndex, GenericWkbArray, GeometryArray},
    builder::{
        GeometryCollectionBuilder, LineStringBuilder, MultiLineStringBuilder, MultiPointBuilder,
        MultiPolygonBuilder, PointBuilder, PolygonBuilder,
        geo_trait_wrappers::{LineWrapper, RectWrapper, TriangleWrapper},
    },
    capacity::GeometryCapacity,
    trait_::{GeoArrowArrayAccessor, GeoArrowArrayBuilder},
};

pub(crate) const DEFAULT_PREFER_MULTI: bool = false;

/// The GeoArrow equivalent to a `Vec<Option<Geometry>>`: a mutable collection of Geometries.
///
/// Each Geometry can have a different dimension. All geometries must have the same coordinate
/// type.
///
/// This currently has the caveat that these geometries must be a _primitive_ geometry type. This
/// does not currently support nested GeometryCollection objects.
///
/// Converting an [`GeometryBuilder`] into a [`GeometryArray`] is `O(1)`.
///
/// # Invariants
///
/// - All arrays must have the same coordinate layout (interleaved or separated)
#[derive(Debug)]
pub struct GeometryBuilder {
    metadata: Arc<Metadata>,

    // Invariant: every item in `types` is `> 0 && < fields.len()`
    types: Vec<i8>,

    /// An array of PointArray, ordered XY, XYZ, XYM, XYZM
    points: [PointBuilder; 4],
    line_strings: [LineStringBuilder; 4],
    polygons: [PolygonBuilder; 4],
    mpoints: [MultiPointBuilder; 4],
    mline_strings: [MultiLineStringBuilder; 4],
    mpolygons: [MultiPolygonBuilder; 4],
    gcs: [GeometryCollectionBuilder; 4],

    // Invariant: `offsets.len() == types.len()`
    offsets: Vec<i32>,

    /// Whether to prefer multi or single arrays for new geometries.
    ///
    /// E.g. if this is `true` and a Point geometry is added, it will be added to the
    /// MultiPointBuilder. If this is `false`, the Point geometry will be added to the
    /// PointBuilder.
    ///
    /// The idea is that always adding multi-geometries will make it easier to downcast later.
    pub(crate) prefer_multi: bool,

    /// The number of nulls that has been deferred and are still to be written.
    ///
    /// Adding nulls is tricky. We often want to use this builder as a generic builder for data
    /// from unknown sources, which then gets downcasted to an array of a specific type.
    ///
    /// In a large majority of the time, this builder will have only data of a single type, which
    /// can then get downcasted to a simple array of a single geometry type and dimension. But in
    /// order for this process to be easy, we want the nulls to be assigned to the same array type
    /// as the actual data.
    ///
    /// When there's a valid geometry pushed before the null, we can add the null to an existing
    /// non-null array type, but if there are no valid geometries yet, we don't know which array to
    /// push the null to. This `deferred_nulls` is the number of initial null values that haven't
    /// yet been written to an array, because we don't know which array to write them to.
    deferred_nulls: usize,
}

impl<'a> GeometryBuilder {
    /// Creates a new empty [`GeometryBuilder`].
    pub fn new(typ: GeometryType) -> Self {
        Self::with_capacity(typ, Default::default())
    }

    /// Creates a new [`GeometryBuilder`] with given capacity and no validity.
    pub fn with_capacity(typ: GeometryType, capacity: GeometryCapacity) -> Self {
        let coord_type = typ.coord_type();

        let points = core::array::from_fn(|i| {
            let dim = Dimension::from_order(i).unwrap();
            PointBuilder::with_capacity(
                PointType::new(dim, Default::default()).with_coord_type(coord_type),
                capacity.point(dim),
            )
        });
        let line_strings = core::array::from_fn(|i| {
            let dim = Dimension::from_order(i).unwrap();
            LineStringBuilder::with_capacity(
                LineStringType::new(dim, Default::default()).with_coord_type(coord_type),
                capacity.line_string(dim),
            )
        });
        let polygons = core::array::from_fn(|i| {
            let dim = Dimension::from_order(i).unwrap();
            PolygonBuilder::with_capacity(
                PolygonType::new(dim, Default::default()).with_coord_type(coord_type),
                capacity.polygon(dim),
            )
        });
        let mpoints = core::array::from_fn(|i| {
            let dim = Dimension::from_order(i).unwrap();
            MultiPointBuilder::with_capacity(
                MultiPointType::new(dim, Default::default()).with_coord_type(coord_type),
                capacity.multi_point(dim),
            )
        });
        let mline_strings = core::array::from_fn(|i| {
            let dim = Dimension::from_order(i).unwrap();
            MultiLineStringBuilder::with_capacity(
                MultiLineStringType::new(dim, Default::default()).with_coord_type(coord_type),
                capacity.multi_line_string(dim),
            )
        });
        let mpolygons = core::array::from_fn(|i| {
            let dim = Dimension::from_order(i).unwrap();
            MultiPolygonBuilder::with_capacity(
                MultiPolygonType::new(dim, Default::default()).with_coord_type(coord_type),
                capacity.multi_polygon(dim),
            )
        });
        let gcs = core::array::from_fn(|i| {
            let dim = Dimension::from_order(i).unwrap();
            GeometryCollectionBuilder::with_capacity(
                GeometryCollectionType::new(dim, Default::default()).with_coord_type(coord_type),
                capacity.geometry_collection(dim),
            )
        });

        // Don't store array metadata on child arrays
        Self {
            metadata: typ.metadata().clone(),
            types: vec![],
            points,
            line_strings,
            polygons,
            mpoints,
            mline_strings,
            mpolygons,
            gcs,
            offsets: vec![],
            deferred_nulls: 0,
            prefer_multi: DEFAULT_PREFER_MULTI,
        }
    }

    /// Change whether to prefer multi or single arrays for new single-part geometries.
    ///
    /// If `true`, a new `Point` will be added to the `MultiPointBuilder` child array, a new
    /// `LineString` will be added to the `MultiLineStringBuilder` child array, and a new `Polygon`
    /// will be added to the `MultiPolygonBuilder` child array.
    ///
    /// This can be desired when the user wants to downcast the array to a single geometry array
    /// later, as casting to a, say, `MultiPointArray` from a `GeometryArray` could be done
    /// zero-copy.
    ///
    /// Note that only geometries added _after_ this method is called will be affected.
    pub fn with_prefer_multi(self, prefer_multi: bool) -> Self {
        Self {
            prefer_multi,
            gcs: self.gcs.map(|gc| gc.with_prefer_multi(prefer_multi)),
            ..self
        }
    }

    /// Reserves capacity for at least `additional` more geometries.
    ///
    /// The collection may reserve more space to speculatively avoid frequent reallocations. After
    /// calling `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    pub fn reserve(&mut self, capacity: GeometryCapacity) {
        let total_num_geoms = capacity.total_num_geoms();
        self.types.reserve(total_num_geoms);
        self.offsets.reserve(total_num_geoms);

        capacity.points.iter().enumerate().for_each(|(i, cap)| {
            self.points[i].reserve(*cap);
        });
        capacity
            .line_strings
            .iter()
            .enumerate()
            .for_each(|(i, cap)| {
                self.line_strings[i].reserve(*cap);
            });
        capacity.polygons.iter().enumerate().for_each(|(i, cap)| {
            self.polygons[i].reserve(*cap);
        });
        capacity.mpoints.iter().enumerate().for_each(|(i, cap)| {
            self.mpoints[i].reserve(*cap);
        });
        capacity
            .mline_strings
            .iter()
            .enumerate()
            .for_each(|(i, cap)| {
                self.mline_strings[i].reserve(*cap);
            });
        capacity.mpolygons.iter().enumerate().for_each(|(i, cap)| {
            self.mpolygons[i].reserve(*cap);
        });
        capacity.gcs.iter().enumerate().for_each(|(i, cap)| {
            self.gcs[i].reserve(*cap);
        });
    }

    /// Reserves the minimum capacity for at least `additional` more Geometries.
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
    pub fn reserve_exact(&mut self, capacity: GeometryCapacity) {
        let total_num_geoms = capacity.total_num_geoms();

        self.types.reserve_exact(total_num_geoms);
        self.offsets.reserve_exact(total_num_geoms);

        capacity.points.iter().enumerate().for_each(|(i, cap)| {
            self.points[i].reserve_exact(*cap);
        });
        capacity
            .line_strings
            .iter()
            .enumerate()
            .for_each(|(i, cap)| {
                self.line_strings[i].reserve_exact(*cap);
            });
        capacity.polygons.iter().enumerate().for_each(|(i, cap)| {
            self.polygons[i].reserve_exact(*cap);
        });
        capacity.mpoints.iter().enumerate().for_each(|(i, cap)| {
            self.mpoints[i].reserve_exact(*cap);
        });
        capacity
            .mline_strings
            .iter()
            .enumerate()
            .for_each(|(i, cap)| {
                self.mline_strings[i].reserve_exact(*cap);
            });
        capacity.mpolygons.iter().enumerate().for_each(|(i, cap)| {
            self.mpolygons[i].reserve_exact(*cap);
        });
        capacity.gcs.iter().enumerate().for_each(|(i, cap)| {
            self.gcs[i].reserve_exact(*cap);
        });
    }

    /// Shrinks the capacity of self to fit.
    pub fn shrink_to_fit(&mut self) {
        self.points.iter_mut().for_each(PointBuilder::shrink_to_fit);
        self.line_strings
            .iter_mut()
            .for_each(LineStringBuilder::shrink_to_fit);
        self.polygons
            .iter_mut()
            .for_each(PolygonBuilder::shrink_to_fit);
        self.mpoints
            .iter_mut()
            .for_each(MultiPointBuilder::shrink_to_fit);
        self.mline_strings
            .iter_mut()
            .for_each(MultiLineStringBuilder::shrink_to_fit);
        self.mpolygons
            .iter_mut()
            .for_each(MultiPolygonBuilder::shrink_to_fit);
        self.gcs
            .iter_mut()
            .for_each(GeometryCollectionBuilder::shrink_to_fit);

        self.offsets.shrink_to_fit();
        self.types.shrink_to_fit();
    }

    /// Consume the builder and convert to an immutable [`GeometryArray`]
    pub fn finish(mut self) -> GeometryArray {
        // If there are still deferred nulls to be written, then there aren't any valid geometries
        // in this array, and just choose a child to write them to.
        if self.deferred_nulls > 0 {
            let dim = Dimension::XY;
            let child = &mut self.points[dim.order()];
            let type_id = child.geometry_type_id();
            Self::flush_deferred_nulls(
                &mut self.deferred_nulls,
                child,
                &mut self.offsets,
                &mut self.types,
                type_id,
            );
        }

        GeometryArray::new(
            self.types.into(),
            self.offsets.into(),
            self.points.map(|arr| arr.finish()),
            self.line_strings.map(|arr| arr.finish()),
            self.polygons.map(|arr| arr.finish()),
            self.mpoints.map(|arr| arr.finish()),
            self.mline_strings.map(|arr| arr.finish()),
            self.mpolygons.map(|arr| arr.finish()),
            self.gcs.map(|arr| arr.finish()),
            self.metadata,
        )
    }

    /// Add a new Point to the end of this array.
    ///
    /// If `self.prefer_multi` is `true`, it will be stored in the `MultiPointBuilder` child
    /// array. Otherwise, it will be stored in the `PointBuilder` child array.
    #[inline]
    fn push_point(&mut self, value: Option<&impl PointTrait<T = f64>>) -> GeoArrowResult<()> {
        if let Some(point) = value {
            let dim: Dimension = point.dim().try_into().unwrap();
            let array_idx = dim.order();

            if self.prefer_multi {
                let child = &mut self.mpoints[array_idx];
                let type_id = child.geometry_type_id();

                Self::flush_deferred_nulls(
                    &mut self.deferred_nulls,
                    child,
                    &mut self.offsets,
                    &mut self.types,
                    type_id,
                );
                Self::add_type(child, &mut self.offsets, &mut self.types, type_id);
                child.push_point(Some(point))?;
            } else {
                let child = &mut self.points[array_idx];
                let type_id = child.geometry_type_id();

                Self::flush_deferred_nulls(
                    &mut self.deferred_nulls,
                    child,
                    &mut self.offsets,
                    &mut self.types,
                    type_id,
                );
                Self::add_type(child, &mut self.offsets, &mut self.types, type_id);
                child.push_point(Some(point));
            }
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    fn add_type<B: GeoArrowArrayBuilder>(
        child: &mut B,
        offsets: &mut Vec<i32>,
        types: &mut Vec<i8>,
        type_id: i8,
    ) {
        offsets.push(child.len().try_into().unwrap());
        types.push(type_id);
    }

    #[inline]
    fn add_point_type(&mut self, dim: Dimension) {
        let child = &self.points[dim.order()];
        self.offsets.push(child.len().try_into().unwrap());
        self.types.push(child.geometry_type_id());
    }

    /// Add a new LineString to the end of this array.
    ///
    /// If `self.prefer_multi` is `true`, it will be stored in the `MultiLineStringBuilder` child
    /// array. Otherwise, it will be stored in the `LineStringBuilder` child array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    fn push_line_string(
        &mut self,
        value: Option<&impl LineStringTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(line_string) = value {
            let dim: Dimension = line_string.dim().try_into().unwrap();
            let array_idx = dim.order();

            if self.prefer_multi {
                let child = &mut self.mline_strings[array_idx];
                let type_id = child.geometry_type_id();

                Self::flush_deferred_nulls(
                    &mut self.deferred_nulls,
                    child,
                    &mut self.offsets,
                    &mut self.types,
                    type_id,
                );
                Self::add_type(child, &mut self.offsets, &mut self.types, type_id);
                child.push_line_string(Some(line_string))?;
            } else {
                let child = &mut self.line_strings[array_idx];
                let type_id = child.geometry_type_id();

                Self::flush_deferred_nulls(
                    &mut self.deferred_nulls,
                    child,
                    &mut self.offsets,
                    &mut self.types,
                    type_id,
                );
                Self::add_type(child, &mut self.offsets, &mut self.types, type_id);
                child.push_line_string(Some(line_string))?;
            }
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    fn add_line_string_type(&mut self, dim: Dimension) {
        let child = &self.line_strings[dim.order()];
        self.offsets.push(child.len().try_into().unwrap());
        self.types.push(child.geometry_type_id());
    }

    /// Add a new Polygon to the end of this array.
    ///
    /// If `self.prefer_multi` is `true`, it will be stored in the `MultiPolygonBuilder` child
    /// array. Otherwise, it will be stored in the `PolygonBuilder` child array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    fn push_polygon(&mut self, value: Option<&impl PolygonTrait<T = f64>>) -> GeoArrowResult<()> {
        if let Some(polygon) = value {
            let dim: Dimension = polygon.dim().try_into().unwrap();
            let array_idx = dim.order();

            if self.prefer_multi {
                let child = &mut self.mpolygons[array_idx];
                let type_id = child.geometry_type_id();

                Self::flush_deferred_nulls(
                    &mut self.deferred_nulls,
                    child,
                    &mut self.offsets,
                    &mut self.types,
                    type_id,
                );
                Self::add_type(child, &mut self.offsets, &mut self.types, type_id);
                child.push_polygon(Some(polygon))?;
            } else {
                let child = &mut self.polygons[array_idx];
                let type_id = child.geometry_type_id();

                Self::flush_deferred_nulls(
                    &mut self.deferred_nulls,
                    child,
                    &mut self.offsets,
                    &mut self.types,
                    type_id,
                );
                Self::add_type(child, &mut self.offsets, &mut self.types, type_id);
                child.push_polygon(Some(polygon))?;
            }
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    fn add_polygon_type(&mut self, dim: Dimension) {
        let child = &self.polygons[dim.order()];
        self.offsets.push(child.len().try_into().unwrap());
        self.types.push(child.geometry_type_id());
    }

    /// Add a new MultiPoint to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    fn push_multi_point(
        &mut self,
        value: Option<&impl MultiPointTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(multi_point) = value {
            let dim: Dimension = multi_point.dim().try_into().unwrap();
            let array_idx = dim.order();

            let child = &mut self.mpoints[array_idx];
            let type_id = child.geometry_type_id();

            Self::flush_deferred_nulls(
                &mut self.deferred_nulls,
                child,
                &mut self.offsets,
                &mut self.types,
                type_id,
            );
            Self::add_type(child, &mut self.offsets, &mut self.types, type_id);
            child.push_multi_point(Some(multi_point))?;
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    fn add_multi_point_type(&mut self, dim: Dimension) {
        let child = &self.mpoints[dim.order()];
        self.offsets.push(child.len().try_into().unwrap());
        self.types.push(child.geometry_type_id());
    }

    /// Add a new MultiLineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    fn push_multi_line_string(
        &mut self,
        value: Option<&impl MultiLineStringTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(multi_line_string) = value {
            let dim: Dimension = multi_line_string.dim().try_into().unwrap();
            let array_idx = dim.order();

            let child = &mut self.mline_strings[array_idx];
            let type_id = child.geometry_type_id();

            Self::flush_deferred_nulls(
                &mut self.deferred_nulls,
                child,
                &mut self.offsets,
                &mut self.types,
                type_id,
            );
            Self::add_type(child, &mut self.offsets, &mut self.types, type_id);
            child.push_multi_line_string(Some(multi_line_string))?;
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    fn add_multi_line_string_type(&mut self, dim: Dimension) {
        let child = &self.mline_strings[dim.order()];
        self.offsets.push(child.len().try_into().unwrap());
        self.types.push(child.geometry_type_id());
    }

    /// Add a new MultiPolygon to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    fn push_multi_polygon(
        &mut self,
        value: Option<&impl MultiPolygonTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(multi_polygon) = value {
            let dim: Dimension = multi_polygon.dim().try_into().unwrap();
            let array_idx = dim.order();

            let child = &mut self.mpolygons[array_idx];
            let type_id = child.geometry_type_id();

            Self::flush_deferred_nulls(
                &mut self.deferred_nulls,
                child,
                &mut self.offsets,
                &mut self.types,
                type_id,
            );
            Self::add_type(child, &mut self.offsets, &mut self.types, type_id);
            child.push_multi_polygon(Some(multi_polygon))?;
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    fn add_multi_polygon_type(&mut self, dim: Dimension) {
        let child = &self.mpolygons[dim.order()];
        self.offsets.push(child.len().try_into().unwrap());
        self.types.push(child.geometry_type_id());
    }

    /// Add a new geometry to this builder
    #[inline]
    pub fn push_geometry(
        &mut self,
        value: Option<&'a impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        use geo_traits::GeometryType::*;

        if let Some(geom) = value {
            match geom.as_type() {
                Point(g) => {
                    self.push_point(Some(g))?;
                }
                LineString(g) => {
                    self.push_line_string(Some(g))?;
                }
                Polygon(g) => {
                    self.push_polygon(Some(g))?;
                }
                MultiPoint(p) => self.push_multi_point(Some(p))?,
                MultiLineString(p) => self.push_multi_line_string(Some(p))?,
                MultiPolygon(p) => self.push_multi_polygon(Some(p))?,
                GeometryCollection(gc) => {
                    if gc.num_geometries() == 1 {
                        self.push_geometry(Some(&gc.geometry(0).unwrap()))?
                    } else {
                        self.push_geometry_collection(Some(gc))?
                    }
                }
                Rect(r) => self.push_polygon(Some(&RectWrapper::try_new(r)?))?,
                Triangle(tri) => self.push_polygon(Some(&TriangleWrapper(tri)))?,
                Line(l) => self.push_line_string(Some(&LineWrapper(l)))?,
            };
        } else {
            self.push_null();
        }
        Ok(())
    }

    /// Add a new GeometryCollection to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[inline]
    fn push_geometry_collection(
        &mut self,
        value: Option<&impl GeometryCollectionTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        if let Some(gc) = value {
            let dim: Dimension = gc.dim().try_into().unwrap();
            let array_idx = dim.order();

            let child = &mut self.gcs[array_idx];
            let type_id = child.geometry_type_id();

            Self::flush_deferred_nulls(
                &mut self.deferred_nulls,
                child,
                &mut self.offsets,
                &mut self.types,
                type_id,
            );
            Self::add_type(child, &mut self.offsets, &mut self.types, type_id);
            child.push_geometry_collection(Some(gc))?;
        } else {
            self.push_null();
        };

        Ok(())
    }

    #[inline]
    fn add_geometry_collection_type(&mut self, dim: Dimension) {
        let child = &self.gcs[dim.order()];
        self.offsets.push(child.len().try_into().unwrap());
        self.types.push(child.geometry_type_id());
    }

    /// Push a null to this builder.
    ///
    /// Adding null values to a union array is tricky, because you don't want to add a null to a
    /// child that would otherwise be totally empty. Ideally, as few children as possible exist and
    /// are non-empty.
    ///
    /// We handle that by pushing nulls to the first non-empty child we find. If no underlying
    /// arrays are non-empty, we add to an internal counter instead. Once the first non-empty
    /// geometry is pushed, then we flush all the "deferred nulls" to that child.
    #[inline]
    pub fn push_null(&mut self) {
        // Iterate through each dimension, then iterate through each child type. If a child exists,
        // push a null to it.
        //
        // Note that we must **also** call `add_*_type` so that the offsets are correct to point
        // the union array to the child.
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let dim_idx = dim.order();
            if !self.points[dim_idx].is_empty() {
                self.add_point_type(dim);
                self.points[dim_idx].push_null();
                return;
            }
            if !self.line_strings[dim_idx].is_empty() {
                self.add_line_string_type(dim);
                self.line_strings[dim_idx].push_null();
                return;
            }
            if !self.polygons[dim_idx].is_empty() {
                self.add_polygon_type(dim);
                self.polygons[dim_idx].push_null();
                return;
            }
            if !self.mpoints[dim_idx].is_empty() {
                self.add_multi_point_type(dim);
                self.mpoints[dim_idx].push_null();
                return;
            }
            if !self.mline_strings[dim_idx].is_empty() {
                self.add_multi_line_string_type(dim);
                self.mline_strings[dim_idx].push_null();
                return;
            }
            if !self.mpolygons[dim_idx].is_empty() {
                self.add_multi_polygon_type(dim);
                self.mpolygons[dim_idx].push_null();
                return;
            }
            if !self.gcs[dim_idx].is_empty() {
                self.add_geometry_collection_type(dim);
                self.gcs[dim_idx].push_null();
                return;
            }
        }

        self.deferred_nulls += 1;
    }

    /// Flush any deferred nulls to the desired array builder.
    fn flush_deferred_nulls<B: GeoArrowArrayBuilder>(
        deferred_nulls: &mut usize,
        child: &mut B,
        offsets: &mut Vec<i32>,
        types: &mut Vec<i8>,
        type_id: i8,
    ) {
        let offset = child.len().try_into().unwrap();
        // For each null we also have to update the offsets and types
        for _ in 0..*deferred_nulls {
            offsets.push(offset);
            types.push(type_id);
            child.push_null();
        }

        *deferred_nulls = 0;
    }

    /// Extend this builder with the given geometries
    pub fn extend_from_iter(
        &mut self,
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait<T = f64> + 'a)>>,
    ) {
        geoms
            .into_iter()
            .try_for_each(|maybe_geom| self.push_geometry(maybe_geom))
            .unwrap();
    }

    /// Create this builder from a slice of nullable Geometries.
    pub fn from_nullable_geometries(
        geoms: &[Option<impl GeometryTrait<T = f64>>],
        typ: GeometryType,
    ) -> GeoArrowResult<Self> {
        let capacity = GeometryCapacity::from_geometries(geoms.iter().map(|x| x.as_ref()))?;
        let mut array = Self::with_capacity(typ, capacity);
        array.extend_from_iter(geoms.iter().map(|x| x.as_ref()));
        Ok(array)
    }
}

impl<O: OffsetSizeTrait> TryFrom<(GenericWkbArray<O>, GeometryType)> for GeometryBuilder {
    type Error = GeoArrowError;

    fn try_from((value, typ): (GenericWkbArray<O>, GeometryType)) -> GeoArrowResult<Self> {
        let wkb_objects = value
            .iter()
            .map(|x| x.transpose())
            .collect::<GeoArrowResult<Vec<_>>>()?;
        Self::from_nullable_geometries(&wkb_objects, typ)
    }
}

impl GeoArrowArrayBuilder for GeometryBuilder {
    fn len(&self) -> usize {
        self.types.len()
    }

    fn push_null(&mut self) {
        self.push_null();
    }

    fn push_geometry(
        &mut self,
        geometry: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        self.push_geometry(geometry)
    }

    fn finish(self) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.finish())
    }
}

// #[cfg(test)]
// mod test {
//     use geoarrow_schema::CoordType;
//     use wkt::wkt;

//     use super::*;
//     use crate::GeoArrowArray;

//     #[test]
//     fn all_items_null() {
//         // Testing the behavior of deferred nulls when there are no valid geometries.
//         let typ = GeometryType::new(Default::default());
//         let mut builder = GeometryBuilder::new(typ);

//         builder.push_null();
//         builder.push_null();
//         builder.push_null();

//         let array = builder.finish();
//         assert_eq!(array.logical_null_count(), 3);

//         // We expect the nulls to be placed in (canonically) the first child
//         assert_eq!(array.points[0].logical_null_count(), 3);
//     }

//     #[test]
//     fn deferred_nulls() {
//         let coord_type = CoordType::Interleaved;
//         let typ = GeometryType::new(Default::default()).with_coord_type(coord_type);

//         let mut builder = GeometryBuilder::new(typ);
//         builder.push_null();
//         builder.push_null();

//         let linestring_arr = crate::test::linestring::array(coord_type, Dimension::XYZ);
//         let linestring_arr_null_count = linestring_arr.logical_null_count();

//         // Push the geometries from the linestring arr onto the geometry builder
//         for geom in linestring_arr.iter() {
//             builder
//                 .push_geometry(geom.transpose().unwrap().as_ref())
//                 .unwrap();
//         }

//         let geom_arr = builder.finish();

//         // Since there are 2 nulls pushed manually and a third from the LineString arr
//         let total_expected_null_count = 2 + linestring_arr_null_count;
//         assert_eq!(geom_arr.logical_null_count(), total_expected_null_count);

//         // All nulls should be in the XYZ linestring child
//         assert_eq!(
//             geom_arr.line_strings[Dimension::XYZ.order()].logical_null_count(),
//             total_expected_null_count
//         );
//     }

//     #[test]
//     fn later_nulls_after_deferred_nulls_pushed_directly() {
//         let coord_type = CoordType::Interleaved;
//         let typ = GeometryType::new(Default::default()).with_coord_type(coord_type);

//         let mut builder = GeometryBuilder::new(typ);
//         builder.push_null();
//         builder.push_null();

//         let point = wkt! { POINT Z (30. 10. 40.) };
//         builder.push_point(Some(&point)).unwrap();

//         let ls = wkt! { LINESTRING (30. 10., 10. 30., 40. 40.) };
//         builder.push_line_string(Some(&ls)).unwrap();

//         builder.push_null();
//         builder.push_null();

//         let geom_arr = builder.finish();

//         assert_eq!(geom_arr.logical_null_count(), 4);

//         // The first two nulls get added to the point z child because those are deferred and the
//         // point z is the first non-null geometry added.
//         assert_eq!(
//             geom_arr.points[Dimension::XYZ.order()].logical_null_count(),
//             2
//         );

//         // The last two nulls get added to the linestring XY child because the current
//         // implementation looks through all XY arrays then all XYZ then etc looking for the first
//         // non-empty array. Since the linestring XY child is non-empty, the last nulls get pushed
//         // here.
//         assert_eq!(
//             geom_arr.line_strings[Dimension::XY.order()].logical_null_count(),
//             2
//         );
//     }

//     // Test pushing nulls that are added after a valid geometry has been pushed.
//     #[test]
//     fn nulls_no_deferred() {
//         let coord_type = CoordType::Interleaved;
//         let typ = GeometryType::new(Default::default()).with_coord_type(coord_type);

//         let mut builder = GeometryBuilder::new(typ);
//         let point = wkt! { POINT Z (30. 10. 40.) };
//         builder.push_point(Some(&point)).unwrap();
//         builder.push_null();
//         builder.push_null();

//         let geom_arr = builder.finish();
//         assert_eq!(geom_arr.logical_null_count(), 2);
//         // All nulls should be in point XYZ child.
//         assert_eq!(
//             geom_arr.points[Dimension::XYZ.order()].logical_null_count(),
//             2
//         );
//     }
// }
