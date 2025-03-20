use super::{new_empty_array, specification::try_check_offsets_bounds, Array, ListArray};
use crate::{
    bitmap::Bitmap,
    datatypes::{DataType, Field},
    error::Error,
    offset::OffsetsBuffer,
};

#[cfg(feature = "arrow")]
mod data;
mod ffi;
pub(super) mod fmt;
mod iterator;
#[allow(unused)]
pub use iterator::*;

/// An array representing a (key, value), both of arbitrary logical types.
#[derive(Clone)]
pub struct MapArray {
    data_type: DataType,
    // invariant: field.len() == offsets.len()
    offsets: OffsetsBuffer<i32>,
    field: Box<dyn Array>,
    // invariant: offsets.len() - 1 == Bitmap::len()
    validity: Option<Bitmap>,
}

impl MapArray {
    /// Returns a new [`MapArray`].
    /// # Errors
    /// This function errors iff:
    /// * The last offset is not equal to the field' length
    /// * The `data_type`'s physical type is not [`crate::datatypes::PhysicalType::Map`]
    /// * The fields' `data_type` is not equal to the inner field of `data_type`
    /// * The validity is not `None` and its length is different from `offsets.len() - 1`.
    pub fn try_new(
        data_type: DataType,
        offsets: OffsetsBuffer<i32>,
        field: Box<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Result<Self, Error> {
        try_check_offsets_bounds(&offsets, field.len())?;

        let inner_field = Self::try_get_field(&data_type)?;

        let inner_data_type = inner_field.data_type();
        let DataType::Struct(inner) = inner_data_type else {
            return Err(Error::InvalidArgumentError(
                format!("MapArray expects `DataType::Struct` as its inner logical type, but found {inner_data_type:?}"),
            ));
        };

        let inner_len = inner.len();
        if inner_len != 2 {
            let msg = format!(
                "MapArray's inner `Struct` must have 2 fields (keys and maps), but found {} fields",
                inner_len
            );
            return Err(Error::InvalidArgumentError(msg));
        }

        let field_data_type = field.data_type();
        if field_data_type != inner_field.data_type() {
            return Err(Error::InvalidArgumentError(
                format!("MapArray expects `field.data_type` to match its inner DataType, but found \n{field_data_type:?}\nvs\n\n\n{inner_field:?}"),
            ));
        }

        if validity
            .as_ref()
            .map_or(false, |validity| validity.len() != offsets.len_proxy())
        {
            return Err(Error::oos(
                "validity mask length must match the number of values",
            ));
        }

        Ok(Self {
            data_type,
            field,
            offsets,
            validity,
        })
    }

    /// Creates a new [`MapArray`].
    /// # Panics
    /// * The last offset is not equal to the field' length.
    /// * The `data_type`'s physical type is not [`crate::datatypes::PhysicalType::Map`],
    /// * The validity is not `None` and its length is different from `offsets.len() - 1`.
    pub fn new(
        data_type: DataType,
        offsets: OffsetsBuffer<i32>,
        field: Box<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Self {
        Self::try_new(data_type, offsets, field, validity).unwrap()
    }

    /// Returns a new null [`MapArray`] of `length`.
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        let field = new_empty_array(Self::get_field(&data_type).data_type().clone());
        Self::new(
            data_type,
            vec![0i32; 1 + length].try_into().unwrap(),
            field,
            Some(Bitmap::new_zeroed(length)),
        )
    }

    /// Returns a new empty [`MapArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        let field = new_empty_array(Self::get_field(&data_type).data_type().clone());
        Self::new(data_type, OffsetsBuffer::default(), field, None)
    }
}

impl MapArray {
    /// Returns a slice of this [`MapArray`].
    /// # Panics
    /// panics iff `offset + length >= self.len()`
    pub fn slice(&mut self, offset: usize, length: usize) {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Returns a slice of this [`MapArray`].
    /// # Safety
    /// The caller must ensure that `offset + length < self.len()`.
    #[inline]
    pub unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        self.validity.as_mut().and_then(|bitmap| {
            bitmap.slice_unchecked(offset, length);
            (bitmap.unset_bits() > 0).then(|| bitmap)
        });
        self.offsets.slice_unchecked(offset, length + 1);
    }

    impl_sliced!();
    impl_to!();
    impl_mut_validity!();
    impl_into_array!();

    pub(crate) fn try_get_field(data_type: &DataType) -> Result<&Field, Error> {
        if let DataType::Map(field, _) = data_type.to_logical_type() {
            Ok(field.as_ref())
        } else {
            Err(Error::oos(
                "The data_type's logical type must be DataType::Map",
            ))
        }
    }

    pub(crate) fn get_field(data_type: &DataType) -> &Field {
        Self::try_get_field(data_type).unwrap()
    }
}

// Accessors
impl MapArray {
    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len_proxy()
    }

    /// returns the offsets
    #[inline]
    pub fn offsets(&self) -> &OffsetsBuffer<i32> {
        &self.offsets
    }

    /// Returns the field (guaranteed to be a `Struct`)
    #[inline]
    pub fn field(&self) -> &Box<dyn Array> {
        &self.field
    }

    /// Returns the element at index `i`.
    #[inline]
    pub fn value(&self, i: usize) -> Box<dyn Array> {
        assert!(i < self.len());
        unsafe { self.value_unchecked(i) }
    }

    /// Returns the element at index `i`.
    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> Box<dyn Array> {
        // soundness: the invariant of the function
        let (start, end) = self.offsets.start_end_unchecked(i);
        let length = end - start;

        // soundness: the invariant of the struct
        self.field.sliced_unchecked(start, length)
    }
}

impl Array for MapArray {
    impl_common_array!();

    fn convert_logical_type(&self, target_data_type: DataType) -> Box<dyn Array> {
        let is_target_map = matches!(target_data_type, DataType::Map { .. });

        let DataType::Map(current_field, _) = self.data_type() else {
            unreachable!(
                "Expected MapArray to have Map data type, but found {:?}",
                self.data_type()
            );
        };

        if is_target_map {
            // For Map-to-Map conversions, we can clone
            // (same top level representation we are still a Map). and then change the subtype in
            // place.
            let mut converted_array = self.to_boxed();
            converted_array.change_type(target_data_type);
            return converted_array;
        }

        // Target type is a LargeList, so we need to convert to a ListArray before converting
        let DataType::LargeList(target_field) = &target_data_type else {
            panic!("MapArray can only be converted to Map or LargeList, but target type is {target_data_type:?}");
        };


        let current_physical_type = current_field.data_type.to_physical_type();
        let target_physical_type = target_field.data_type.to_physical_type();

        if current_physical_type != target_physical_type {
            panic!(
                "Inner physical types must be equal for conversion. Current: {:?}, Target: {:?}",
                current_physical_type, target_physical_type
            );
        }

        let mut converted_field = self.field.clone();
        converted_field.change_type(target_field.data_type.clone());

        let original_offsets = self.offsets().clone();
        let converted_offsets = unsafe { original_offsets.map_unchecked(|offset| offset as i64) };

        let converted_list = ListArray::new(
            target_data_type,
            converted_offsets,
            converted_field,
            self.validity.clone(),
        );

        Box::new(converted_list)
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    #[inline]
    fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn Array> {
        Box::new(self.clone().with_validity(validity))
    }
}
