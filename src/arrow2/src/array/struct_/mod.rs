use std::ops::DerefMut;
use crate::{
    bitmap::Bitmap,
    datatypes::{DataType, Field},
    error::Error,
};

use super::{new_empty_array, new_null_array, Array};

#[cfg(feature = "arrow")]
mod data;
mod ffi;
pub(super) mod fmt;
mod iterator;
mod mutable;
pub use mutable::*;

/// A [`StructArray`] is a nested [`Array`] with an optional validity representing
/// multiple [`Array`] with the same number of rows.
/// # Example
/// ```
/// use arrow2::array::*;
/// use arrow2::datatypes::*;
/// let boolean = BooleanArray::from_slice(&[false, false, true, true]).boxed();
/// let int = Int32Array::from_slice(&[42, 28, 19, 31]).boxed();
///
/// let fields = vec![
///     Field::new("b", DataType::Boolean, false),
///     Field::new("c", DataType::Int32, false),
/// ];
///
/// let array = StructArray::new(DataType::Struct(fields), vec![boolean, int], None);
/// ```
#[derive(Clone)]
pub struct StructArray {
    data_type: DataType,
    values: Vec<Box<dyn Array>>,
    validity: Option<Bitmap>,
}

impl StructArray {
    /// Returns a new [`StructArray`].
    /// # Errors
    /// This function errors iff:
    /// * `data_type`'s physical type is not [`crate::datatypes::PhysicalType::Struct`].
    /// * the children of `data_type` are empty
    /// * the values's len is different from children's length
    /// * any of the values's data type is different from its corresponding children' data type
    /// * any element of values has a different length than the first element
    /// * the validity's length is not equal to the length of the first element
    pub fn try_new(
        data_type: DataType,
        values: Vec<Box<dyn Array>>,
        validity: Option<Bitmap>,
    ) -> Result<Self, Error> {
        let fields = Self::try_get_fields(&data_type)?;
        if fields.is_empty() {
            return Err(Error::oos("A StructArray must contain at least one field"));
        }
        if fields.len() != values.len() {
            return Err(Error::oos(
                "A StructArray must have a number of fields in its DataType equal to the number of child values",
            ));
        }

        fields
            .iter().map(|a| &a.data_type)
            .zip(values.iter().map(|a| a.data_type()))
            .enumerate()
            .try_for_each(|(index, (data_type, child))| {
                if data_type != child {
                    Err(Error::oos(format!(
                        "The children DataTypes of a StructArray must equal the children data types.
                         However, the field {index} has data type {data_type:?} but the value has data type {child:?}"
                    )))
                } else {
                    Ok(())
                }
            })?;

        let len = values[0].len();
        values
            .iter()
            .map(|a| a.len())
            .enumerate()
            .try_for_each(|(index, a_len)| {
                if a_len != len {
                    Err(Error::oos(format!(
                        "The children must have an equal number of values.
                         However, the values at index {index} have a length of {a_len}, which is different from values at index 0, {len}."
                    )))
                } else {
                    Ok(())
                }
            })?;

        if validity
            .as_ref()
            .map_or(false, |validity| validity.len() != len)
        {
            return Err(Error::oos(
                "The validity length of a StructArray must match its number of elements",
            ));
        }

        Ok(Self {
            data_type,
            values,
            validity,
        })
    }

    /// Returns a new [`StructArray`]
    /// # Panics
    /// This function panics iff:
    /// * `data_type`'s physical type is not [`crate::datatypes::PhysicalType::Struct`].
    /// * the children of `data_type` are empty
    /// * the values's len is different from children's length
    /// * any of the values's data type is different from its corresponding children' data type
    /// * any element of values has a different length than the first element
    /// * the validity's length is not equal to the length of the first element
    pub fn new(data_type: DataType, values: Vec<Box<dyn Array>>, validity: Option<Bitmap>) -> Self {
        Self::try_new(data_type, values, validity).unwrap()
    }

    /// Creates an empty [`StructArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        if let DataType::Struct(fields) = &data_type.to_logical_type() {
            let values = fields
                .iter()
                .map(|field| new_empty_array(field.data_type().clone()))
                .collect();
            Self::new(data_type, values, None)
        } else {
            panic!("StructArray must be initialized with DataType::Struct");
        }
    }

    /// Creates a null [`StructArray`] of length `length`.
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        if let DataType::Struct(fields) = &data_type {
            let values = fields
                .iter()
                .map(|field| new_null_array(field.data_type().clone(), length))
                .collect();
            Self::new(data_type, values, Some(Bitmap::new_zeroed(length)))
        } else {
            panic!("StructArray must be initialized with DataType::Struct");
        }
    }
}

// must use
impl StructArray {
    /// Deconstructs the [`StructArray`] into its individual components.
    #[must_use]
    pub fn into_data(self) -> (Vec<Field>, Vec<Box<dyn Array>>, Option<Bitmap>) {
        let Self {
            data_type,
            values,
            validity,
        } = self;
        let fields = if let DataType::Struct(fields) = data_type {
            fields
        } else {
            unreachable!()
        };
        (fields, values, validity)
    }

    /// Slices this [`StructArray`].
    /// # Panics
    /// * `offset + length` must be smaller than `self.len()`.
    /// # Implementation
    /// This operation is `O(F)` where `F` is the number of fields.
    pub fn slice(&mut self, offset: usize, length: usize) {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Slices this [`StructArray`].
    /// # Implementation
    /// This operation is `O(F)` where `F` is the number of fields.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    pub unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        self.validity.as_mut().and_then(|bitmap| {
            bitmap.slice_unchecked(offset, length);
            (bitmap.unset_bits() > 0).then(|| bitmap)
        });
        self.values
            .iter_mut()
            .for_each(|x| x.slice_unchecked(offset, length));
    }

    impl_sliced!();

    impl_to!();

    impl_mut_validity!();

    impl_into_array!();
}

// Accessors
impl StructArray {
    #[inline]
    fn len(&self) -> usize {
        self.values[0].len()
    }

    /// The optional validity.
    #[inline]
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Returns the values of this [`StructArray`].
    pub fn values(&self) -> &[Box<dyn Array>] {
        &self.values
    }

    /// Returns the fields of this [`StructArray`].
    pub fn fields(&self) -> &[Field] {
        Self::get_fields(&self.data_type)
    }
}

impl StructArray {
    /// Returns the fields the `DataType::Struct`.
    pub(crate) fn try_get_fields(data_type: &DataType) -> Result<&[Field], Error> {
        match data_type.to_logical_type() {
            DataType::Struct(fields) => Ok(fields),
            _ => Err(Error::oos(
                "Struct array must be created with a DataType whose physical type is Struct",
            )),
        }
    }

    /// Returns the fields the `DataType::Struct`.
    pub fn get_fields(data_type: &DataType) -> &[Field] {
        Self::try_get_fields(data_type).unwrap()
    }
}

impl Array for StructArray {
    impl_common_array!();

    fn direct_children<'a>(&'a mut self) -> Box<dyn Iterator<Item=&'a mut dyn Array> + 'a> {
        let iter = self.values
            .iter_mut()
            .map(|x| x.deref_mut());

        Box::new(iter)
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    #[inline]
    fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn Array> {
        Box::new(self.clone().with_validity(validity))
    }
}
