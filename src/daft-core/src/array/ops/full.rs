use crate::{
    array::growable::{Growable, GrowableArray},
    datatypes::{DaftArrayType, DataType},
};

pub trait FullNull {
    fn full_null(name: &str, dtype: &DataType, length: usize) -> Self;
    fn empty(name: &str, dtype: &DataType) -> Self;
}

impl<Arr> FullNull for Arr
where
    Arr: GrowableArray + DaftArrayType,
{
    fn full_null(name: &str, dtype: &DataType, length: usize) -> Self {
        let mut growable =
            Arr::make_growable(name.to_string(), dtype, Vec::<&Arr>::new(), true, length);
        growable.add_nulls(length);
        growable.build().unwrap().downcast::<Arr>().unwrap().clone()
    }

    fn empty(name: &str, dtype: &DataType) -> Self {
        let mut growable = Arr::make_growable(name.to_string(), dtype, Vec::<&Arr>::new(), true, 0);
        growable.build().unwrap().downcast::<Arr>().unwrap().clone()
    }
}
