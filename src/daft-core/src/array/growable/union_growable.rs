use arrow::buffer::ScalarBuffer;
use common_error::DaftResult;

use super::Growable;
use crate::{
    array::{UnionArray, growable::make_growable},
    datatypes::{DataType, Field},
    series::{IntoSeries, Series},
};

pub struct UnionGrowable<'a> {
    name: String,
    dtype: DataType,
    ids: Vec<i8>,
    arrays: Vec<&'a UnionArray>,
    children_growables: Vec<Box<dyn Growable + 'a>>,
    offsets: Option<Vec<i32>>,
}

impl<'a> UnionGrowable<'a> {
    pub fn new(
        name: &str,
        dtype: &DataType,
        children: Vec<&'a UnionArray>,
        _use_validity: bool,
        capacity: usize,
    ) -> Self {
        match dtype {
            DataType::Union(fields, _, _) => {
                let has_offsets = children[0].offsets().is_some();

                let children_growables: Vec<Box<dyn Growable>> = fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        make_growable(
                            f.name.as_ref(),
                            &f.dtype,
                            children
                                .iter()
                                .map(|a| a.children.get(i).unwrap())
                                .collect::<Vec<_>>(),
                            false,
                            capacity,
                        )
                    })
                    .collect::<Vec<_>>();

                Self {
                    name: name.to_string(),
                    dtype: dtype.clone(),
                    ids: Vec::with_capacity(capacity),
                    arrays: children,
                    children_growables,
                    offsets: if has_offsets {
                        Some(Vec::with_capacity(capacity))
                    } else {
                        None
                    },
                }
            }
            _ => panic!("Cannot create UnionGrowable from dtype: {}", dtype),
        }
    }
}

impl Growable for UnionGrowable<'_> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        let array = self.arrays[index];

        let ids = &array.ids()[start..start + len];
        self.ids.extend(ids);

        if let Some(x) = self.offsets.as_mut() {
            let offsets = &array.offsets().clone().unwrap()[start..start + len];

            for (&type_, &offset) in ids.iter().zip(offsets.iter()) {
                let field = &mut self.children_growables[type_ as usize];

                x.push(field.len() as i32);
                field.extend(index, offset as usize, 1);
            }
        } else {
            self.children_growables
                .iter_mut()
                .for_each(|field| field.extend(index, start, len));
        }
    }

    fn add_nulls(&mut self, _additional: usize) {}

    fn build(&mut self) -> DaftResult<Series> {
        let build_children = self
            .children_growables
            .iter_mut()
            .map(|growable| growable.build())
            .collect::<DaftResult<Vec<_>>>()?;

        let ids = std::mem::take(&mut self.ids);
        let offsets = std::mem::take(&mut self.offsets);

        let ids_buffer = ScalarBuffer::from(ids);
        let offsets_buffer = if let Some(offsets) = offsets {
            let offsets_buffer = ScalarBuffer::from(offsets);
            Some(offsets_buffer)
        } else {
            None
        };

        Ok(UnionArray::new(
            Field::new(self.name.clone(), self.dtype.clone()),
            ids_buffer,
            build_children,
            offsets_buffer,
        )
        .into_series())
    }

    fn len(&self) -> usize {
        self.ids.len()
    }
}
