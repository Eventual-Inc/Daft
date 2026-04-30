use daft_common_error::DaftResult;

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
    id_map: [usize; 128],
}

impl<'a> UnionGrowable<'a> {
    pub fn new(
        name: &str,
        dtype: &DataType,
        children: Vec<&'a UnionArray>,
        use_validity: bool,
        capacity: usize,
    ) -> Self {
        match dtype {
            DataType::Union(fields, type_ids, mode) => {
                let has_offsets = mode.is_dense();

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
                            use_validity,
                            capacity,
                        )
                    })
                    .collect::<Vec<_>>();

                let mut id_map = [0; 128];
                for (i, type_id) in type_ids.iter().enumerate() {
                    assert!(*type_id >= 0, "Union Type Ids must be >= 0");
                    id_map[*type_id as usize] = i;
                }

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
                    id_map,
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
                let type_idx = self.id_map[type_ as usize];
                let field = &mut self.children_growables[type_idx];

                x.push(field.len() as i32);
                field.extend(index, offset as usize, 1);
            }
        } else {
            self.children_growables
                .iter_mut()
                .for_each(|field| field.extend(index, start, len));
        }
    }

    fn add_nulls(&mut self, additional: usize) {
        let DataType::Union(_, type_ids, _) = &self.dtype else {
            panic!("UnionGrowable should only be used with UnionArray");
        };
        let null_id = type_ids[0];
        for _ in 0..additional {
            self.ids.push(null_id);
            if let Some(offsets) = self.offsets.as_mut() {
                // Dense: point to the null value we're about to append.
                let null_offset = self.children_growables[0].len() as i32;
                offsets.push(null_offset);
                self.children_growables[0].add_nulls(1);
                // Other children don't grow for this row in a dense union.
            } else {
                // Sparse: every child must grow by 1 to stay aligned.
                for child in &mut self.children_growables {
                    child.add_nulls(1);
                }
            }
        }
    }

    fn build(&mut self) -> DaftResult<Series> {
        let build_children = self
            .children_growables
            .iter_mut()
            .map(|growable| growable.build())
            .collect::<DaftResult<Vec<_>>>()?;

        let ids = std::mem::take(&mut self.ids);
        let offsets = std::mem::take(&mut self.offsets);

        Ok(UnionArray::new(
            Field::new(self.name.clone(), self.dtype.clone()),
            ids,
            build_children,
            offsets,
        )
        .into_series())
    }

    fn len(&self) -> usize {
        self.ids.len()
    }
}
