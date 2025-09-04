use std::collections::VecDeque;

use parquet2::{
    encoding::hybrid_rle::HybridRleDecoder,
    page::{split_buffer, DataPage, DictPage, Page},
    read::levels::get_bit_width,
};

use super::{
    super::Pages,
    utils::{DecodedState, MaybeNext, PageState},
};
use crate::{array::Array, bitmap::MutableBitmap, error::Result};

/// trait describing deserialized repetition and definition levels
pub trait Nested: std::fmt::Debug + Send + Sync {
    fn inner(&mut self) -> (Vec<i64>, Option<MutableBitmap>);

    fn push(&mut self, length: i64, is_valid: bool);

    fn is_nullable(&self) -> bool;

    fn is_repeated(&self) -> bool {
        false
    }

    // Whether the Arrow container requires all items to be filled.
    fn is_required(&self) -> bool;

    /// number of rows
    fn len(&self) -> usize;

    /// number of values associated to the primitive type this nested tracks
    fn num_values(&self) -> usize;
}

#[derive(Debug, Default)]
pub struct NestedPrimitive {
    is_nullable: bool,
    length: usize,
}

impl NestedPrimitive {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            is_nullable,
            length: 0,
        }
    }
}

impl Nested for NestedPrimitive {
    fn inner(&mut self) -> (Vec<i64>, Option<MutableBitmap>) {
        (Default::default(), Default::default())
    }

    fn is_nullable(&self) -> bool {
        self.is_nullable
    }

    fn is_required(&self) -> bool {
        false
    }

    fn push(&mut self, _value: i64, _is_valid: bool) {
        self.length += 1
    }

    fn len(&self) -> usize {
        self.length
    }

    fn num_values(&self) -> usize {
        self.length
    }
}

#[derive(Debug, Default)]
pub struct NestedOptional {
    pub validity: MutableBitmap,
    pub offsets: Vec<i64>,
}

impl Nested for NestedOptional {
    fn inner(&mut self) -> (Vec<i64>, Option<MutableBitmap>) {
        let offsets = std::mem::take(&mut self.offsets);
        let validity = std::mem::take(&mut self.validity);
        (offsets, Some(validity))
    }

    fn is_nullable(&self) -> bool {
        true
    }

    fn is_repeated(&self) -> bool {
        true
    }

    fn is_required(&self) -> bool {
        // it may be for FixedSizeList
        false
    }

    fn push(&mut self, value: i64, is_valid: bool) {
        self.offsets.push(value);
        self.validity.push(is_valid);
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn num_values(&self) -> usize {
        self.offsets.last().copied().unwrap_or(0) as usize
    }
}

impl NestedOptional {
    pub fn with_capacity(capacity: usize) -> Self {
        let offsets = Vec::<i64>::with_capacity(capacity + 1);
        let validity = MutableBitmap::with_capacity(capacity);
        Self { validity, offsets }
    }
}

#[derive(Debug, Default)]
pub struct NestedValid {
    pub offsets: Vec<i64>,
}

impl Nested for NestedValid {
    fn inner(&mut self) -> (Vec<i64>, Option<MutableBitmap>) {
        let offsets = std::mem::take(&mut self.offsets);
        (offsets, None)
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn is_repeated(&self) -> bool {
        true
    }

    fn is_required(&self) -> bool {
        // it may be for FixedSizeList
        false
    }

    fn push(&mut self, value: i64, _is_valid: bool) {
        self.offsets.push(value);
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn num_values(&self) -> usize {
        self.offsets.last().copied().unwrap_or(0) as usize
    }
}

impl NestedValid {
    pub fn with_capacity(capacity: usize) -> Self {
        let offsets = Vec::<i64>::with_capacity(capacity + 1);
        Self { offsets }
    }
}

#[derive(Debug, Default)]
pub struct NestedStructValid {
    length: usize,
}

impl NestedStructValid {
    pub fn new() -> Self {
        Self { length: 0 }
    }
}

impl Nested for NestedStructValid {
    fn inner(&mut self) -> (Vec<i64>, Option<MutableBitmap>) {
        (Default::default(), None)
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn is_required(&self) -> bool {
        true
    }

    fn push(&mut self, _value: i64, _is_valid: bool) {
        self.length += 1;
    }

    fn len(&self) -> usize {
        self.length
    }

    fn num_values(&self) -> usize {
        self.length
    }
}

#[derive(Debug, Default)]
pub struct NestedStruct {
    validity: MutableBitmap,
}

impl NestedStruct {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            validity: MutableBitmap::with_capacity(capacity),
        }
    }
}

impl Nested for NestedStruct {
    fn inner(&mut self) -> (Vec<i64>, Option<MutableBitmap>) {
        (Default::default(), Some(std::mem::take(&mut self.validity)))
    }

    fn is_nullable(&self) -> bool {
        true
    }

    fn is_required(&self) -> bool {
        true
    }

    fn push(&mut self, _value: i64, is_valid: bool) {
        self.validity.push(is_valid)
    }

    fn len(&self) -> usize {
        self.validity.len()
    }

    fn num_values(&self) -> usize {
        self.validity.len()
    }
}

/// A decoder that knows how to map `State` -> Array
pub(super) trait NestedDecoder<'a> {
    type State: PageState<'a>;
    type Dictionary;
    type DecodedState: DecodedState;

    fn build_state(
        &self,
        page: &'a DataPage,
        dict: Option<&'a Self::Dictionary>,
        is_parent_nullable: bool,
    ) -> Result<Self::State>;

    /// Initializes a new state
    fn with_capacity(&self, capacity: usize) -> Self::DecodedState;

    fn push_valid(&self, state: &mut Self::State, decoded: &mut Self::DecodedState) -> Result<()>;
    fn push_null(&self, decoded: &mut Self::DecodedState);

    fn deserialize_dict(&self, page: &DictPage) -> Self::Dictionary;
}

/// The initial info of nested data types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitNested {
    /// Primitive data types
    Primitive(bool),
    /// List data types
    List(bool),
    /// Struct data types
    Struct(bool),
}

/// Initialize [`NestedState`] from `&[InitNested]`.
pub fn init_nested(init: &[InitNested], capacity: usize) -> NestedState {
    let container = init
        .iter()
        .map(|init| match init {
            InitNested::Primitive(is_nullable) => {
                Box::new(NestedPrimitive::new(*is_nullable)) as Box<dyn Nested>
            }
            InitNested::List(is_nullable) => {
                if *is_nullable {
                    Box::new(NestedOptional::with_capacity(capacity)) as Box<dyn Nested>
                } else {
                    Box::new(NestedValid::with_capacity(capacity)) as Box<dyn Nested>
                }
            }
            InitNested::Struct(is_nullable) => {
                if *is_nullable {
                    Box::new(NestedStruct::with_capacity(capacity)) as Box<dyn Nested>
                } else {
                    Box::new(NestedStructValid::new()) as Box<dyn Nested>
                }
            }
        })
        .collect();
    NestedState::new(container)
}

pub struct NestedPage<'a> {
    iter: std::iter::Peekable<std::iter::Zip<HybridRleDecoder<'a>, HybridRleDecoder<'a>>>,
}

impl<'a> NestedPage<'a> {
    pub fn try_new(page: &'a DataPage) -> Result<Self> {
        let (rep_levels, def_levels, _) = split_buffer(page)?;

        let max_rep_level = page.descriptor.max_rep_level;
        let max_def_level = page.descriptor.max_def_level;

        let reps =
            HybridRleDecoder::try_new(rep_levels, get_bit_width(max_rep_level), page.num_values())?;
        let defs =
            HybridRleDecoder::try_new(def_levels, get_bit_width(max_def_level), page.num_values())?;

        let iter = reps.zip(defs).peekable();

        Ok(Self { iter })
    }

    // number of values (!= number of rows)
    pub fn len(&self) -> usize {
        self.iter.size_hint().0
    }
}

/// The state of nested data types.
#[derive(Debug)]
pub struct NestedState {
    /// The nesteds composing `NestedState`.
    pub nested: Vec<Box<dyn Nested>>,
}

impl NestedState {
    /// Creates a new [`NestedState`].
    pub fn new(nested: Vec<Box<dyn Nested>>) -> Self {
        Self { nested }
    }

    /// The number of rows in this state
    pub fn len(&self) -> usize {
        // outermost is the number of rows
        self.nested[0].len()
    }
}

/// Extends `items` by consuming `page`, first trying to complete the last `item`
/// and extending it if more are needed
#[allow(clippy::too_many_arguments)]
pub(super) fn extend<'a, D: NestedDecoder<'a>>(
    page: &'a DataPage,
    init: &[InitNested],
    items: &mut VecDeque<(NestedState, D::DecodedState)>,
    dict: Option<&'a D::Dictionary>,
    rows_remaining: &mut usize,
    values_remaining: &mut usize,
    decoder: &D,
    chunk_size: Option<usize>,
    is_parent_nullable: bool,
) -> Result<()> {
    let mut values_page = decoder.build_state(page, dict, is_parent_nullable)?;
    let mut page = NestedPage::try_new(page)?;

    let capacity = chunk_size.unwrap_or(0);
    // chunk_size = None, remaining = 44 => chunk_size = 44
    let chunk_size = chunk_size.unwrap_or(usize::MAX);

    let (mut nested, mut decoded) = if let Some((nested, decoded)) = items.pop_back() {
        (nested, decoded)
    } else {
        // there is no state => initialize it
        (init_nested(init, capacity), decoder.with_capacity(0))
    };
    let existing = nested.len();

    let additional = (chunk_size - existing).min(*rows_remaining);

    // extend the current state
    extend_offsets2(
        &mut page,
        &mut values_page,
        &mut nested.nested,
        *rows_remaining,
        values_remaining,
        &mut decoded,
        decoder,
        additional,
    )?;
    assert!(
        *rows_remaining >= nested.len() - existing,
        "Rows remaining ({}) is less than the number of new rows seen ({}). Please file an issue.",
        *rows_remaining,
        nested.len() - existing,
    );
    assert!(
        nested.len() <= chunk_size,
        "Number of rows in the chunk ({}) exceeds the chunk size ({}). Please file an issue.",
        nested.len(),
        chunk_size,
    );
    *rows_remaining -= nested.len() - existing;
    items.push_back((nested, decoded));

    // If we've filled the current chunk, but there are rows remaining in the current page, start
    // filling up new chunks.
    while page.len() > 0 && (*rows_remaining > 0 || *values_remaining > 0) {
        let additional = chunk_size.min(*rows_remaining);

        let mut nested = init_nested(init, additional);
        let mut decoded = decoder.with_capacity(0);
        extend_offsets2(
            &mut page,
            &mut values_page,
            &mut nested.nested,
            *rows_remaining,
            values_remaining,
            &mut decoded,
            decoder,
            additional,
        )?;
        assert!(
            *rows_remaining >= nested.len(),
            "Rows remaining ({}) is less than the number of new rows seen ({}). Please file an issue.",
            *rows_remaining,
            nested.len(),
        );
        assert!(
            nested.len() <= chunk_size,
            "Number of rows in the chunk ({}) exceeds the chunk size ({}). Please file an issue.",
            nested.len(),
            chunk_size,
        );
        *rows_remaining -= nested.len();
        items.push_back((nested, decoded));
    }
    Ok(())
}

/// Helper function that fills a chunk with nested values decoded from the current page. At most
/// `additional` values will be added to the current chunk.
///
///
/// # Arguments
///
/// * `page`             - The repetition and definition levels for the current Parquet page.
/// * `values_state`     - The state of our nested values.
/// * `nested`           - The state of our nested data types.
/// * `rows_remaining`   - The global number of top-level rows that remain in the current row group.
/// * `values_remaining` - The global number of leaf values that remain in the current row group.
/// * `decoded`          - The state of our decoded values.
/// * `decoder`          - The decoder for the leaf-level type.
/// * `additional`       - The number of top-level rows to read for the current chunk. This is the
///   min of `chunk size - number of rows existing in the current chunk` and `rows_remaining`.
#[allow(clippy::too_many_arguments)]
fn extend_offsets2<'a, D: NestedDecoder<'a>>(
    page: &mut NestedPage<'a>,
    values_state: &mut D::State,
    nested: &mut [Box<dyn Nested>],
    rows_remaining: usize,
    values_remaining: &mut usize,
    decoded: &mut D::DecodedState,
    decoder: &D,
    additional: usize,
) -> Result<()> {
    // Check that we have at least one value (which can be null) per row.
    assert!(
        *values_remaining >= rows_remaining,
        "Values remaining({}) is lower than the number of rows remaining ({}). Please file an issue.",
        *values_remaining,
        rows_remaining,
    );
    let max_depth = nested.len();

    let mut cum_sum = vec![0u32; max_depth + 1];
    for (i, nest) in nested.iter().enumerate() {
        let delta = nest.is_nullable() as u32 + nest.is_repeated() as u32;
        cum_sum[i + 1] = cum_sum[i] + delta;
    }

    let mut cum_rep = vec![0u32; max_depth + 1];
    for (i, nest) in nested.iter().enumerate() {
        let delta = nest.is_repeated() as u32;
        cum_rep[i + 1] = cum_rep[i] + delta;
    }

    let mut rows_seen = 0;
    loop {
        let next_rep = page.iter.peek().map(|x| x.0.as_ref()).transpose().unwrap();
        match next_rep {
            Some(next_rep) => {
                // A repetition level of 0 indicates that we're reading a new record. For more
                // details, see:
                // Melnik, Sergey et al. “Dremel.” Proceedings of the VLDB Endowment 13 (2020): 3461 - 3472.
                if *next_rep == 0 {
                    // A row might have values that overflow across multiple data pages. If the
                    // overflowing row is the last row in our result (either because it is the last
                    // row in the column, or in the limit(), or in the show()), then we might have
                    // continued reading data pages despite `rows_remaining <= rows_seen`. We only
                    // know that we've read all values to read when either `values_remaining` is 0,
                    // or when `rows_remaining <= rows_seen` and we see a new record. In the latter
                    // case, the remaining values lie outside of the rows we're retrieving, so we
                    // zero out `values_remaining`.
                    if rows_seen >= rows_remaining {
                        *values_remaining = 0;
                        break;
                    }
                    if rows_seen >= additional {
                        break;
                    }
                    rows_seen += 1;
                }
            }
            None => break,
        }
        let (rep, def) = page.iter.next().unwrap();
        *values_remaining -= 1;
        let rep = rep?;
        let def = def?;

        let mut is_required = false;
        for depth in 0..max_depth {
            let right_level = rep <= cum_rep[depth] && def >= cum_sum[depth];
            if is_required || right_level {
                let length = nested
                    .get(depth + 1)
                    .map(|x| x.len() as i64)
                    // the last depth is the leaf, which is always increased by 1
                    .unwrap_or(1);

                let nest = &mut nested[depth];

                let is_valid = nest.is_nullable() && def > cum_sum[depth];
                nest.push(length, is_valid);
                is_required = nest.is_required() && !is_valid;

                if depth == max_depth - 1 {
                    // the leaf / primitive
                    let is_valid = (def != cum_sum[depth]) || !nest.is_nullable();
                    if right_level && is_valid {
                        decoder.push_valid(values_state, decoded)?;
                    } else {
                        decoder.push_null(decoded);
                    }
                }
            }
        }
    }
    assert!(
        rows_seen <= rows_remaining,
        "Rows seen ({}) is greater than the number of rows remaining ({}). Please file an issue.",
        rows_seen,
        rows_remaining,
    );
    assert!(
        rows_seen <= additional,
        "Rows seen ({}) is greater than the additional number of rows to read in the current data page ({}). Please file an issue.",
        rows_seen,
        additional,
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[inline]
pub(super) fn next<'a, I, D>(
    iter: &'a mut I,
    items: &mut VecDeque<(NestedState, D::DecodedState)>,
    dict: &'a mut Option<D::Dictionary>,
    rows_remaining: &mut usize,
    values_remaining: &mut usize,
    init: &[InitNested],
    chunk_size: Option<usize>,
    is_parent_nullable: bool,
    decoder: &D,
) -> MaybeNext<Result<(NestedState, D::DecodedState)>>
where
    I: Pages,
    D: NestedDecoder<'a>,
{
    // front[a1, a2, a3, ...]back
    if items.len() > 1 {
        return MaybeNext::Some(Ok(items.pop_front().unwrap()));
    }
    if (items.len() == 1)
        && items.front().unwrap().0.len() == chunk_size.unwrap_or(usize::MAX)
        && *values_remaining == 0
    {
        return MaybeNext::Some(Ok(items.pop_front().unwrap()));
    }
    if *rows_remaining == 0 && *values_remaining == 0 {
        return match items.pop_front() {
            Some(decoded) => MaybeNext::Some(Ok(decoded)),
            None => MaybeNext::None,
        };
    }
    match iter.next() {
        Err(e) => MaybeNext::Some(Err(e.into())),
        Ok(None) => {
            if let Some(decoded) = items.pop_front() {
                MaybeNext::Some(Ok(decoded))
            } else {
                MaybeNext::None
            }
        }
        Ok(Some(page)) => {
            let page = match page {
                Page::Data(page) => page,
                Page::Dict(dict_page) => {
                    *dict = Some(decoder.deserialize_dict(dict_page));
                    return MaybeNext::More;
                }
            };

            // there is a new page => consume the page from the start
            let error = extend(
                page,
                init,
                items,
                dict.as_ref(),
                rows_remaining,
                values_remaining,
                decoder,
                chunk_size,
                is_parent_nullable,
            );
            match error {
                Ok(_) => {}
                Err(e) => return MaybeNext::Some(Err(e)),
            };

            if (items.len() == 1)
                && (items.front().unwrap().0.len() < chunk_size.unwrap_or(usize::MAX)
                    || *values_remaining > 0)
            {
                MaybeNext::More
            } else {
                MaybeNext::Some(Ok(items.pop_front().unwrap()))
            }
        }
    }
}

/// Type def for a sharable, boxed dyn [`Iterator`] of NestedStates and arrays
pub type NestedArrayIter<'a> =
    Box<dyn Iterator<Item = Result<(NestedState, Box<dyn Array>)>> + Send + Sync + 'a>;
