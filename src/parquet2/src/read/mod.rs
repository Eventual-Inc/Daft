mod column;
mod compression;
mod indexes;
pub mod levels;
mod metadata;
mod page;
#[cfg(feature = "async")]
mod stream;

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

pub use column::*;
pub use compression::{decompress, BasicDecompressor, Decompressor};
pub use metadata::{deserialize_metadata, read_metadata, read_metadata_with_size};
#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use page::{
    get_owned_page_stream_from_column_start, get_page_stream, get_page_stream_from_column_start,
};
pub use page::{IndexedPageReader, PageFilter, PageIterator, PageMetaData, PageReader};

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use stream::read_metadata as read_metadata_async;

use crate::metadata::{ColumnChunkMetaData, RowGroupList, RowGroupMetaData};
use crate::{error::Result, metadata::FileMetaData};

pub use indexes::{read_columns_indexes, read_pages_locations};

/// Filters row group metadata to only those row groups,
/// for which the predicate function returns true
pub fn filter_row_groups(
    metadata: &FileMetaData,
    predicate: &dyn Fn(&RowGroupMetaData, usize) -> bool,
) -> FileMetaData {
    let mut filtered_row_groups = Vec::<RowGroupMetaData>::new();
    let mut num_rows = 0;
    for (i, row_group_metadata) in metadata.row_groups.iter() {
        if predicate(row_group_metadata, *i) {
            filtered_row_groups.push(row_group_metadata.clone());
            num_rows += row_group_metadata.num_rows();
        }
    }
    let filtered_row_groups = RowGroupList::from_iter(filtered_row_groups.into_iter().enumerate());
    metadata.clone_with_row_groups(num_rows, filtered_row_groups)
}

/// Returns a new [`PageReader`] by seeking `reader` to the beginning of `column_chunk`.
pub fn get_page_iterator<R: Read + Seek>(
    column_chunk: &ColumnChunkMetaData,
    mut reader: R,
    pages_filter: Option<PageFilter>,
    scratch: Vec<u8>,
    max_page_size: usize,
) -> Result<PageReader<R>> {
    let pages_filter = pages_filter.unwrap_or_else(|| Arc::new(|_, _| true));

    let (col_start, _) = column_chunk.byte_range();
    reader.seek(SeekFrom::Start(col_start))?;
    Ok(PageReader::new(
        reader,
        column_chunk,
        pages_filter,
        scratch,
        max_page_size,
    ))
}

/// Returns all [`ColumnChunkMetaData`] associated to `field_name`.
/// For non-nested types, this returns an iterator with a single column
pub fn get_field_columns<'a>(
    columns: &'a [ColumnChunkMetaData],
    field_name: &'a str,
) -> impl Iterator<Item = &'a ColumnChunkMetaData> {
    columns
        .iter()
        .filter(move |x| x.descriptor().path_in_schema[0] == field_name)
}
