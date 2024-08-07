use std::{
    cmp::min,
    io::{self, BufReader, Read, Seek, SeekFrom},
};

use parquet_format_safe::{thrift::{self, protocol::{field_id, TCompactInputProtocol, TInputProtocol, TType}}, ColumnOrder, EncryptionAlgorithm, KeyValue, RowGroup, SchemaElement};
use parquet_format_safe::FileMetaData as TFileMetaData;

use super::super::{
    metadata::FileMetaData, DEFAULT_FOOTER_READ_SIZE, FOOTER_SIZE, HEADER_SIZE, PARQUET_MAGIC,
};

use crate::error::{Error, Result};

pub(super) fn metadata_len(buffer: &[u8], len: usize) -> i32 {
    i32::from_le_bytes(buffer[len - 8..len - 4].try_into().unwrap())
}

// see (unstable) Seek::stream_len
fn stream_len(seek: &mut impl Seek) -> std::result::Result<u64, std::io::Error> {
    let old_pos = seek.stream_position()?;
    let len = seek.seek(SeekFrom::End(0))?;

    // Avoid seeking a third time when we were already at the end of the
    // stream. The branch is usually way cheaper than a seek operation.
    if old_pos != len {
        seek.seek(SeekFrom::Start(old_pos))?;
    }

    Ok(len)
}

/// Reads a [`FileMetaData`] from the reader, located at the end of the file.
pub fn read_metadata<R: Read + Seek>(reader: &mut R) -> Result<FileMetaData> {
    // check file is large enough to hold footer
    let file_size = stream_len(reader)?;
    read_metadata_with_size(reader, file_size)
}


/// Reads a [`FileMetaData`] from the reader, located at the end of the file, with known file size.
pub fn read_metadata_with_size<R: Read + Seek>(
    reader: &mut R,
    file_size: u64,
) -> Result<FileMetaData> {
    if file_size < HEADER_SIZE + FOOTER_SIZE {
        return Err(Error::oos(
            "A parquet file must contain a header and footer with at least 12 bytes",
        ));
    }
    println!("IN READ METADATA");

    // read and cache up to DEFAULT_FOOTER_READ_SIZE bytes from the end and process the footer
    let default_end_len = min(DEFAULT_FOOTER_READ_SIZE, file_size) as usize;
    reader.seek(SeekFrom::End(-(default_end_len as i64)))?;

    let mut buffer = Vec::with_capacity(default_end_len);
    reader
        .by_ref()
        .take(default_end_len as u64)
        .read_to_end(&mut buffer)?;

    // check this is indeed a parquet file
    if buffer[default_end_len - 4..] != PARQUET_MAGIC {
        return Err(Error::oos("The file must end with PAR1"));
    }

    let metadata_len = metadata_len(&buffer, default_end_len);

    let metadata_len: u64 = metadata_len.try_into()?;

    let footer_len = FOOTER_SIZE + metadata_len;
    if footer_len > file_size {
        return Err(Error::oos(
            "The footer size must be smaller or equal to the file's size",
        ));
    }

    let reader: &[u8] = if (footer_len as usize) < buffer.len() {
        // the whole metadata is in the bytes we already read
        let remaining = buffer.len() - footer_len as usize;
        &buffer[remaining..]
    } else {
        // the end of file read by default is not long enough, read again including the metadata.
        reader.seek(SeekFrom::End(-(footer_len as i64)))?;

        buffer.clear();
        buffer.try_reserve(footer_len as usize)?;
        reader.take(footer_len).read_to_end(&mut buffer)?;

        &buffer
    };

    // a highly nested but sparse struct could result in many allocations
    let max_size = reader.len() * 2 + 1024;
    let mut buff = Vec::with_capacity(reader.len());
    buff.extend_from_slice(reader);
    let mut reader = io::Cursor::new(buff);
    println!("num rows: {}", deserialize_num_rows(&mut reader, max_size)?);
    let curr_pos = reader.position();
    println!("read N bytes: {curr_pos} out of {metadata_len}");
    panic!("only printing num rows");
    // deserialize_metadata(reader, max_size)
}

/// Parse loaded metadata bytes
pub fn deserialize_metadata<R: Read + Seek>(reader: R, max_size: usize) -> Result<FileMetaData> {
    let mut rr = BufReader::new(reader);
    let mut prot = TCompactInputProtocol::new(&mut rr, max_size);
    let num_rows = partial_num_rows_deserialize(&mut prot)?;
    panic!("only printing num rows");
    // let metadata = TFileMetaData::read_from_in_protocol(&mut prot)?;
    // FileMetaData::try_from_thrift(metadata)
}


pub fn deserialize_num_rows<R: Read + Seek>(reader: R, max_size: usize) -> Result<i64> {
    let mut prot = TCompactInputProtocol::new(reader, max_size);
    let num_rows = partial_num_rows_deserialize(&mut prot)?;
    Ok(num_rows)
}

// impl ReadThrift for FileMetaData {
fn partial_num_rows_deserialize<T: TInputProtocol>(i_prot: &mut T) -> thrift::Result<i64> {
    
      i_prot.read_struct_begin()?;
      let mut f_1: Option<i32> = None;
      let mut f_2: Option<Vec<SchemaElement>> = None;
      let mut f_3: Option<i64> = None;
      let mut f_4: Option<Vec<RowGroup>> = None;
      let mut f_5: Option<Vec<KeyValue>> = None;
      let mut f_6: Option<String> = None;
      let mut f_7: Option<Vec<ColumnOrder>> = None;
      let mut f_8: Option<EncryptionAlgorithm> = None;
      let mut f_9: Option<Vec<u8>> = None;
      loop {
        let field_ident = i_prot.read_field_begin()?;
        if field_ident.field_type == TType::Stop {
          break;
        }
        let field_id = field_id(&field_ident)?;
        println!("deserializing field_id: {field_id}");
        match field_id {
          1 => {
            let val = i_prot.read_i32()?;
            f_1 = Some(val);
          },
          2 => {
            let val = i_prot.read_list()?;
            f_2 = Some(val);
          },
          3 => {
            let val = i_prot.read_i64()?;
            return Ok(val);
          },
          4 => {
            let val = i_prot.read_list()?;
            f_4 = Some(val);
          },
          5 => {
            let val = i_prot.read_list()?;
            f_5 = Some(val);
          },
          6 => {
            let val = i_prot.read_string()?;
            f_6 = Some(val);
          },
          7 => {
            let val = i_prot.read_list()?;
            f_7 = Some(val);
          },
          8 => {
            let val = EncryptionAlgorithm::read_from_in_protocol(i_prot)?;
            f_8 = Some(val);
          },
          9 => {
            let val = i_prot.read_bytes()?;
            f_9 = Some(val);
          },
          _ => {
            i_prot.skip(field_ident.field_type)?;
          },
        };
        i_prot.read_field_end()?;
      }
      i_prot.read_struct_end()?;
      panic!("We didn't decode num rows");
}
//   }