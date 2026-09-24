//! Index-pruned, log-time ordered traversal using the CRC-validating decoder.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use bytes::Bytes;
use common_error::DaftResult;
use mcap::records::{ChunkIndex, MessageHeader, Record};

use crate::{MAX_RECORD_LENGTH, McapReadOptions, mcap_error};

pub(crate) enum IndexedAction {
    ReadChunk {
        offset: u64,
        length: usize,
    },
    Message {
        header: MessageHeader,
        data: Vec<u8>,
    },
    End,
}

pub(crate) struct IndexedReader {
    chunks: VecDeque<ChunkIndex>,
    // File position breaks ties between equal timestamps deterministically.
    messages: BTreeMap<(u64, u64, usize), (MessageHeader, Vec<u8>)>,
    channels: Option<BTreeSet<u16>>,
    start_time: Option<u64>,
    end_time: Option<u64>,
}

impl IndexedReader {
    pub(crate) fn new(summary: &mcap::Summary, options: &McapReadOptions) -> DaftResult<Self> {
        let channels: Option<BTreeSet<_>> = options.topics.as_ref().map(|topics| {
            summary
                .channels
                .iter()
                .filter_map(|(id, channel)| topics.contains(&channel.topic).then_some(*id))
                .collect()
        });
        let mut chunks: Vec<_> = summary
            .chunk_indexes
            .iter()
            .filter(|chunk| {
                !options
                    .start_time
                    .is_some_and(|start| chunk.message_end_time < start)
                    && !options
                        .end_time
                        .is_some_and(|end| chunk.message_start_time >= end)
                    && channels.as_ref().is_none_or(|channels| {
                        !channels.is_empty()
                            && (chunk.message_index_offsets.is_empty()
                                || chunk
                                    .message_index_offsets
                                    .keys()
                                    .any(|id| channels.contains(id)))
                    })
            })
            .cloned()
            .collect();
        for chunk in &chunks {
            if chunk.compressed_size > MAX_RECORD_LENGTH as u64
                || chunk.uncompressed_size > MAX_RECORD_LENGTH as u64
            {
                return Err(mcap_error("indexed chunk exceeds record length limit"));
            }
            // Validate before using an index-provided length for allocation/I/O.
            let data_offset = chunk.compressed_data_offset().map_err(mcap_error)?;
            let expected_length = (data_offset - chunk.chunk_start_offset)
                .checked_add(chunk.compressed_size)
                .ok_or_else(|| mcap_error("chunk length overflow"))?;
            if chunk.chunk_length != expected_length {
                return Err(mcap_error(
                    "chunk index length does not match compressed size",
                ));
            }
        }
        chunks.sort_by_key(|chunk| (chunk.message_start_time, chunk.chunk_start_offset));
        Ok(Self {
            chunks: chunks.into(),
            messages: BTreeMap::new(),
            channels,
            start_time: options.start_time,
            end_time: options.end_time,
        })
    }

    pub(crate) fn next_action(&mut self) -> DaftResult<IndexedAction> {
        // Load all overlapping chunks before emitting a message, including ties.
        if self.chunks.front().is_some_and(|chunk| {
            self.messages
                .first_key_value()
                .is_none_or(|(key, _)| chunk.message_start_time <= key.0)
        }) {
            let chunk = self.chunks.pop_front().unwrap();
            return Ok(IndexedAction::ReadChunk {
                offset: chunk.chunk_start_offset,
                length: usize::try_from(chunk.chunk_length).map_err(mcap_error)?,
            });
        }
        Ok(match self.messages.pop_first() {
            Some((_, (header, data))) => IndexedAction::Message { header, data },
            None => IndexedAction::End,
        })
    }

    pub(crate) async fn insert_chunk(&mut self, offset: u64, bytes: Bytes) -> DaftResult<()> {
        if bytes.first() != Some(&mcap::records::op::CHUNK) {
            return Err(mcap_error("chunk index does not point to a chunk record"));
        }
        let options = mcap::tokio::LinearReaderOptions::default()
            .with_skip_start_magic(true)
            .with_skip_end_magic(true)
            .with_prevalidate_chunk_crcs(true)
            .with_record_length_limit(MAX_RECORD_LENGTH);
        let mut reader =
            mcap::tokio::LinearReader::new_with_options(std::io::Cursor::new(bytes), &options);
        let mut buffer = Vec::new();
        let mut position = 0;
        while let Some(opcode) = reader.next_record(&mut buffer).await {
            if let Record::Message { header, data } =
                mcap::parse_record(opcode.map_err(mcap_error)?, &buffer).map_err(mcap_error)?
                && !self.start_time.is_some_and(|start| header.log_time < start)
                && !self.end_time.is_some_and(|end| header.log_time >= end)
                && self
                    .channels
                    .as_ref()
                    .is_none_or(|channels| channels.contains(&header.channel_id))
            {
                self.messages.insert(
                    (header.log_time, offset, position),
                    (header, data.into_owned()),
                );
            }
            position += 1;
        }
        Ok(())
    }
}
