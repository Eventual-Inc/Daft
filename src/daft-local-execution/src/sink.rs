use std::{collections::VecDeque, sync::Arc};

use daft_micropartition::MicroPartition;

pub enum StreamingSink {
    Limit {
        limit: usize,
        partitions: VecDeque<Arc<MicroPartition>>,
        num_rows_taken: usize,
    },
    Coalesce {
        partitions: VecDeque<Arc<MicroPartition>>,
        chunk_sizes: Vec<usize>,
        current_chunk: usize,
        holding_area: Vec<Arc<MicroPartition>>,
    },
}

pub enum SinkResultType {
    NeedMoreInput,
    Finished,
}

impl StreamingSink {
    pub fn sink(&mut self, input: &Arc<MicroPartition>) -> SinkResultType {
        match self {
            StreamingSink::Limit {
                limit,
                partitions,
                num_rows_taken,
            } => {
                if num_rows_taken == limit {
                    return SinkResultType::Finished;
                }
                let input_num_rows = input.len();
                if *num_rows_taken + input_num_rows <= *limit {
                    *num_rows_taken += input_num_rows;
                    partitions.push_back(input.clone());
                    SinkResultType::NeedMoreInput
                } else {
                    let num_rows_to_take = *limit - *num_rows_taken;
                    let taken = input.head(num_rows_to_take).unwrap();
                    *num_rows_taken = *limit;
                    partitions.push_back(Arc::new(taken));
                    SinkResultType::Finished
                }
            }
            StreamingSink::Coalesce {
                partitions,
                chunk_sizes,
                current_chunk,
                holding_area,
            } => {
                holding_area.push(input.clone());

                if holding_area.len() == chunk_sizes[*current_chunk] {
                    let concated_partition = MicroPartition::concat(
                        &holding_area.iter().map(|x| x.as_ref()).collect::<Vec<_>>(),
                    )
                    .unwrap();
                    partitions.push_back(Arc::new(concated_partition));
                    holding_area.clear();
                    *current_chunk += 1;
                }

                if *current_chunk == chunk_sizes.len() {
                    SinkResultType::Finished
                } else {
                    SinkResultType::NeedMoreInput
                }
            }
        }
    }

    pub fn stream_out_one(&mut self) -> Option<Arc<MicroPartition>> {
        match self {
            StreamingSink::Limit { partitions, .. } => partitions.pop_front(),
            StreamingSink::Coalesce { partitions, .. } => partitions.pop_front(),
        }
    }
}
