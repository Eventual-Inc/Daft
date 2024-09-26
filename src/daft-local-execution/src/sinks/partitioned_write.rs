use std::{collections::HashMap, sync::Arc};

use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_file_formats::FileFormat;
use daft_core::{
    prelude::{SchemaRef, Utf8Array},
    series::IntoSeries,
};
use daft_dsl::ExprRef;
use daft_io::IOStatsContext;
use daft_micropartition::{create_file_writer, FileWriter, MicroPartition};
use daft_plan::OutputFileInfo;
use daft_table::Table;
use snafu::ResultExt;

use crate::{
    buffer::RowBasedBuffer,
    channel::{create_channel, PipelineChannel, Receiver, Sender},
    pipeline::PipelineNode,
    runtime_stats::{CountingReceiver, RuntimeStatsContext},
    ExecutionRuntimeHandle, JoinSnafu, NUM_CPUS,
};

struct SizedDataWriter {
    root_dir: String,
    file_format: FileFormat,
    compression: Option<String>,
    io_config: Option<daft_io::IOConfig>,
    writer: Box<dyn FileWriter>,
    target_file_rows: usize,
    written_files: Vec<Option<String>>,
    written_rows_so_far: usize,
}

impl SizedDataWriter {
    fn new(
        root_dir: String,
        target_file_rows: usize,
        file_format: FileFormat,
        compression: Option<String>,
        io_config: Option<daft_io::IOConfig>,
    ) -> DaftResult<Self> {
        Ok(Self {
            writer: create_file_writer(&root_dir, 0, &compression, &io_config, file_format)?,
            root_dir,
            file_format,
            compression,
            io_config,
            target_file_rows,
            written_files: vec![],
            written_rows_so_far: 0,
        })
    }

    fn write(&mut self, data: &Arc<MicroPartition>) -> DaftResult<()> {
        let len = data.len();
        self.writer.write(data)?;
        if self.written_rows_so_far + len >= self.target_file_rows {
            let file_path = self.writer.close()?;
            if let Some(file_path) = file_path {
                self.written_files.push(Some(file_path));
            }
            self.written_rows_so_far = 0;
            self.writer = create_file_writer(
                &self.root_dir,
                self.written_files.len(),
                &self.compression,
                &self.io_config,
                self.file_format,
            )?;
        }
        Ok(())
    }

    fn finalize(&mut self) -> DaftResult<Vec<Option<String>>> {
        if let Some(file_path) = self.writer.close()? {
            self.written_files.push(Some(file_path));
        }
        Ok(self.written_files.clone())
    }
}

pub(crate) struct PartitionedWriteNode {
    child: Box<dyn PipelineNode>,
    runtime_stats: Arc<RuntimeStatsContext>,
    file_info: OutputFileInfo,
    file_schema: SchemaRef,
    target_file_rows: usize,
    target_chunk_rows: usize,
}

impl PartitionedWriteNode {
    pub(crate) fn new(
        child: Box<dyn PipelineNode>,
        file_info: &OutputFileInfo,
        file_schema: &SchemaRef,
        target_file_rows: usize,
        target_chunk_rows: usize,
    ) -> Self {
        Self {
            child,
            runtime_stats: RuntimeStatsContext::new(),
            file_info: file_info.clone(),
            file_schema: file_schema.clone(),
            target_file_rows,
            target_chunk_rows,
        }
    }

    pub(crate) fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }

    fn partition(
        partition_cols: &[ExprRef],
        default_partition_fallback: Arc<str>,
        data: &Arc<MicroPartition>,
    ) -> DaftResult<(Vec<MicroPartition>, Table, Vec<Arc<str>>)> {
        let (split_tables, partition_values) = data.partition_by_value(partition_cols)?;
        let concated = partition_values
            .concat_or_get(IOStatsContext::new("MicroPartition::partition_by_value"))?;
        let partition_values_table = concated.first().unwrap();
        let pkey_names = partition_values_table.column_names();

        let mut values_string_values = Vec::with_capacity(partition_values_table.len());
        for name in pkey_names.iter() {
            let column = partition_values_table.get_column(name)?;
            let string_names = column.to_str_values()?;
            let default_part = Utf8Array::from_iter(
                "default",
                std::iter::once(Some(default_partition_fallback.clone())),
            )
            .into_series();
            let null_filled = string_names.if_else(&default_part, &column.not_null()?)?;
            values_string_values.push(null_filled);
        }

        let mut part_keys_postfixes = Vec::with_capacity(partition_values_table.len());
        for i in 0..partition_values_table.len() {
            let postfix = pkey_names
                .iter()
                .zip(values_string_values.iter())
                .map(|(pkey, values)| {
                    format!("{}={}", pkey, values.utf8().unwrap().get(i).unwrap())
                })
                .collect::<Vec<_>>()
                .join("/");
            part_keys_postfixes.push(Arc::from(postfix));
        }

        Ok((
            split_tables,
            partition_values_table.clone(),
            part_keys_postfixes,
        ))
    }

    async fn run_writer(
        mut input_receiver: Receiver<Arc<MicroPartition>>,
        file_info: Arc<OutputFileInfo>,
        default_partition_fallback: Arc<str>,
        target_chunk_rows: usize,
        target_file_rows: usize,
    ) -> DaftResult<Option<Table>> {
        let mut writers: HashMap<Arc<str>, SizedDataWriter> = HashMap::new();
        let mut buffers: HashMap<Arc<str>, RowBasedBuffer> = HashMap::new();
        let mut partition_key_values: HashMap<Arc<str>, Table> = HashMap::new();
        while let Some(data) = input_receiver.recv().await {
            let (split_tables, partition_values_table, part_keys_postfixes) = Self::partition(
                file_info.partition_cols.as_ref().unwrap(),
                default_partition_fallback.clone(),
                &data,
            )?;
            for (idx, (postfix, partition)) in part_keys_postfixes
                .iter()
                .zip(split_tables.into_iter())
                .enumerate()
            {
                if !partition_key_values.contains_key(postfix) {
                    let partition_value_row = partition_values_table.slice(idx, idx + 1)?;
                    partition_key_values.insert(postfix.clone(), partition_value_row);
                }

                let buffer = if let Some(buffer) = buffers.get_mut(postfix) {
                    buffer
                } else {
                    let buffer = RowBasedBuffer::new(target_chunk_rows);
                    buffers.insert(postfix.clone(), buffer);
                    &mut buffers.get_mut(postfix).unwrap()
                };
                buffer.push(Arc::new(partition));

                if let Some(ready) = buffer.pop_enough()? {
                    for part in ready {
                        if let Some(writer) = writers.get_mut(postfix) {
                            writer.write(&part)?;
                        } else {
                            let mut writer = SizedDataWriter::new(
                                format!("{}/{}", file_info.root_dir, postfix),
                                target_file_rows,
                                file_info.file_format,
                                file_info.compression.clone(),
                                file_info.io_config.clone(),
                            )?;
                            writer.write(&part)?;
                            writers.insert(postfix.clone(), writer);
                        }
                    }
                }
            }
        }
        for (postfix, buffer) in buffers.iter_mut() {
            let remaining = buffer.pop_all()?;
            if let Some(part) = remaining {
                if let Some(writer) = writers.get_mut(postfix) {
                    writer.write(&part)?;
                } else {
                    let mut writer = SizedDataWriter::new(
                        format!("{}/{}", file_info.root_dir, postfix),
                        target_file_rows,
                        file_info.file_format,
                        file_info.compression.clone(),
                        file_info.io_config.clone(),
                    )?;
                    writer.write(&part)?;
                    writers.insert(postfix.clone(), writer);
                }
            }
        }

        let mut written_files = Vec::with_capacity(writers.len());
        let mut partition_keys_values = Vec::with_capacity(writers.len());
        for (postfix, partition_key_value) in partition_key_values.iter() {
            let writer = writers.get_mut(postfix).unwrap();
            let file_paths = writer.finalize()?;
            if !file_paths.is_empty() {
                if file_paths.len() > partition_key_value.len() {
                    let mut columns = Vec::with_capacity(partition_key_value.num_columns());
                    let column_names = partition_key_value.column_names();
                    for name in column_names {
                        let column = partition_key_value.get_column(name)?;
                        let broadcasted = column.broadcast(file_paths.len())?;
                        columns.push(broadcasted);
                    }
                    let table = Table::from_nonempty_columns(columns)?;
                    partition_keys_values.push(table);
                } else {
                    partition_keys_values.push(partition_key_value.clone());
                }
            }
            written_files.extend(file_paths.into_iter());
        }
        if written_files.is_empty() {
            return Ok(None);
        }
        let written_files_table = Table::from_nonempty_columns(vec![Utf8Array::from_iter(
            "path",
            written_files.into_iter(),
        )
        .into_series()])?;
        if !partition_keys_values.is_empty() {
            let unioned = written_files_table.union(&Table::concat(&partition_keys_values)?)?;
            Ok(Some(unioned))
        } else {
            Ok(Some(written_files_table))
        }
    }

    fn spawn_writers(
        num_writers: usize,
        task_set: &mut tokio::task::JoinSet<DaftResult<Option<Table>>>,
        file_info: &Arc<OutputFileInfo>,
        default_partition_fallback: &str,
        target_chunk_rows: usize,
        target_file_rows: usize,
    ) -> Vec<Sender<Arc<MicroPartition>>> {
        let mut writer_senders = Vec::with_capacity(num_writers);
        for _ in 0..num_writers {
            let (writer_sender, writer_receiver) = create_channel(1);
            task_set.spawn(Self::run_writer(
                writer_receiver,
                file_info.clone(),
                default_partition_fallback.into(),
                target_chunk_rows,
                target_file_rows,
            ));
            writer_senders.push(writer_sender);
        }
        writer_senders
    }

    async fn dispatch(
        mut input_receiver: CountingReceiver,
        senders: Vec<Sender<Arc<MicroPartition>>>,
        partiton_cols: Vec<ExprRef>,
    ) -> DaftResult<()> {
        while let Some(data) = input_receiver.recv().await {
            let partitioned = data
                .as_data()
                .partition_by_hash(&partiton_cols, senders.len())?;
            for (idx, mp) in partitioned.into_iter().enumerate() {
                if !mp.is_empty() {
                    let _ = senders[idx].send(mp.into()).await;
                }
            }
        }
        Ok(())
    }
}

impl TreeDisplay for PartitionedWriteNode {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        writeln!(display, "{}", self.name()).unwrap();
        use common_display::DisplayLevel::*;
        match level {
            Compact => {}
            _ => {
                let rt_result = self.runtime_stats.result();
                rt_result.display(&mut display, true, true, true).unwrap();
            }
        }
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }
}

impl PipelineNode for PartitionedWriteNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![self.child.as_ref()]
    }

    fn name(&self) -> &'static str {
        "PartitionedWrite"
    }

    fn start(
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeHandle,
    ) -> crate::Result<PipelineChannel> {
        let child = self.child.as_mut();
        let child_results_receiver = child
            .start(false, runtime_handle)?
            .get_receiver_with_stats(&self.runtime_stats);

        let mut destination_channel = PipelineChannel::new(1, maintain_order);
        let destination_sender =
            destination_channel.get_next_sender_with_stats(&self.runtime_stats);
        let file_info = Arc::new(self.file_info.clone());
        let target_chunk_rows = self.target_chunk_rows;
        let target_file_rows = self.target_file_rows;
        let schema = self.file_schema.clone();
        runtime_handle.spawn(
            async move {
                let mut task_set = tokio::task::JoinSet::new();
                let writer_senders = Self::spawn_writers(
                    *NUM_CPUS,
                    &mut task_set,
                    &file_info,
                    "__HIVE_DEFAULT_PARTITION__",
                    target_chunk_rows,
                    target_file_rows,
                );
                Self::dispatch(
                    child_results_receiver,
                    writer_senders,
                    file_info.partition_cols.clone().unwrap(),
                )
                .await?;
                let mut results = vec![];
                while let Some(result) = task_set.join_next().await {
                    if let Some(result) = result.context(JoinSnafu)?? {
                        results.push(result);
                    }
                }
                if results.is_empty() {
                    return Ok(());
                }
                let result_mp =
                    Arc::new(MicroPartition::new_loaded(schema, Arc::new(results), None));
                let _ = destination_sender.send(result_mp.into()).await;
                Ok(())
            },
            self.name(),
        );
        Ok(destination_channel)
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}
