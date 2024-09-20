use std::{cmp::min, collections::HashMap, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormat;
use daft_core::{prelude::Utf8Array, series::IntoSeries};
use daft_dsl::ExprRef;
use daft_io::IOStatsContext;
use daft_micropartition::{FileWriter, MicroPartition};
use daft_plan::OutputFileInfo;
use daft_table::Table;
use tracing::instrument;

use super::{
    blocking_sink::{BlockingSink, BlockingSinkStatus},
    buffer::Buffer,
};
use crate::pipeline::PipelineResultType;

fn create_file_writer(
    root_dir: &str,
    file_idx: usize,
    compression: &Option<String>,
    io_config: &Option<daft_io::IOConfig>,
    format: FileFormat,
) -> DaftResult<Box<dyn FileWriter>> {
    match format {
        #[cfg(feature = "python")]
        FileFormat::Parquet => Ok(Box::new(
            daft_micropartition::py_writers::PyArrowParquetWriter::new(
                root_dir,
                file_idx,
                compression,
                io_config,
            )?,
        )),
        #[cfg(feature = "python")]
        FileFormat::Csv => Ok(Box::new(
            daft_micropartition::py_writers::PyArrowCSVWriter::new(root_dir, file_idx, io_config)?,
        )),
        _ => Err(DaftError::ComputeError(
            "Unsupported file format for physical write".to_string(),
        )),
    }
}

struct SizedDataWriter {
    root_dir: String,
    file_format: FileFormat,
    compression: Option<String>,
    io_config: Option<daft_io::IOConfig>,
    writer: Box<dyn FileWriter>,
    size: usize,
    written_files: Vec<Option<String>>,
    written_bytes_so_far: usize,
}

impl SizedDataWriter {
    fn new(
        root_dir: String,
        size: usize,
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
            size,
            written_files: vec![],
            written_bytes_so_far: 0,
        })
    }

    fn write(&mut self, data: &Arc<MicroPartition>) -> DaftResult<()> {
        let size = data.size_bytes().unwrap().unwrap();
        self.writer.write(data)?;
        if self.written_bytes_so_far + size >= self.size {
            let file_path = self.writer.close()?;
            if let Some(file_path) = file_path {
                self.written_files.push(Some(file_path));
            }
            self.written_bytes_so_far = 0;
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
            let mut written_files = self.written_files.clone();
            written_files.push(Some(file_path));
            Ok(written_files.clone())
        } else {
            Ok(self.written_files.clone())
        }
    }
}

struct UnpartitionedWriteExecutor {
    writer: SizedDataWriter,
    buffer: Buffer,
}

impl UnpartitionedWriteExecutor {
    fn new(
        file_info: &OutputFileInfo,
        inflation_factor: f64,
        target_file_size: usize,
        target_row_group_size: usize,
    ) -> DaftResult<Self> {
        let target_in_memory_file_size = target_file_size * inflation_factor as usize;
        let target_chunk_size = match file_info.file_format {
            FileFormat::Parquet => min(
                target_in_memory_file_size,
                target_row_group_size * inflation_factor as usize,
            ),
            FileFormat::Csv => 1024 * 1024,
            _ => {
                return Err(DaftError::ComputeError(
                    "Unsupported file format for physical write".to_string(),
                ))
            }
        };
        Ok(Self {
            writer: SizedDataWriter::new(
                file_info.root_dir.clone(),
                target_in_memory_file_size,
                file_info.file_format,
                file_info.compression.clone(),
                file_info.io_config.clone(),
            )?,
            buffer: Buffer::new(target_chunk_size),
        })
    }

    fn submit(&mut self, data: &Arc<MicroPartition>) -> DaftResult<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.buffer.push(data.clone());
        if let Some(partition) = self.buffer.try_clear() {
            let part = partition?;
            self.writer.write(&part)?;
        }
        Ok(())
    }

    fn finalize(&mut self) -> DaftResult<Table> {
        while let Some(part) = self.buffer.try_clear() {
            let part = part?;
            self.writer.write(&part)?;
        }
        if let Some(part) = self.buffer.clear_all() {
            let part = part?;
            self.writer.write(&part)?;
        }

        let written_files = self.writer.finalize();
        let written_file_paths_series =
            Utf8Array::from_iter("path", written_files?.into_iter()).into_series();
        let result_table = Table::from_nonempty_columns(vec![written_file_paths_series])?;
        Ok(result_table)
    }
}

struct PartitionedWriteExecutor {
    writers: HashMap<String, SizedDataWriter>,
    buffers: HashMap<String, (Buffer, Table)>,
    target_chunk_size: usize,
    root_dir: String,
    file_format: FileFormat,
    compression: Option<String>,
    io_config: Option<daft_io::IOConfig>,
    partition_cols: Vec<ExprRef>,
}

impl PartitionedWriteExecutor {
    fn new(
        file_info: &OutputFileInfo,
        inflation_factor: f64,
        target_file_size: usize,
        target_row_group_size: usize,
    ) -> DaftResult<Self> {
        assert!(file_info.partition_cols.is_some());
        let target_in_memory_file_size = target_file_size * inflation_factor as usize;
        let target_chunk_size = match file_info.file_format {
            FileFormat::Parquet => min(
                target_in_memory_file_size,
                target_row_group_size * inflation_factor as usize,
            ),
            FileFormat::Csv => 1024 * 1024,
            _ => {
                return Err(DaftError::ComputeError(
                    "Unsupported file format for physical write".to_string(),
                ))
            }
        };
        Ok(Self {
            writers: HashMap::new(),
            buffers: HashMap::new(),
            target_chunk_size,
            root_dir: file_info.root_dir.clone(),
            file_format: file_info.file_format,
            compression: file_info.compression.clone(),
            io_config: file_info.io_config.clone(),
            partition_cols: file_info.partition_cols.clone().unwrap(),
        })
    }

    fn submit(&mut self, data: &Arc<MicroPartition>) -> DaftResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        let (partitioned, partition_keys) = data.partition_by_value(&self.partition_cols)?;
        let concated = partition_keys
            .concat_or_get(IOStatsContext::new("MicroPartition::partition_by_value"))?;
        let partition_keys_table = concated.first().unwrap();
        let partition_col_names = partition_keys.column_names();
        let mut values_string_values = Vec::with_capacity(partition_col_names.len());
        for name in partition_col_names.iter() {
            let column = partition_keys_table.get_column(name)?;
            let string_names = column.to_str_values()?;
            let default_part = Utf8Array::from_iter(
                "default",
                std::iter::once(Some("__HIVE_DEFAULT_PARTITION__")),
            )
            .into_series();
            let null_filled = string_names.if_else(&default_part, &column.not_null()?)?;
            values_string_values.push(null_filled);
        }

        let mut part_keys_postfixes = Vec::with_capacity(partition_keys_table.len());
        for i in 0..partition_keys_table.len() {
            let postfix = partition_col_names
                .iter()
                .zip(values_string_values.iter())
                .map(|(pkey, values)| {
                    format!("{}={}", pkey, values.utf8().unwrap().get(i).unwrap())
                })
                .collect::<Vec<_>>()
                .join("/");
            part_keys_postfixes.push(postfix);
        }

        for (idx, (postfix, partition)) in part_keys_postfixes
            .iter()
            .zip(partitioned.into_iter())
            .enumerate()
        {
            let buffer = if let Some((buffer, _)) = self.buffers.get_mut(postfix) {
                buffer
            } else {
                let buffer = Buffer::new(self.target_chunk_size);
                let row = partition_keys_table.slice(idx, idx + 1)?;
                self.buffers.insert(postfix.clone(), (buffer, row));
                &mut self.buffers.get_mut(postfix).unwrap().0
            };
            buffer.push(Arc::new(partition));
            if let Some(partition) = buffer.try_clear() {
                let part = partition?;
                if let Some(writer) = self.writers.get_mut(postfix) {
                    writer.write(&part)?;
                } else {
                    let mut writer = SizedDataWriter::new(
                        format!("{}/{}", self.root_dir, postfix),
                        self.target_chunk_size,
                        self.file_format,
                        self.compression.clone(),
                        self.io_config.clone(),
                    )?;
                    writer.write(&part)?;
                    self.writers.insert(postfix.clone(), writer);
                }
            }
        }
        Ok(())
    }

    fn finalize(&mut self) -> DaftResult<Table> {
        for (postfix, (buffer, _)) in self.buffers.iter_mut() {
            while let Some(partition) = buffer.try_clear() {
                let part = partition?;
                if let Some(writer) = self.writers.get_mut(postfix) {
                    writer.write(&part)?;
                } else {
                    let mut writer = SizedDataWriter::new(
                        format!("{}/{}", self.root_dir, postfix),
                        self.target_chunk_size,
                        self.file_format,
                        self.compression.clone(),
                        self.io_config.clone(),
                    )?;
                    writer.write(&part)?;
                    self.writers.insert(postfix.clone(), writer);
                }
            }
            if let Some(partition) = buffer.clear_all() {
                let part = partition?;
                if let Some(writer) = self.writers.get_mut(postfix) {
                    writer.write(&part)?;
                } else {
                    let mut writer = SizedDataWriter::new(
                        format!("{}/{}", self.root_dir, postfix),
                        self.target_chunk_size,
                        self.file_format,
                        self.compression.clone(),
                        self.io_config.clone(),
                    )?;
                    writer.write(&part)?;
                    self.writers.insert(postfix.clone(), writer);
                }
            }
        }

        let mut written_files = Vec::with_capacity(self.writers.len());
        let mut partition_keys_values = Vec::with_capacity(self.writers.len());
        for (postfix, (_, row)) in self.buffers.iter() {
            let writer = self.writers.get_mut(postfix).unwrap();
            let file_paths = writer.finalize()?;
            if !file_paths.is_empty() {
                if file_paths.len() > row.len() {
                    let mut columns = Vec::with_capacity(row.num_columns());
                    let column_names = row.column_names();
                    for name in column_names {
                        let column = row.get_column(name)?;
                        let broadcasted = column.broadcast(file_paths.len())?;
                        columns.push(broadcasted);
                    }
                    let table = Table::from_nonempty_columns(columns)?;
                    partition_keys_values.push(table);
                } else {
                    partition_keys_values.push(row.clone());
                }
            }
            written_files.extend(file_paths.into_iter());
        }
        let written_files_table = Table::from_nonempty_columns(vec![Utf8Array::from_iter(
            "path",
            written_files.into_iter(),
        )
        .into_series()])?;
        if !partition_keys_values.is_empty() {
            let unioned = written_files_table.union(&Table::concat(&partition_keys_values)?)?;
            Ok(unioned)
        } else {
            Ok(written_files_table)
        }
    }
}
enum WriteExecutor {
    Unpartitioned(UnpartitionedWriteExecutor),
    Partitioned(PartitionedWriteExecutor),
}

impl WriteExecutor {
    fn new(
        file_info: OutputFileInfo,
        inflation_factor: f64,
        target_file_size: usize,
        target_row_group_size: usize,
    ) -> DaftResult<Self> {
        match file_info.partition_cols {
            Some(_) => Ok(Self::Partitioned(PartitionedWriteExecutor::new(
                &file_info,
                inflation_factor,
                target_file_size,
                target_row_group_size,
            )?)),
            None => Ok(Self::Unpartitioned(UnpartitionedWriteExecutor::new(
                &file_info,
                inflation_factor,
                target_file_size,
                target_row_group_size,
            )?)),
        }
    }

    fn submit(&mut self, data: &Arc<MicroPartition>) -> DaftResult<()> {
        match self {
            Self::Unpartitioned(executor) => executor.submit(data),
            Self::Partitioned(executor) => executor.submit(data),
        }
    }

    fn finalize(&mut self) -> DaftResult<Table> {
        match self {
            Self::Unpartitioned(executor) => executor.finalize(),
            Self::Partitioned(executor) => executor.finalize(),
        }
    }
}

pub(crate) struct PhysicalWriteSink {
    write_executor: WriteExecutor,
}

impl PhysicalWriteSink {
    pub(crate) fn new(
        file_info: OutputFileInfo,
        inflation_factor: f64,
        target_file_size: usize,
        target_row_group_size: usize,
    ) -> DaftResult<Self> {
        Ok(Self {
            write_executor: WriteExecutor::new(
                file_info,
                inflation_factor,
                target_file_size,
                target_row_group_size,
            )?,
        })
    }

    pub(crate) fn boxed(self) -> Box<dyn BlockingSink> {
        Box::new(self)
    }
}

impl BlockingSink for PhysicalWriteSink {
    #[instrument(skip_all, name = "PhysicalWriteSink::sink")]
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<BlockingSinkStatus> {
        self.write_executor.submit(input)?;
        Ok(BlockingSinkStatus::NeedMoreInput)
    }

    #[instrument(skip_all, name = "PhysicalWriteSink::finalize")]
    fn finalize(&mut self) -> DaftResult<Option<PipelineResultType>> {
        let written_files_table = self.write_executor.finalize()?;
        let schema = written_files_table.schema.clone();
        let result = Arc::new(MicroPartition::new_loaded(
            schema,
            Arc::new(vec![written_files_table]),
            None,
        ));
        Ok(Some(result.into()))
    }
    fn name(&self) -> &'static str {
        "PhysicalWriteSink"
    }
}
