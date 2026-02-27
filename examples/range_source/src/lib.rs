use std::{ffi::CStr, sync::Arc};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use daft_ext::prelude::*;

// ── Module ──────────────────────────────────────────────────────────

#[daft_extension]
struct RangeExtension;

impl DaftExtension for RangeExtension {
    fn install(session: &mut dyn DaftSession) {
        session.define_source(Arc::new(RangeSource));
    }
}

// ── Source ──────────────────────────────────────────────────────────

struct RangeSource;

/// Parse options JSON, returning (n, partitions).
fn parse_options(options: &str) -> (u64, u32) {
    let v: serde_json::Value = serde_json::from_str(options).unwrap_or_default();
    let n = v.get("n").and_then(|v| v.as_u64()).unwrap_or(1000);
    let partitions = v.get("partitions").and_then(|v| v.as_u64()).unwrap_or(4) as u32;
    (n, partitions)
}

impl DaftSource for RangeSource {
    fn name(&self) -> &CStr {
        c"range"
    }

    fn schema(&self, _options: &str) -> DaftResult<Vec<Field>> {
        Ok(vec![Field::new("value", DataType::Int64, false)])
    }

    fn num_tasks(&self, options: &str) -> u32 {
        let (_, partitions) = parse_options(options);
        partitions
    }

    fn create_task(
        &self,
        options: &str,
        task_index: u32,
        pushdowns: &ScanPushdowns,
    ) -> DaftResult<Box<dyn DaftSourceTask>> {
        let (n, partitions) = parse_options(options);
        let per_task = n / partitions as u64;
        let start = per_task * task_index as u64;
        let end = if task_index == partitions - 1 {
            n
        } else {
            start + per_task
        };
        Ok(Box::new(RangeTask {
            current: start,
            end,
            limit: pushdowns.limit,
            emitted: 0,
        }))
    }
}

// ── Task ────────────────────────────────────────────────────────────

struct RangeTask {
    current: u64,
    end: u64,
    limit: Option<usize>,
    emitted: usize,
}

impl DaftSourceTask for RangeTask {
    fn next_batch(&mut self) -> DaftResult<Option<RecordBatch>> {
        if self.current >= self.end {
            return Ok(None);
        }
        if let Some(limit) = self.limit {
            if self.emitted >= limit {
                return Ok(None);
            }
        }

        let batch_size = 1024.min((self.end - self.current) as usize);
        let batch_size = if let Some(limit) = self.limit {
            batch_size.min(limit - self.emitted)
        } else {
            batch_size
        };

        let values: Int64Array = (self.current..self.current + batch_size as u64)
            .map(|v| Some(v as i64))
            .collect();
        self.current += batch_size as u64;
        self.emitted += batch_size;

        Ok(Some(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)])),
                vec![Arc::new(values)],
            )
            .map_err(|e| DaftError::RuntimeError(e.to_string()))?,
        ))
    }
}
