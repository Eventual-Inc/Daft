use std::collections::HashMap;

use common_error::DaftResult;
use common_metrics::{ROWS_IN_KEY, ROWS_OUT_KEY, Stat};
use pyo3::{Py, PyAny, PyResult, Python, types::PyAnyMethods};

use crate::{
    scheduling::task::TaskContext,
    statistics::{StatisticsSubscriber, TaskEvent},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct BarId(i64);

impl From<&TaskContext> for BarId {
    fn from(task_context: &TaskContext) -> Self {
        Self(((task_context.query_idx as i64) << 32) | (task_context.last_node_id as i64))
    }
}

pub(crate) struct FlotillaProgressBar {
    progress_bar_pyobject: Py<PyAny>,
    bar_id_to_rows_out: HashMap<BarId, u64>,
    bar_id_to_rows_in: HashMap<BarId, u64>,
}

impl FlotillaProgressBar {
    pub fn try_new(py: Python) -> PyResult<Self> {
        let progress_bar_module = py.import(pyo3::intern!(py, "daft.runners.progress_bar"))?;
        let progress_bar_class = progress_bar_module.getattr(pyo3::intern!(py, "ProgressBar"))?;
        let progress_bar = progress_bar_class.call1((true,))?.extract::<Py<PyAny>>()?;
        Ok(Self {
            progress_bar_pyobject: progress_bar,
            bar_id_to_rows_out: HashMap::new(),
            bar_id_to_rows_in: HashMap::new(),
        })
    }

    fn make_bar_or_update_total(&self, bar_id: BarId, bar_name: &str) -> PyResult<()> {
        Python::attach(|py| {
            let progress_bar = self
                .progress_bar_pyobject
                .getattr(py, pyo3::intern!(py, "make_bar_or_update_total"))?;
            progress_bar.call1(py, (bar_id.0, bar_name))?;
            Ok(())
        })
    }

    fn update_bar(&self, bar_id: BarId, message: Option<String>) -> PyResult<()> {
        Python::attach(|py| {
            let progress_bar = self
                .progress_bar_pyobject
                .getattr(py, pyo3::intern!(py, "update_bar"))?;
            progress_bar.call1(py, (bar_id.0, message))?;
            Ok(())
        })
    }

    fn close(&self) {
        Python::attach(|py| {
            let progress_bar = self
                .progress_bar_pyobject
                .getattr(py, pyo3::intern!(py, "close"))
                .expect("Failed to get close method");
            progress_bar.call0(py).expect("Failed to call close method");
        });
    }
}

impl Drop for FlotillaProgressBar {
    fn drop(&mut self) {
        self.close();
    }
}

impl StatisticsSubscriber for FlotillaProgressBar {
    fn handle_event(&mut self, event: &TaskEvent) -> DaftResult<()> {
        match event {
            TaskEvent::Submitted { context, name } => {
                self.make_bar_or_update_total(BarId::from(context), name)?;
                Ok(())
            }
            // For progress bar we don't care if it is scheduled, for now.
            TaskEvent::Scheduled { .. } => Ok(()),
            TaskEvent::Completed { context, stats } => {
                let bar_id = BarId::from(context);
                let last_node_id = context.last_node_id as usize;
                let mut rows_out = 0;
                let mut rows_in = 0;
                for (node_id, snapshot) in stats {
                    if *node_id != last_node_id {
                        continue;
                    }
                    for (k, v) in snapshot.iter() {
                        if let Stat::Count(c) = v {
                            if k == ROWS_OUT_KEY {
                                rows_out += c;
                            } else if k == ROWS_IN_KEY {
                                rows_in += c;
                            }
                        }
                    }
                }

                let total_rows_out = {
                    let e = self.bar_id_to_rows_out.entry(bar_id).or_insert(0);
                    *e += rows_out;
                    *e
                };
                let total_rows_in = {
                    let e = self.bar_id_to_rows_in.entry(bar_id).or_insert(0);
                    *e += rows_in;
                    *e
                };

                self.update_bar(
                    bar_id,
                    Some(format!(
                        "{} rows out, {} rows in",
                        Stat::Count(total_rows_out),
                        Stat::Count(total_rows_in)
                    )),
                )?;
                Ok(())
            }
            // We don't care about failed tasks as they will be retried
            TaskEvent::Failed { .. } => Ok(()),
            // We consider cancelled tasks as finished tasks
            TaskEvent::Cancelled { context } => {
                self.update_bar(BarId::from(context), None)?;
                Ok(())
            }
        }
    }
}
