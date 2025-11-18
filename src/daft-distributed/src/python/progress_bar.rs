use common_error::DaftResult;
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
}

impl FlotillaProgressBar {
    pub fn try_new(py: Python) -> PyResult<Self> {
        let progress_bar_module = py.import(pyo3::intern!(py, "daft.runners.progress_bar"))?;
        let progress_bar_class = progress_bar_module.getattr(pyo3::intern!(py, "ProgressBar"))?;
        let progress_bar = progress_bar_class.call1((true,))?.extract::<Py<PyAny>>()?;
        Ok(Self {
            progress_bar_pyobject: progress_bar,
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

    fn update_bar(&self, bar_id: BarId) -> PyResult<()> {
        Python::attach(|py| {
            let progress_bar = self
                .progress_bar_pyobject
                .getattr(py, pyo3::intern!(py, "update_bar"))?;
            progress_bar.call1(py, (bar_id.0,))?;
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
            TaskEvent::Completed { context, .. } => {
                self.update_bar(BarId::from(context))?;
                Ok(())
            }
            // We don't care about failed tasks as they will be retried
            TaskEvent::Failed { .. } => Ok(()),
            // We consider cancelled tasks as finished tasks
            TaskEvent::Cancelled { context } => {
                self.update_bar(BarId::from(context))?;
                Ok(())
            }
        }
    }
}
