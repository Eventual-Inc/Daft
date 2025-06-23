use common_error::DaftResult;
use pyo3::{types::PyAnyMethods, PyObject, PyResult, Python};

use crate::statistics::{StatisticsEvent, StatisticsSubscriber};

pub(crate) struct FlotillaProgressBar {
    progress_bar_pyobject: PyObject,
}

impl FlotillaProgressBar {
    pub fn try_new(py: Python) -> PyResult<Self> {
        let progress_bar_module = py.import(pyo3::intern!(py, "daft.runners.progress_bar"))?;
        let progress_bar_class = progress_bar_module.getattr(pyo3::intern!(py, "ProgressBar"))?;
        let progress_bar = progress_bar_class.call1((true,))?.extract::<PyObject>()?;
        Ok(Self {
            progress_bar_pyobject: progress_bar,
        })
    }

    pub fn make_bar_or_update_total(&self, bar_id: i64, bar_name: &str) -> PyResult<()> {
        Python::with_gil(|py| {
            let progress_bar = self
                .progress_bar_pyobject
                .getattr(py, pyo3::intern!(py, "make_bar_or_update_total"))?;
            progress_bar.call1(py, (bar_id, bar_name))?;
            Ok(())
        })
    }

    pub fn update_bar(&self, bar_id: i64) -> PyResult<()> {
        Python::with_gil(|py| {
            let progress_bar = self
                .progress_bar_pyobject
                .getattr(py, pyo3::intern!(py, "update_bar"))?;
            progress_bar.call1(py, (bar_id,))?;
            Ok(())
        })
    }

    pub fn close(&self) {
        Python::with_gil(|py| {
            let progress_bar = self
                .progress_bar_pyobject
                .getattr(py, pyo3::intern!(py, "close"))
                .expect("Failed to get close method");
            progress_bar.call0(py).expect("Failed to call close method");
        })
    }
}

impl Drop for FlotillaProgressBar {
    fn drop(&mut self) {
        self.close();
    }
}

impl StatisticsSubscriber for FlotillaProgressBar {
    fn handle_event(&self, event: &StatisticsEvent) -> DaftResult<()> {
        match event {
            StatisticsEvent::TaskSubmitted { task_id, task_name } => {
                self.make_bar_or_update_total(
                    (task_id.stage_id() + task_id.plan_id()) as i64,
                    task_name,
                )?;
                Ok(())
            }
            StatisticsEvent::TaskScheduled { .. } => Ok(()),
            StatisticsEvent::TaskFinished { task_id } => {
                self.update_bar((task_id.stage_id() + task_id.plan_id()) as i64)?;
                Ok(())
            }
        }
    }
}
