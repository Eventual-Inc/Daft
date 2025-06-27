use std::{collections::HashMap, sync::Mutex};

use common_error::DaftResult;
use pyo3::{types::PyAnyMethods, PyObject, PyResult, Python};

use crate::{
    plan::PlanID,
    scheduling::task::TaskContext,
    statistics::{StatisticsEvent, StatisticsSubscriber},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct BarId(i64);

impl From<&TaskContext> for BarId {
    fn from(task_context: &TaskContext) -> Self {
        Self(
            ((task_context.stage_id as i64) << 48)
                | ((task_context.plan_id as i64) << 32)
                | (task_context.node_id as i64),
        )
    }
}

pub(crate) struct FlotillaProgressBar {
    progress_bar_pyobject: PyObject,
    bar_ids_by_plan_id: Mutex<HashMap<PlanID, Vec<BarId>>>,
}

impl FlotillaProgressBar {
    pub fn try_new(py: Python) -> PyResult<Self> {
        let progress_bar_module = py.import(pyo3::intern!(py, "daft.runners.progress_bar"))?;
        let progress_bar_class = progress_bar_module.getattr(pyo3::intern!(py, "ProgressBar"))?;
        let progress_bar = progress_bar_class.call1((false,))?.extract::<PyObject>()?;
        Ok(Self {
            progress_bar_pyobject: progress_bar,
            bar_ids_by_plan_id: Mutex::new(HashMap::new()),
        })
    }

    fn make_bar_or_update_total(&self, bar_id: BarId, bar_name: &str) -> PyResult<()> {
        Python::with_gil(|py| {
            let progress_bar = self
                .progress_bar_pyobject
                .getattr(py, pyo3::intern!(py, "make_bar_or_update_total"))?;
            progress_bar.call1(py, (bar_id.0, bar_name))?;
            Ok(())
        })
    }

    fn update_bar(&self, bar_id: BarId) -> PyResult<()> {
        Python::with_gil(|py| {
            let progress_bar = self
                .progress_bar_pyobject
                .getattr(py, pyo3::intern!(py, "update_bar"))?;
            progress_bar.call1(py, (bar_id.0,))?;
            Ok(())
        })
    }

    fn close_bars(&self, bar_ids: Vec<BarId>) {
        Python::with_gil(|py| {
            for bar_id in bar_ids {
                let progress_bar = self
                    .progress_bar_pyobject
                    .getattr(py, pyo3::intern!(py, "close_bar"))
                    .expect("Failed to get close_bar method");
                progress_bar
                    .call1(py, (bar_id.0,))
                    .expect("Failed to call close_bar method");
            }
        });
    }
}

impl StatisticsSubscriber for FlotillaProgressBar {
    fn handle_event(&self, event: &StatisticsEvent) -> DaftResult<()> {
        match event {
            StatisticsEvent::SubmittedTask { context, name } => {
                let bar_id = BarId::from(context);
                self.bar_ids_by_plan_id
                    .lock()
                    .unwrap()
                    .entry(context.plan_id)
                    .or_default()
                    .push(bar_id);
                self.make_bar_or_update_total(bar_id, name)?;
                Ok(())
            }
            // For progress bar we don't care if it is scheduled, for now.
            StatisticsEvent::ScheduledTask { .. } => Ok(()),
            StatisticsEvent::FinishedTask { context } => {
                self.update_bar(BarId::from(context))?;
                Ok(())
            }
            StatisticsEvent::PlanStarted { .. } => Ok(()),
            StatisticsEvent::PlanFinished { plan_id } => {
                let bar_ids = self
                    .bar_ids_by_plan_id
                    .lock()
                    .unwrap()
                    .remove(plan_id)
                    .unwrap_or_default();
                self.close_bars(bar_ids);
                Ok(())
            }
        }
    }
}
