use pyo3::{types::PyAnyMethods, PyObject, Python};

use super::*;

pub(crate) fn in_notebook() -> bool {
    pyo3::Python::with_gil(|py| {
        py.import(pyo3::intern!(py, "daft.utils"))
            .and_then(|m| m.getattr(pyo3::intern!(py, "in_notebook")))
            .and_then(|m| m.call0())
            .and_then(|m| m.extract())
            .expect("Failed to determine if running in notebook")
    })
}

struct TqdmProgressBar {
    pb_id: usize,
    manager: TqdmProgressBarManager,
}

impl ProgressBar for TqdmProgressBar {
    fn set_message(&self, message: String) -> DaftResult<()> {
        self.manager.update_bar(self.pb_id, message.as_str())
    }

    fn close(&self) -> DaftResult<()> {
        self.manager.close_bar(self.pb_id)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TqdmProgressBarManager {
    inner: Arc<PyObject>,
}

impl TqdmProgressBarManager {
    pub fn new() -> Self {
        Python::with_gil(|py| {
            let module = py.import("daft.runners.progress_bar")?;
            let progress_bar_class = module.getattr("SwordfishProgressBar")?;
            let pb_object = progress_bar_class.call0()?;
            DaftResult::Ok(Self {
                inner: Arc::new(pb_object.into()),
            })
        })
        .expect("Failed to create progress bar")
    }

    fn update_bar(&self, pb_id: usize, message: &str) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.inner
                .call_method1(py, "update_bar", (pb_id, message))?;
            DaftResult::Ok(())
        })
    }

    fn close_bar(&self, pb_id: usize) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.inner.call_method1(py, "close_bar", (pb_id,))?;
            DaftResult::Ok(())
        })
    }
}

impl ProgressBarManager for TqdmProgressBarManager {
    fn make_new_bar(
        &self,
        _color: ProgressBarColor,
        prefix: &str,
    ) -> DaftResult<Box<dyn ProgressBar>> {
        let bar_format = format!("🗡️ 🐟 {prefix}: {{elapsed}} {{desc}}", prefix = prefix);
        let pb_id = Python::with_gil(|py| {
            let pb_id = self.inner.call_method1(py, "make_new_bar", (bar_format,))?;
            let pb_id = pb_id.extract::<usize>(py)?;
            DaftResult::Ok(pb_id)
        })?;

        DaftResult::Ok(Box::new(TqdmProgressBar {
            pb_id,
            manager: self.clone(),
        }))
    }

    fn close_all(&self) -> DaftResult<()> {
        Python::with_gil(|py| {
            self.inner.call_method0(py, "close")?;
            DaftResult::Ok(())
        })
    }
}
