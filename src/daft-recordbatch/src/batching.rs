use pyo3::prelude::*;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use std::sync::Arc;
use futures::StreamExt;
use daft::python::PyRecordBatch;  

#[pyclass]
struct BatchIterator {
    prefetch_size: usize,
    batch_size: usize,
    receiver: Receiver<PyResult<PyRecordBatch>>, 
    #[pyo3(get)]
    worker_handle: PyObject,
}

#[pymethods]
impl BatchIterator {
    #[new]
    fn new(
        partitions_iter: PyObject, 
        batch_size: usize,
        prefetch_size: usize,
    ) -> PyResult<Self> {
        let (sender, receiver) = mpsc::channel(prefetch_size);
        
        let worker_handle = Python::with_gil(|py| {
            pyo3_asyncio::tokio::future_into_py(py, async move {
                let mut stream = PyPartitionStream::new(partitions_iter);
                
                while let Some(partition) = stream.next().await {
                    let record_batch = convert_to_record_batch(partition).await?;
                    split_and_buffer(record_batch, batch_size, sender.clone()).await?;
                }
                Ok(())
            })
        })?;

        Ok(Self {
            prefetch_size,
            batch_size,
            receiver,
            worker_handle,
        })
    }

    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<PyRecordBatch>> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            self.receiver.recv().await.transpose()
        })
    }
}

struct PyPartitionStream {
    iter: PyObject,
}

impl PyPartitionStream {
    fn new(iter: PyObject) -> Self {
        Self { iter }
    }
    
    async fn next(&mut self) -> Option<PyObject> {
        Python::with_gil(|py| {
            self.iter.call_method0(py, "__next__")
                .ok()
                .and_then(|res| {
                    if res.is_none(py) {
                        None
                    } else {
                        Some(res)
                    }
                })
        })
    }
}


async fn convert_to_record_batch(partition: PyObject) -> PyResult<PyRecordBatch> {
    Python::with_gil(|py| {
        let record_batch: &PyAny = partition.call_method0(py, "to_record_batch")?;
        record_batch.extract(py)
    })
}

#[pyclass]
struct BatchIterator {
    batch_size: usize,
    drop_last: bool, 
    buffer: Option<PyRecordBatch>, 
    receiver: Receiver<PyResult<PyRecordBatch>>,
    #[pyo3(get)]
    worker_handle: PyObject,
}

async fn split_and_buffer(
    record_batch: PyRecordBatch,
    batch_size: usize,
    drop_last: bool,
    sender: Sender<PyResult<PyRecordBatch>>,
    buffer: &mut Option<PyRecordBatch>,  
) -> PyResult<()> {
    
    let combined = if let Some(buf) = buffer.take() {
        buf.concat(&record_batch)?
    } else {
        record_batch
    };

    let total_rows = combined.num_rows();
    let mut offset = 0;

    while offset + batch_size <= total_rows {
        let end = offset + batch_size;
        let sliced = combined.slice(offset, end - offset)?;
        sender.send(Ok(sliced)).await?;
        offset = end;
    }

    if !drop_last && offset < total_rows {
        *buffer = Some(combined.slice(offset, total_rows - offset)?);
    }

    Ok(())
}

#[pymethods]
impl BatchIterator {
    #[new]
    fn new(
        partitions_iter: PyObject,
        batch_size: usize,
        prefetch_size: usize,
        drop_last: bool,  
    ) -> PyResult<Self> {
        let (sender, receiver) = mpsc::channel(prefetch_size);
        
        let worker_handle = Python::with_gil(|py| {
            pyo3_asyncio::tokio::future_into_py(py, async move {
                let mut stream = PyPartitionStream::new(partitions_iter);
                
                while let Some(partition) = stream.next().await {
                    let record_batch = convert_to_record_batch(partition).await?;
                    split_and_buffer(record_batch, batch_size, drop_last, sender.clone(), &mut buffer).await?;
                }
                
                if !drop_last {
                    if let Some(buf) = buffer {
                        sender.send(Ok(buf)).await?;
                    }
                }
                
                Ok(())
            })
        })?;

        Ok(Self {
            batch_size,
            drop_last,
            buffer: None, 
            receiver,
            worker_handle,
        })
    }
}
