#![feature(let_chains)]

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(not(target_env = "msvc"))]
union U {
    x: &'static u8,
    y: &'static libc::c_char,
}

#[cfg(target_env = "gnu")]
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: Option<&'static libc::c_char> = Some(unsafe {
    U {
        x: &b"oversize_threshold:1,background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000\0"[0],
    }
    .y
});

#[cfg(target_os = "macos")]
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: Option<&'static libc::c_char> = Some(unsafe {
    U {
        x: &b"oversize_threshold:1,background_thread:false,dirty_decay_ms:0,muzzy_decay_ms:0\0"[0],
    }
    .y
});

fn should_enable_chrome_trace() -> bool {
    let chrome_trace_var_name = "DAFT_DEV_ENABLE_CHROME_TRACE";
    if let Ok(val) = std::env::var(chrome_trace_var_name)
        && matches!(val.trim().to_lowercase().as_str(), "1" | "true")
    {
        true
    } else {
        false
    }
}

#[cfg(feature = "python")]
pub mod pylib {
    use common_tracing::init_tracing;
    use lazy_static::lazy_static;
    use pyo3::prelude::*;
    lazy_static! {
        static ref LOG_RESET_HANDLE: pyo3_log::ResetHandle = pyo3_log::init();
    }

    #[pyfunction]
    pub fn version() -> &'static str {
        daft_core::VERSION
    }

    #[pyfunction]
    pub fn build_type() -> &'static str {
        daft_core::DAFT_BUILD_TYPE
    }

    #[pyfunction]
    pub fn test_logging() {
        log::debug!("DEBUG from rust");
        log::info!("INFO from rust");
        log::warn!("WARN from rust");
        log::error!("ERROR from rust");
    }

    #[pyfunction]
    pub fn refresh_logger(py: Python) -> PyResult<()> {
        use log::LevelFilter;
        let logging = py.import("logging")?;
        let python_log_level = logging
            .getattr("getLogger")?
            .call0()?
            .getattr("level")?
            .extract::<usize>()
            .unwrap_or(0);

        // https://docs.python.org/3/library/logging.html#logging-levels
        let level_filter = match python_log_level {
            0 => LevelFilter::Off,
            1..=10 => LevelFilter::Debug,
            11..=20 => LevelFilter::Info,
            21..=30 => LevelFilter::Warn,
            _ => LevelFilter::Error,
        };

        LOG_RESET_HANDLE.reset();
        log::set_max_level(level_filter);
        Ok(())
    }

    #[pymodule]
    fn daft(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
        refresh_logger(_py)?;
        init_tracing(crate::should_enable_chrome_trace());

        common_daft_config::register_modules(_py, m)?;
        common_system_info::register_modules(_py, m)?;
        common_resource_request::register_modules(_py, m)?;
        daft_core::register_modules(_py, m)?;
        daft_core::python::register_modules(_py, m)?;
        daft_local_execution::register_modules(_py, m)?;
        daft_dsl::register_modules(_py, m)?;
        daft_table::register_modules(_py, m)?;
        daft_io::register_modules(_py, m)?;
        daft_parquet::register_modules(_py, m)?;
        daft_csv::register_modules(_py, m)?;
        daft_json::register_modules(_py, m)?;
        daft_plan::register_modules(_py, m)?;
        daft_micropartition::register_modules(_py, m)?;
        daft_scan::register_modules(_py, m)?;
        daft_scheduler::register_modules(_py, m)?;
        daft_sql::register_modules(_py, m)?;
        daft_functions::register_modules(_py, m)?;
        m.add_wrapped(wrap_pyfunction!(version))?;
        m.add_wrapped(wrap_pyfunction!(build_type))?;
        m.add_wrapped(wrap_pyfunction!(refresh_logger))?;
        m.add_wrapped(wrap_pyfunction!(test_logging))?;
        Ok(())
    }
}
