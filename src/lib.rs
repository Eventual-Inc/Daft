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
        common_version::VERSION
    }

    #[pyfunction]
    pub fn build_type() -> &'static str {
        common_version::DAFT_BUILD_TYPE
    }

    #[pyfunction]
    pub fn get_max_log_level() -> &'static str {
        log::max_level().as_str()
    }

    #[pyfunction]
    pub fn refresh_logger(py: Python) -> PyResult<()> {
        use log::LevelFilter;
        let logging = py.import_bound(pyo3::intern!(py, "logging"))?;
        let python_log_level = logging
            .getattr(pyo3::intern!(py, "getLogger"))?
            .call0()?
            .getattr(pyo3::intern!(py, "level"))?
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
    fn daft(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
        refresh_logger(py)?;
        init_tracing(crate::should_enable_chrome_trace());

        common_daft_config::register_modules(m)?;
        common_system_info::register_modules(m)?;
        common_resource_request::register_modules(m)?;
        common_file_formats::python::register_modules(m)?;
        common_scan_info::register_modules(m)?;
        daft_core::register_modules(m)?;
        daft_core::python::register_modules(m)?;
        daft_local_execution::register_modules(m)?;
        daft_dsl::register_modules(m)?;
        daft_table::register_modules(m)?;
        daft_io::register_modules(m)?;
        daft_parquet::register_modules(m)?;
        daft_csv::register_modules(m)?;
        daft_json::register_modules(m)?;
        daft_logical_plan::register_modules(m)?;
        daft_micropartition::register_modules(m)?;
        daft_scan::register_modules(m)?;
        daft_scheduler::register_modules(m)?;
        daft_sql::register_modules(m)?;
        daft_functions::register_modules(m)?;
        daft_functions_json::register_modules(m)?;
        daft_connect::register_modules(m)?;

        // Register catalog module
        let catalog_module = daft_catalog::python::register_modules(m)?;
        daft_catalog_python_catalog::python::register_modules(&catalog_module)?;

        m.add_wrapped(wrap_pyfunction!(version))?;
        m.add_wrapped(wrap_pyfunction!(build_type))?;
        m.add_wrapped(wrap_pyfunction!(refresh_logger))?;
        m.add_wrapped(wrap_pyfunction!(get_max_log_level))?;
        daft_image::python::register_modules(m)?;
        Ok(())
    }
}
