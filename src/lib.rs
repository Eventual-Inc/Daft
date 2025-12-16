#![allow(clippy::useless_conversion)]

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
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: Option<&'static libc::c_char> = Some(unsafe {
    U {
        x: &b"oversize_threshold:1,background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000\0"[0],
    }
    .y
});

#[cfg(target_os = "macos")]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: Option<&'static libc::c_char> = Some(unsafe {
    U {
        x: &b"oversize_threshold:1,background_thread:false,dirty_decay_ms:0,muzzy_decay_ms:0\0"[0],
    }
    .y
});

#[cfg(feature = "python")]
pub mod pylib {
    use std::sync::LazyLock;

    use common_logging::GLOBAL_LOGGER;
    use common_tracing::init_opentelemetry_providers;
    use pyo3::prelude::*;

    static LOG_RESET_HANDLE: LazyLock<pyo3_log::ResetHandle> = LazyLock::new(|| {
        let py_logger = Box::new(pyo3_log::Logger::default());
        let handle = py_logger.reset_handle();

        GLOBAL_LOGGER.set_base_logger(py_logger);
        log::set_boxed_logger(Box::new(GLOBAL_LOGGER.clone())).unwrap();
        handle
    });

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
        // If the `DAFT_LOG` environment variable is set, it will be used to set the log level.
        // Otherwise, the log level will be set to the level of the Python logger.
        let env_level = std::env::var("DAFT_LOG")
            .ok()
            .and_then(|v| v.parse::<LevelFilter>().ok());

        let level_filter = if let Some(level) = env_level {
            level
        } else {
            let logging = py.import(pyo3::intern!(py, "logging"))?;
            let python_log_level = logging
                .getattr(pyo3::intern!(py, "getLogger"))?
                .call0()?
                .getattr(pyo3::intern!(py, "level"))?
                .extract::<usize>()
                .unwrap_or(0);

            match python_log_level {
                0 => LevelFilter::Off,
                1..=10 => LevelFilter::Debug,
                11..=20 => LevelFilter::Info,
                21..=30 => LevelFilter::Warn,
                31..=40 => LevelFilter::Error,
                _ => LevelFilter::Error, // CRITICAL or anything above also becomes Error
            }
        };

        LOG_RESET_HANDLE.reset();
        log::set_max_level(level_filter);
        Ok(())
    }

    #[pyfunction]
    pub fn set_compute_runtime_num_worker_threads(num_threads: usize) -> PyResult<()> {
        common_runtime::set_compute_runtime_num_worker_threads(num_threads)?;
        Ok(())
    }

    #[pymodule]
    fn daft(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
        refresh_logger(py)?;
        init_opentelemetry_providers();

        common_daft_config::register_modules(m)?;
        common_system_info::register_modules(m)?;
        common_resource_request::register_modules(m)?;
        common_file_formats::python::register_modules(m)?;
        common_scan_info::register_modules(m)?;
        common_metrics::register_modules(m)?;
        daft_ai::register_modules(m)?;
        daft_catalog::register_modules(m)?;
        daft_context::register_modules(m)?;
        daft_runners::register_modules(m)?;
        daft_core::register_modules(m)?;
        daft_core::python::register_modules(m)?;
        daft_csv::register_modules(m)?;
        daft_distributed::register_modules(m)?;
        daft_dsl::register_modules(m)?;
        daft_functions::register_modules(m)?;
        daft_io::register_modules(m)?;
        daft_json::register_modules(m)?;
        daft_local_execution::register_modules(m)?;
        daft_local_plan::register_modules(m)?;
        daft_logical_plan::register_modules(m)?;
        daft_parquet::register_modules(m)?;
        daft_micropartition::register_modules(m)?;
        daft_recordbatch::register_modules(m)?;
        daft_scan::register_modules(m)?;
        daft_session::register_modules(m)?;
        daft_sql::register_modules(m)?;
        daft_shuffles::python::register_modules(m)?;
        daft_file::python::register_modules(m)?;
        // Register testing module
        let testing_module = PyModule::new(m.py(), "testing")?;
        m.add_submodule(&testing_module)?;
        daft_scan::python::register_testing_modules(&testing_module)?;

        m.add_wrapped(wrap_pyfunction!(version))?;
        m.add_wrapped(wrap_pyfunction!(build_type))?;
        m.add_wrapped(wrap_pyfunction!(refresh_logger))?;
        m.add_wrapped(wrap_pyfunction!(get_max_log_level))?;
        m.add_wrapped(wrap_pyfunction!(set_compute_runtime_num_worker_threads))?;

        daft_dashboard::register_modules(m)?;
        daft_cli::register_modules(m)?;

        // We need to do this here because it's the only point in the rust codebase that we have access to all crates.
        let mut functions_registry = daft_dsl::functions::FUNCTION_REGISTRY
            .write()
            .expect("Failed to acquire write lock on function registry");
        functions_registry.register::<daft_functions::numeric::NumericFunctions>();
        functions_registry.register::<daft_functions::float::FloatFunctions>();
        functions_registry.register::<daft_functions_uri::UriFunctions>();
        functions_registry.register::<daft_image::functions::ImageFunctions>();
        functions_registry.register::<daft_functions_binary::BinaryFunctions>();
        functions_registry.register::<daft_functions_list::ListFunctions>();
        functions_registry.register::<daft_functions_utf8::Utf8Functions>();
        functions_registry.register::<daft_functions_json::JsonFunctions>();
        functions_registry.register::<daft_functions_serde::SerdeFunctions>();
        functions_registry.register::<daft_functions_temporal::TemporalFunctions>();
        functions_registry.register::<daft_functions::MiscFunctions>();
        functions_registry.register::<daft_functions::distance::DistanceFunctions>();
        functions_registry.register::<daft_functions_tokenize::TokenizeFunctions>();

        functions_registry.add_fn(daft_functions::coalesce::Coalesce);
        functions_registry.add_fn(daft_file::File);
        functions_registry.add_fn(daft_file::Size);
        functions_registry.add_fn(daft_file::VideoFile);
        functions_registry.add_fn(daft_file::AudioFile);
        functions_registry
            .add_fn(daft_functions::monotonically_increasing_id::MonotonicallyIncreasingId);
        functions_registry.register::<daft_functions::distance::DistanceFunctions>();

        Ok(())
    }
}
