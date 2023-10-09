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

#[cfg(feature = "python")]
pub mod pylib {
    use pyo3::prelude::*;

    #[pyfunction]
    pub fn version() -> &'static str {
        daft_core::VERSION
    }

    #[pyfunction]
    pub fn build_type() -> &'static str {
        daft_core::DAFT_BUILD_TYPE
    }

    #[pymodule]
    fn daft(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
        pyo3_log::init();

        daft_core::register_modules(_py, m)?;
        daft_core::python::register_modules(_py, m)?;
        daft_dsl::register_modules(_py, m)?;
        daft_table::register_modules(_py, m)?;
        daft_io::register_modules(_py, m)?;
        daft_parquet::register_modules(_py, m)?;
        daft_csv::register_modules(_py, m)?;
        daft_plan::register_modules(_py, m)?;

        m.add_wrapped(wrap_pyfunction!(version))?;
        m.add_wrapped(wrap_pyfunction!(build_type))?;
        Ok(())
    }
}
