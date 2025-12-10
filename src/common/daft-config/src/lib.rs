pub use common_io_config::IOConfig;
use serde::{Deserialize, Serialize};

/// Resolve and parse the boolean configuration item from the Env. It's considered true when the
/// value is "1" or "true"; otherwise, it is false.
fn parse_bool_from_env(env_var_name: &str) -> Option<bool> {
    if let Ok(val) = std::env::var(env_var_name) {
        Some(matches!(val.trim().to_lowercase().as_str(), "1" | "true"))
    } else {
        None
    }
}

/// Resolve and parse the string configuration item from the Env.
fn parse_string_from_env(env_var: &str, trim: bool) -> Option<String> {
    if let Ok(val) = std::env::var(env_var) {
        Some(if trim { val.trim().to_string() } else { val })
    } else {
        None
    }
}

/// Resolve and parse the numeric-type configuration item from the Env. If parsing fails, print an
/// error message and return the default value.
fn parse_number_from_env<T: std::str::FromStr + std::fmt::Display>(
    env_var: &str,
    default_val: T,
) -> Option<T> {
    parse_number_from_env_with_custom_parser(env_var, default_val, |_| None)
}

fn parse_number_from_env_with_custom_parser<T: std::str::FromStr + std::fmt::Display>(
    env_var: &str,
    default_val: T,
    custom_parser: impl FnOnce(&str) -> Option<T>,
) -> Option<T> {
    if let Ok(val) = std::env::var(env_var) {
        if let Some(parsed) = custom_parser(&val) {
            return Some(parsed);
        }

        if let Ok(parsed) = val.trim().parse::<T>() {
            return Some(parsed);
        }

        eprintln!(
            "Invalid {} value: {}, using default {}",
            env_var, val, default_val
        );
        return Some(default_val);
    }
    None
}

/// Configurations for Daft to use during the building of a Dataframe's plan.
///
/// 1. Creation of a Dataframe including any file listing and schema inference that needs to happen. Note
///    that this does not include the actual scan, which is taken care of by the DaftExecutionConfig.
/// 2. Building of logical plan nodes
#[derive(Clone, Serialize, Deserialize, Default, Eq, PartialEq)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct DaftPlanningConfig {
    pub default_io_config: IOConfig,
    pub disable_join_reordering: bool,
    pub enable_strict_filter_pushdown: bool,
}

#[cfg(not(debug_assertions))]
impl std::fmt::Debug for DaftPlanningConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DaftPlanningConfig").finish()
    }
}

impl DaftPlanningConfig {
    const ENV_DAFT_DEV_DISABLE_JOIN_REORDERING: &'static str = "DAFT_DEV_DISABLE_JOIN_REORDERING";
    const ENV_DAFT_DEV_ENABLE_STRICT_FILTER_PUSHDOWN: &'static str =
        "DAFT_DEV_ENABLE_STRICT_FILTER_PUSHDOWN";

    #[must_use]
    pub fn from_env() -> Self {
        let mut cfg: Self = Default::default();

        if let Some(val) = parse_bool_from_env(Self::ENV_DAFT_DEV_DISABLE_JOIN_REORDERING) {
            cfg.disable_join_reordering = val;
        }

        if let Some(val) = parse_bool_from_env(Self::ENV_DAFT_DEV_ENABLE_STRICT_FILTER_PUSHDOWN) {
            cfg.enable_strict_filter_pushdown = val;
        }

        cfg
    }
}

/// Configurations for Daft to use during the execution of a Dataframe
///  Note that this should be immutable for a given end-to-end execution of a logical plan.
///
/// Execution entails everything that happens when a Dataframe `.collect()`, `.show()` or similar is called:
/// 1. Logical plan optimization
/// 2. Logical-to-physical-plan translation
/// 3. Task generation from physical plan
/// 4. Task scheduling
/// 5. Task local execution
#[derive(Clone, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct DaftExecutionConfig {
    pub enable_scan_task_split_and_merge: bool,
    pub scan_tasks_min_size_bytes: usize,
    pub scan_tasks_max_size_bytes: usize,
    pub max_sources_per_scan_task: usize,
    pub parquet_split_row_groups_max_files: usize,
    pub broadcast_join_size_bytes_threshold: usize,
    pub hash_join_partition_size_leniency: f64,
    pub sample_size_for_sort: usize,
    pub num_preview_rows: usize,
    pub parquet_target_filesize: usize,
    pub parquet_target_row_group_size: usize,
    pub parquet_inflation_factor: f64,
    pub csv_target_filesize: usize,
    pub csv_inflation_factor: f64,
    pub json_target_filesize: usize,
    pub json_inflation_factor: f64,
    pub shuffle_aggregation_default_partitions: usize,
    pub partial_aggregation_threshold: usize,
    pub high_cardinality_aggregation_threshold: f64,
    pub read_sql_partition_size_bytes: usize,
    pub default_morsel_size: usize,
    pub shuffle_algorithm: String,
    pub pre_shuffle_merge_threshold: usize,
    pub scantask_max_parallel: usize,
    pub native_parquet_writer: bool,
    pub min_cpu_per_task: f64,
    pub actor_udf_ready_timeout: usize,
    pub maintain_order: bool,
    pub enable_dynamic_batching: bool,
    pub dynamic_batching_strategy: String,
}

#[cfg(not(debug_assertions))]
impl std::fmt::Debug for DaftExecutionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DaftExecutionConfig").finish()
    }
}

impl Default for DaftExecutionConfig {
    fn default() -> Self {
        Self {
            enable_scan_task_split_and_merge: false,
            scan_tasks_min_size_bytes: 96 * 1024 * 1024, // 96MB
            scan_tasks_max_size_bytes: 384 * 1024 * 1024, // 384MB
            max_sources_per_scan_task: 10,
            parquet_split_row_groups_max_files: 10,
            broadcast_join_size_bytes_threshold: 10 * 1024 * 1024, // 10 MiB
            hash_join_partition_size_leniency: 0.5,
            sample_size_for_sort: 20,
            num_preview_rows: 8,
            parquet_target_filesize: 512 * 1024 * 1024, // 512MB
            parquet_target_row_group_size: 128 * 1024 * 1024, // 128MB
            parquet_inflation_factor: 3.0,
            csv_target_filesize: 512 * 1024 * 1024, // 512MB
            csv_inflation_factor: 0.5,
            json_target_filesize: 512 * 1024 * 1024, // 512MB
            json_inflation_factor: 0.25,
            shuffle_aggregation_default_partitions: 200,
            partial_aggregation_threshold: 10000,
            high_cardinality_aggregation_threshold: 0.8,
            read_sql_partition_size_bytes: 512 * 1024 * 1024, // 512MB
            default_morsel_size: 128 * 1024,
            shuffle_algorithm: "auto".to_string(),
            pre_shuffle_merge_threshold: 1024 * 1024 * 1024, // 1GB
            scantask_max_parallel: 8,
            native_parquet_writer: true,
            min_cpu_per_task: 0.5,
            actor_udf_ready_timeout: 120,
            maintain_order: true,
            enable_dynamic_batching: false,
            dynamic_batching_strategy: "auto".to_string(),
        }
    }
}

impl DaftExecutionConfig {
    const ENV_DAFT_SHUFFLE_ALGORITHM: &'static str = "DAFT_SHUFFLE_ALGORITHM";
    const ENV_DAFT_SCANTASK_MAX_PARALLEL: &'static str = "DAFT_SCANTASK_MAX_PARALLEL";
    const ENV_DAFT_NATIVE_PARQUET_WRITER: &'static str = "DAFT_NATIVE_PARQUET_WRITER";
    const ENV_DAFT_MIN_CPU_PER_TASK: &'static str = "DAFT_MIN_CPU_PER_TASK";
    const ENV_DAFT_ACTOR_UDF_READY_TIMEOUT: &'static str = "DAFT_ACTOR_UDF_READY_TIMEOUT";
    const ENV_PARQUET_INFLATION_FACTOR: &'static str = "DAFT_PARQUET_INFLATION_FACTOR";
    const ENV_CSV_INFLATION_FACTOR: &'static str = "DAFT_CSV_INFLATION_FACTOR";
    const ENV_JSON_INFLATION_FACTOR: &'static str = "DAFT_JSON_INFLATION_FACTOR";
    const ENV_DAFT_MAINTAIN_ORDER: &'static str = "DAFT_MAINTAIN_ORDER";

    #[must_use]
    pub fn from_env() -> Self {
        let mut cfg = Self::default();

        if let Some(val) = parse_string_from_env(Self::ENV_DAFT_SHUFFLE_ALGORITHM, true) {
            cfg.shuffle_algorithm = val;
        }

        if let Some(val) = parse_number_from_env_with_custom_parser(
            Self::ENV_DAFT_SCANTASK_MAX_PARALLEL,
            cfg.scantask_max_parallel,
            |v| {
                if v == "auto" { Some(0) } else { None }
            },
        ) {
            cfg.scantask_max_parallel = val;
        }

        if let Some(val) = parse_bool_from_env(Self::ENV_DAFT_NATIVE_PARQUET_WRITER) {
            cfg.native_parquet_writer = val;
        }

        if let Some(val) =
            parse_number_from_env(Self::ENV_DAFT_MIN_CPU_PER_TASK, cfg.min_cpu_per_task)
        {
            cfg.min_cpu_per_task = val;
        }

        if let Some(val) = parse_number_from_env(
            Self::ENV_DAFT_ACTOR_UDF_READY_TIMEOUT,
            cfg.actor_udf_ready_timeout,
        ) {
            cfg.actor_udf_ready_timeout = val;
        }

        if let Some(val) = parse_bool_from_env(Self::ENV_DAFT_MAINTAIN_ORDER) {
            cfg.maintain_order = val;
        }

        if let Some(val) = parse_number_from_env(
            Self::ENV_PARQUET_INFLATION_FACTOR,
            cfg.parquet_inflation_factor,
        ) {
            cfg.parquet_inflation_factor = val;
        }

        if let Some(val) =
            parse_number_from_env(Self::ENV_CSV_INFLATION_FACTOR, cfg.csv_inflation_factor)
        {
            cfg.csv_inflation_factor = val;
        }

        if let Some(val) =
            parse_number_from_env(Self::ENV_JSON_INFLATION_FACTOR, cfg.json_inflation_factor)
        {
            cfg.json_inflation_factor = val;
        }

        cfg
    }
}

#[cfg(feature = "python")]
mod python;

#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
pub use python::PyDaftExecutionConfig;
#[cfg(feature = "python")]
pub use python::PyDaftPlanningConfig;

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<python::PyDaftExecutionConfig>()?;
    parent.add_class::<python::PyDaftPlanningConfig>()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{DaftExecutionConfig, DaftPlanningConfig};

    #[test]
    fn test_from_env_for_planning_config() {
        // ENV_DAFT_DEV_DISABLE_JOIN_REORDERING
        {
            let cfg = DaftPlanningConfig::from_env();
            assert_eq!(cfg.disable_join_reordering, false);

            unsafe {
                std::env::set_var(
                    DaftPlanningConfig::ENV_DAFT_DEV_DISABLE_JOIN_REORDERING,
                    "1",
                );
            }
            let cfg = DaftPlanningConfig::from_env();
            assert_eq!(cfg.disable_join_reordering, true);

            unsafe {
                std::env::set_var(
                    DaftPlanningConfig::ENV_DAFT_DEV_DISABLE_JOIN_REORDERING,
                    "false",
                );
            }
            let cfg = DaftPlanningConfig::from_env();
            assert_eq!(cfg.disable_join_reordering, false);

            unsafe {
                std::env::remove_var(DaftPlanningConfig::ENV_DAFT_DEV_DISABLE_JOIN_REORDERING);
            }
        }

        // ENV_DAFT_DEV_ENABLE_STRICT_FILTER_PUSHDOWN
        {
            let cfg = DaftPlanningConfig::from_env();
            assert_eq!(cfg.enable_strict_filter_pushdown, false);

            unsafe {
                std::env::set_var(
                    DaftPlanningConfig::ENV_DAFT_DEV_ENABLE_STRICT_FILTER_PUSHDOWN,
                    "0",
                );
            }
            let cfg = DaftPlanningConfig::from_env();
            assert_eq!(cfg.enable_strict_filter_pushdown, false);

            unsafe {
                std::env::set_var(
                    DaftPlanningConfig::ENV_DAFT_DEV_ENABLE_STRICT_FILTER_PUSHDOWN,
                    "true",
                );
            }
            let cfg = DaftPlanningConfig::from_env();
            assert_eq!(cfg.enable_strict_filter_pushdown, true);

            unsafe {
                std::env::remove_var(
                    DaftPlanningConfig::ENV_DAFT_DEV_ENABLE_STRICT_FILTER_PUSHDOWN,
                );
            }
        }
    }

    #[test]
    fn test_from_env_for_execution_config() {
        // ENV_DAFT_SHUFFLE_ALGORITHM
        {
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.shuffle_algorithm, "auto");

            unsafe {
                std::env::set_var(
                    DaftExecutionConfig::ENV_DAFT_SHUFFLE_ALGORITHM,
                    "pre_shuffle_merge  ",
                );
            }
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.shuffle_algorithm, "pre_shuffle_merge");

            unsafe {
                std::env::set_var(
                    DaftExecutionConfig::ENV_DAFT_SHUFFLE_ALGORITHM,
                    "map_reduce  ",
                );
            }
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.shuffle_algorithm, "map_reduce");

            unsafe {
                std::env::remove_var(DaftExecutionConfig::ENV_DAFT_SHUFFLE_ALGORITHM);
            }
        }

        // ENV_DAFT_SCANTASK_MAX_PARALLEL
        {
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.scantask_max_parallel, 8);

            unsafe {
                std::env::set_var(DaftExecutionConfig::ENV_DAFT_SCANTASK_MAX_PARALLEL, "16");
            }
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.scantask_max_parallel, 16);

            unsafe {
                std::env::set_var(DaftExecutionConfig::ENV_DAFT_SCANTASK_MAX_PARALLEL, "auto");
            }
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.scantask_max_parallel, 0);

            unsafe {
                std::env::set_var(
                    DaftExecutionConfig::ENV_DAFT_SCANTASK_MAX_PARALLEL,
                    "invalid",
                );
            }
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.scantask_max_parallel, 8);

            unsafe {
                std::env::remove_var(DaftExecutionConfig::ENV_DAFT_SCANTASK_MAX_PARALLEL);
            }
        }

        // ENV_DAFT_NATIVE_PARQUET_WRITER
        {
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.native_parquet_writer, true);

            unsafe {
                std::env::set_var(DaftExecutionConfig::ENV_DAFT_NATIVE_PARQUET_WRITER, "false");
            }
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.native_parquet_writer, false);

            unsafe {
                std::env::set_var(DaftExecutionConfig::ENV_DAFT_NATIVE_PARQUET_WRITER, "1");
            }
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.native_parquet_writer, true);

            unsafe {
                std::env::remove_var(DaftExecutionConfig::ENV_DAFT_NATIVE_PARQUET_WRITER);
            }
        }

        // ENV_DAFT_MIN_CPU_PER_TASK
        {
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.min_cpu_per_task, 0.5);

            unsafe {
                std::env::set_var(DaftExecutionConfig::ENV_DAFT_MIN_CPU_PER_TASK, "0.1");
            }
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.min_cpu_per_task, 0.1);

            unsafe {
                std::env::set_var(DaftExecutionConfig::ENV_DAFT_MIN_CPU_PER_TASK, "invalid");
            }
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.min_cpu_per_task, 0.5);

            unsafe {
                std::env::remove_var(DaftExecutionConfig::ENV_DAFT_MIN_CPU_PER_TASK);
            }
        }

        // ENV_DAFT_ACTOR_UDF_READY_TIMEOUT
        {
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.actor_udf_ready_timeout, 120);

            unsafe {
                std::env::set_var(DaftExecutionConfig::ENV_DAFT_ACTOR_UDF_READY_TIMEOUT, "300");
            }
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.actor_udf_ready_timeout, 300);

            unsafe {
                std::env::set_var(
                    DaftExecutionConfig::ENV_DAFT_ACTOR_UDF_READY_TIMEOUT,
                    "invalid",
                );
            }
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.actor_udf_ready_timeout, 120);

            unsafe {
                std::env::remove_var(DaftExecutionConfig::ENV_DAFT_ACTOR_UDF_READY_TIMEOUT);
            }
        }

        // ENV_DAFT_MAINTAIN_ORDER
        {
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.maintain_order, true);

            unsafe {
                std::env::set_var(DaftExecutionConfig::ENV_DAFT_MAINTAIN_ORDER, "false");
            }
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.maintain_order, false);

            unsafe {
                std::env::set_var(DaftExecutionConfig::ENV_DAFT_MAINTAIN_ORDER, "1");
            }
            let cfg = DaftExecutionConfig::from_env();
            assert_eq!(cfg.maintain_order, true);

            unsafe {
                std::env::remove_var(DaftExecutionConfig::ENV_DAFT_MAINTAIN_ORDER);
            }
        }
    }
}
