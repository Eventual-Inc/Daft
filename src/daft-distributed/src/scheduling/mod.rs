pub(super) mod dispatcher;
// `local_worker` executes tasks via `daft_local_execution::execute_local_plan`,
// which is only available without the python feature. Rust test runs use
// `--no-default-features`, so this works for tests; skip under `--all-features`.
#[cfg(all(test, not(feature = "python")))]
pub(crate) mod local_worker;
pub(super) mod scheduler;
pub(crate) mod task;

#[cfg(test)]
pub(crate) mod tests;
pub(crate) mod worker;
