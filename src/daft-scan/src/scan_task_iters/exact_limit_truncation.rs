use super::BoxScanTaskIter;

#[must_use]
pub(crate) fn exact_limit_truncation(
    scan_tasks: BoxScanTaskIter<'_>,
    limit: usize,
) -> BoxScanTaskIter<'_> {
    let mut exact_min_num_rows_read = 0;
    Box::new(scan_tasks.into_iter().take_while(move |st| {
        let old_exact_min_num_rows_read = exact_min_num_rows_read;
        if let Ok(st) = st {
            exact_min_num_rows_read += st.num_rows().unwrap_or(0);
        }
        old_exact_min_num_rows_read < limit
    }))
}
