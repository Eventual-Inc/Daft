use daft_core::utils::display::display_date32;
use hudi::table::Table as HudiTable;
pub async fn dummy_api() {
    display_date32(1);

    let t = HudiTable::new("/tmp/hudi_table").await;
    match t {
        Ok(_) => println!("Hudi table created successfully."),
        Err(e) => eprintln!("Failed to create Hudi table: {}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_dummy_api() {
        dummy_api().await;
    }
}
