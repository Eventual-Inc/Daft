use common_error::DaftResult;
use daft_micropartition::MicroPartition;

trait Operator: Sync + Send {
    fn name(&self) -> &str;
}


trait Source: Operator {
    async fn stream(&mut self) -> futures::stream::LocalBoxStream<DaftResult<MicroPartition>>;
}
