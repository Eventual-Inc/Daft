/// A Table in a Data Catalog
///
/// This is a trait because there are many different implementations of this, for example
/// Iceberg, DeltaLake, Hive and more.
pub trait DataCatalogTable {}
