/// DataCatalog is a catalog of data sources
///
/// It allows registering and retrieving data sources, as well as querying their schemas.
/// The catalog is used by the query planner to resolve table references in SQL queries.
pub trait DataCatalog {}
