use daft_dsl::functions::FunctionModule;

pub mod cosine;
pub mod jaccard;
pub mod pearson;

pub use cosine::cosine_similarity;
pub use jaccard::jaccard_similarity;
pub use pearson::pearson_correlation;

pub struct SimilarityFunctions;

impl FunctionModule for SimilarityFunctions {
    fn register(parent: &mut daft_dsl::functions::FunctionRegistry) {
        parent.add_fn(cosine::CosineSimilarityFunction);
        parent.add_fn(pearson::PearsonCorrelationFunction);
        parent.add_fn(jaccard::JaccardSimilarityFunction);
    }
}
