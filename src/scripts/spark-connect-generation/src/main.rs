/// Based on definition of [`tonic::include_proto`]
const GENERATED_CODE: &str = include_str!(concat!(
    env!("OUT_DIR"),
    concat!("/", "spark.connect", ".rs")
));

fn main() {
    println!("{GENERATED_CODE}");
}
