fn main() -> std::io::Result<()> {
    tonic_build::configure().build_server(true).compile_protos(
        &[
            "proto/spark/connect/base.proto",
            "proto/spark/connect/catalog.proto",
            "proto/spark/connect/commands.proto",
            "proto/spark/connect/common.proto",
            "proto/spark/connect/expressions.proto",
            "proto/spark/connect/relations.proto",
            "proto/spark/connect/types.proto",
        ],
        &["proto"],
    )?;
    Ok(())
}
