fn main() -> std::io::Result<()> {
    let mut config = tonic_prost_build::Config::new();

    // todo: having issues with prost_wkt_types on Windows
    // config
    //     .type_attribute(".", "#[derive(serde::Serialize)]")
    //     .extern_path(".google.protobuf.Any", "::prost_wkt_types::Any")
    //     .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
    //     .extern_path(".google.protobuf.Value", "::prost_wkt_types::Value");

    tonic_prost_build::configure()
        .build_server(true)
        .compile_with_config(
            config,
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
