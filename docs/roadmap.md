# Roadmap

*Last updated: March 2026*

This roadmap outlines the big picture of what the Daft team plans to work on in the coming year, as well as some of the features to expect from these.

Please note that items on this roadmap are subject to change at any time. If there are features you would like to implement, we welcome and encourage open-source contributions. Our team is happy to provide guidance, help scope the work, and review PRs.

Feel free to open an issue or PR on [GitHub ↗](https://github.com/Eventual-Inc/Daft) or join our [Daft Slack Community ↗](https://daft.ai/slack).

## Performance

1. **Performant large scale shuffles** - Support for native shuffling services for data transfer in our distributed engine (Flotilla). This includes a specialized peer-to-peer service based on Arrow Flight RPC. ([discussion #6472 ↗](https://github.com/Eventual-Inc/Daft/discussions/6472))
2. **Distributed engine optimization** - Optimizing our distributed engine (Flotilla) to dynamically schedule and execute tasks to minimize worker setup overhead and contention, add dynamic partitioning.
3. **Memory management** - Tracking and managing memory usage within our local runner to reduce chances of OOMs, inform backpressure and dynamic batching.
4. **Hash-based operator improvements** - Improve partitioning and hash-table building techniques within various hash-based operators, particularly groupby aggregations and hash joins in low-cardinality cases.

## Key Features

1. **Checkpointing** - Ability to checkpoint and resume long running workloads from a given checkpoint. ([discussion #6446](https://github.com/Eventual-Inc/Daft/discussions/6446))
2. **Kubernetes support** - Distributed Daft running directly on Kubernetes.
3. **Multiple distributed backends** - Enable Daft to run distributed on multiple backends with a generic interface and continued support of Ray as a backend.

## Observability

1. **Daft Dashboard** - Observability dashboard with support for Flotilla, providing cluster, task and partition level observability.
2. **Improved Debugging** - Support for capturing and retrieving debugging information and run history.
3. **Memory and CPU observability** - Ability to capture and retrieve resource utilization metrics.

## Extensibility

1. **Native extensions** - Support for Daft native extensions via the Arrow C interfaces; enabling developers to extend Daft in any language which supports C-based interop.
2. **Data Source refactor** - Simplify the source pipeline to make adding native Rust sources straightforward.
3. **Arrow2 deprecation** ([discussion #5741](https://github.com/Eventual-Inc/Daft/discussions/5741))


## Future Work

The following features are in consideration, but are not currently on our roadmap. We're sharing these to highlight opportunities for open source contributions, invite discussion around implementation approaches, and provide further visibility. These features have been tagged with `help wanted` and `good first issue` on [Daft repo ↗](https://github.com/Eventual-Inc/Daft).

1. **Improved Delta Lake support** (see [roadmap for Delta Lake ↗](https://github.com/Eventual-Inc/Daft/issues/2457))
    - Support for reading tables with deletion vectors ([issue #1954 ↗](https://github.com/Eventual-Inc/Daft/issues/1954))
    - Support for reading tables with column mappings ([issue #1955 ↗](https://github.com/Eventual-Inc/Daft/issues/1955))
2. **VARIANT type** - Support for the VARIANT type into Daft's type system which is compatible with the parquet VARIANT.
3. **`Result<T>` type** - Add a `Result<T>` type (Either) to Daft's type system to support graceful handling of fallible operations.

If you are interested in working on any of these features, feel free to open an issue or start a discussion on [GitHub ↗](https://github.com/Eventual-Inc/Daft) or join our [Daft Slack Community ↗](https://daft.ai/slack). Our team can provide technical direction and help scope the work appropriately. Thank you in advance 💜
