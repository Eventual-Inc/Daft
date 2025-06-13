# Roadmap

_Last updated: May 2025_

What is in store for Daft in 2025? This roadmap outlines the big picture of what the Daft team plans to work on in the coming year, as well as some of the features to expect from these.

Please note that items on this roadmap are subject to change any time. If there are features you would like to implement, we highly welcome and encourage open source contributions! Our team is happy to provide guidance, help scope the work, and review PRs. Feel free to open an issue or PR on [Github](https://github.com/Eventual-Inc/Daft) or join our [Daft Slack Community](https://join.slack.com/t/dist-data/shared_invite/zt-2e77olvxw-uyZcPPV1SRchhi8ah6ZCtg).

<!-- See also [Contribute to Daft](contributing.md) for information. -->

## Multimodality

- Support generic data source and data sink interfaces that can be implemented outside of Daft
- Enhanced support for JSON with a VARIANT data type and JSON_TABLE
- More in-built and optimized expressions for multimodal and nested datatypes

## AI

- Higher level abstractions for building AI applications on top of Daft
- Better AI-specific observability and metrics in AI functions
    - Tokens per second
    - Estimated API costs
- Better primitives for AI workloads ([discussion #3547](https://github.com/Eventual-Inc/Daft/discussions/3547))
    - Async UDFs
    - Streaming UDFs
- Native LLM inference functions with Pydantic integration ([discussion #2774](https://github.com/Eventual-Inc/Daft/discussions/2774))

## Performance & Scalability

- Incorporate our local streaming execution engine (Swordfish) into distributed ray runner
    - Handle Map-only workloads at any scale factor (100TB+)
    - Handle 10TB+ Shuffle workloads
- More powerful cost-based optimizer, implementing advanced optimizations
    - Improve the join ordering algorithm to be dynamic-programming based
    - Semi-join reduction
    - Common subquery elimination
- To complement our blazing fast S3 readers, we aim to build the fastest S3 writes in the wild west

## Out-of-the-box Experience

- Continue expanding feature set and compatibility of [Daft’s PySpark connector](spark_connect.md#show) so that running Spark workloads on Daft is a simple plug-and-play ([issue #3581](https://github.com/Eventual-Inc/Daft/issues/3581))
    - Ordinal column references ([issue #4270](https://github.com/Eventual-Inc/Daft/issues/4270))
    - Window function support ([issue #2108](https://github.com/Eventual-Inc/Daft/issues/2108))
- Improve catalog and table integrations
    - Support for Iceberg deletion vectors and upserts (see [roadmap for Iceberg](https://github.com/Eventual-Inc/Daft/issues/2458))
    - Better Unity Catalog support ([issue #2482](https://github.com/Eventual-Inc/Daft/issues/2482))
- Improve observability tools (logging/metrics/traces) ([issue #4380](https://github.com/Eventual-Inc/Daft/issues/4380))
- Improve experience working with AI tools
    - LLM context file ([issue #4293](https://github.com/Eventual-Inc/Daft/issues/4293))

## Future Work

The following features would be valuable additions to Daft, but are not currently on our immediate development roadmap. We're sharing these to highlight opportunities for open source contributions, invite discussion around implementation approaches, and provide visibility into longer-term possibilities. These features have been tagged with `help wanted` and `good first issue` on [Daft repo](https://github.com/Eventual-Inc/Daft).

- Improved Delta Lake support (see [roadmap for Delta Lake](https://github.com/Eventual-Inc/Daft/issues/2457))
    - Support for reading tables with deletion vectors ([issue #1954](https://github.com/Eventual-Inc/Daft/issues/1954))
    - Support for reading tables with column mappings ([issue #1955](https://github.com/Eventual-Inc/Daft/issues/1955))
- Improved Apache Hudi support (see [roadmap for Apache Hudi](https://github.com/Eventual-Inc/Daft/issues/4389))
- Expressions parity with PySpark: Temporal ([issue #3798](https://github.com/Eventual-Inc/Daft/issues/3798)), Math ([issue #3793](https://github.com/Eventual-Inc/Daft/issues/3793)), String ([issue #3792](https://github.com/Eventual-Inc/Daft/issues/3792))

If you are interested in working on any of these features, feel free to open an issue or start a discussion on [Github](https://github.com/Eventual-Inc/Daft) or join our [Daft Slack Community](https://join.slack.com/t/dist-data/shared_invite/zt-2e77olvxw-uyZcPPV1SRchhi8ah6ZCtg). Our team can provide technical direction and help scope the work appropriately. Thank you in advance 💜

<!-- See also [Contribute to Daft](contributing.md) for information. -->
