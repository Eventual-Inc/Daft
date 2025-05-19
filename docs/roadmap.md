# Roadmap

_Last updated: May 2025_

What is in store for Daft in 2025? This roadmap outlines the big picture of what the Daft team plans to work on in the coming year, as well as some of the features to expect from these.

## Multimodality

- Support generic data source and data sink interfaces that can be implemented outside of Daft
- Enhanced support for JSON with a VARIANT data type and JSON_TABLE
- More in-built and optimized expressions for multimodal and nested datatypes

## AI

- Higher level abstractions for building AI applications on top of Daft
- Better AI-specific observability and metrics in AI functions
    - Tokens per second
    - Estimated API costs
- Better primitives for AI workloads (see [discussion](https://github.com/Eventual-Inc/Daft/discussions/3547))
    - Async UDFs
    - Streaming UDFs
- Native LLM inference functions with Pydantic integration (see [discussion](https://github.com/Eventual-Inc/Daft/discussions/2774))

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

- Continue expanding feature set and compatibility of [Daft’s PySpark connector](spark_connect.md#show) so that running Spark workloads on Daft is a simple plug-and-play (see [issue](https://github.com/Eventual-Inc/Daft/issues/3581))
    - Ordinal column references (see [issue](https://github.com/Eventual-Inc/Daft/issues/4270))
    - Window function support (see [issue](https://github.com/Eventual-Inc/Daft/issues/2108))
- Improve catalog and table integrations
    - Support for Iceberg deletion vectors and upserts (see [roadmap for Iceberg](https://github.com/Eventual-Inc/Daft/issues/2458), [issue for upsert](https://github.com/Eventual-Inc/Daft/issues/3844))
    - Better Unity Catalog support (see [issue](https://github.com/Eventual-Inc/Daft/issues/2482))
    - Better Delta Lake support (see [roadmap for Delta Lake](https://github.com/Eventual-Inc/Daft/issues/2457))
- Improve observability tools (logging/metrics/traces)
- Improve experience working with AI tools
    - LLM context file (see [issue](https://github.com/Eventual-Inc/Daft/issues/4293))

Please note that items on this list are subject to change any time. If there are features missing that you would like to implement, we highly encourage open source contributions! Our team is happy to provide guidance and answer any questions, feel free to open an issue or PR on [Github](https://github.com/Eventual-Inc/Daft) or join our [Daft Slack Community](https://join.slack.com/t/dist-data/shared_invite/zt-2e77olvxw-uyZcPPV1SRchhi8ah6ZCtg).

<!-- See also [Contribute to Daft](contributing.md) for information. -->
