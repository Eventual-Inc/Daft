# Flotilla Distributed Repartition Flow

```mermaid
flowchart TD
    A[LogicalPlan::Repartition\nRepartitionSpec] --> B{Spec Type}

    B -->|Hash / Random / Range| C[Translator::gen_shuffle_node\nresolve target num_partitions]
    C --> D{shuffle_algorithm}
    D -->|auto / pre_shuffle_merge| E[Optional PreShuffleMergeNode]
    D -->|map_reduce| F[Skip PreShuffleMerge]
    E --> G[RepartitionNode]
    F --> G

    G --> H[produce_tasks: pipeline LocalPhysicalPlan::repartition]
    H --> I[Local RepartitionSink per upstream task]
    I --> I1[Hash: partition_by_hash]
    I --> I2[Random: partition_by_random]
    I --> I3[Range: partition_by_range]

    I1 --> J[Each upstream task emits vector of N local buckets]
    I2 --> J
    I3 --> J

    J --> K[transpose_materialized_outputs_from_stream\ngroup by bucket index]
    K --> L[For each final partition i:\nmerge all task contributions]
    L --> M[MaterializedOutput::into_in_memory_scan_with_psets]
    M --> N[Emit 1 downstream task per final partition]
    N --> O[Next distributed operator]

    B -->|IntoPartitions| P[IntoPartitionsNode path]
    P --> Q[IntoPartitionsSink\nconcat all rows, slice evenly]
    Q --> R[Emit N partitions to downstream]

    classDef logical fill:#16313f,stroke:#70a7c4,color:#e8f4fb;
    classDef physical fill:#1c3d31,stroke:#73be8f,color:#ecfff3;
    classDef shuffle fill:#3b2f1b,stroke:#f0c36d,color:#fff4db;
    classDef alt fill:#3b2238,stroke:#d591d0,color:#ffeefe;

    class A,B logical;
    class C,D,E,F,G,H,I,J,K,L,M,N,O shuffle;
    class I1,I2,I3 physical;
    class P,Q,R alt;
```

## Notes

- For `Hash` / `Random` / `Range`, Flotilla executes a distributed shuffle with transpose+merge.
- The transpose step is why output has exactly `N` downstream tasks: one task per final partition index.
- `IntoPartitions` does **not** go through `RepartitionSink` in this distributed path; it has its own node/sink.
