# daft-ir

Consolidated crate for daft's IR e.g. daft-dsl, daft-logical-plan, daft-schema, and various common packages.

## TODOs

- rex: subqueries (should be eliminated though?)
- rex: window functions
- rel: explode
- rel: unpivot
- rel: sort
- rel: repartition
- rel: join
- rel: sink
- rel: sample
- rel: window
- rel: top_n
- chore: the goal is one big flat namespace to simplify IR consumption.
- chore: pushdowns and partitioning to their own crates.
- chore: source and scan types consolidated.
