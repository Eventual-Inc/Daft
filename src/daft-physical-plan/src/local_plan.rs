enum LocalPhysicalPlan {
    // InMemoryScan(InMemoryScan),
    PhysicalScan(PhysicalScan),
    // EmptyScan(EmptyScan),
    Project(Project),
    Filter(Filter),
    Limit(Limit),
    // Explode(Explode),
    // Unpivot(Unpivot),
    Sort(Sort),
    // Split(Split),
    // Sample(Sample),
    // MonotonicallyIncreasingId(MonotonicallyIncreasingId),
    // Coalesce(Coalesce),
    // Flatten(Flatten),
    // FanoutRandom(FanoutRandom),
    // FanoutByHash(FanoutByHash),
    // #[allow(dead_code)]
    // FanoutByRange(FanoutByRange),
    // ReduceMerge(ReduceMerge),
    Aggregate(Aggregate),
    // Pivot(Pivot),
    // Concat(Concat),
    HashJoin(HashJoin),
    // SortMergeJoin(SortMergeJoin),
    // BroadcastJoin(BroadcastJoin),
    PhysicalWrite(PhysicalWrite),
    // TabularWriteJson(TabularWriteJson),
    // TabularWriteCsv(TabularWriteCsv),
    // #[cfg(feature = "python")]
    // IcebergWrite(IcebergWrite),
    // #[cfg(feature = "python")]
    // DeltaLakeWrite(DeltaLakeWrite),
    // #[cfg(feature = "python")]
    // LanceWrite(LanceWrite),
}


struct PhysicalScan {}
struct Project {}
struct Filter {}
struct Limit {}
struct Sort {}
struct Aggregate {}
struct HashJoin {}
struct PhysicalWrite {}