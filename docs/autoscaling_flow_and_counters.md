# Flotilla 模式下 Autoscaling 触发流转与计数器（代码视角）

本文从 Daft 代码实现出发，梳理 flotilla 模式下的动态伸缩（autoscaling）链路、计数器使用与日志观测点。版本基线：分支 aime/1761057089-fix-autoscaling-downscale。

## 背景与结论概览

- 当前实现中，扩容通过 DefaultScheduler 的判定返回 autoscaling 请求，RayWorkerManager 聚合后调用 Python 侧 `flotilla.try_autoscale()`，最终调用 `ray.autoscaler.sdk.request_resources`。该 API 仅支持“请求增量”，不会触发缩容。
- 之前的 `downscale_below_threshold_ticks` 虽有计算，但未进入调度判定路径（未被使用），因此不会触发任何缩容动作。
- 修复后在调度主循环中引入“稳定窗口 + 安全回收”的下扩逻辑：当 backlog/capacity 比值低于阈值且持续稳定，按“最长闲置优先”主动 retire 闲置的 RaySwordfishWorker，以驱动 Ray 在节点空闲下执行下扩。

## autoscaling_request：来源、构造与作用（仅扩容）

- 入口与来源：
  - `DefaultScheduler::get_autoscaling_request()` 在满足扩容判定（待调度任务数 ÷ 总 CPU 容量 > DAFT_AUTOSCALING_THRESHOLD）时，返回所有 backlog 任务的 `TaskResourceRequest` 列表。
  - 该列表在 `SchedulerActor::run_scheduler_loop()` 中被取出并传递给 `WorkerManager.try_autoscale(request)`。
- 聚合与调用：
  - `RayWorkerManager::try_autoscale()` 聚合 CPU/GPU/内存请求，并在当前容量与历史最大请求（单调上限）之上时，调用 Python 侧 `flotilla.try_autoscale(bundles)`。
  - `flotilla.try_autoscale()` 内部调用 `ray.autoscaler.sdk.request_resources(bundles)`：
    - 这是 Ray 的扩容 API，意为“立刻扩展集群以满足指定资源需求”，绕过正常扩容速度限制。
    - 该 API 不支持负数请求，不会缩容。
- 作用与边界：
  - autoscaling_request 的作用是“扩容加速”，仅用于 scale-up，避免 backlog 长时间堆积。
  - 修复后当扩容信号出现时，会显式将 `downscale_below_threshold_ticks` 归零，避免在短期抖动下扩缩互相拉扯导致振荡。

## downscale_below_threshold_ticks：定义、维护与作用（缩容稳定窗口）

- 定义与维护：
  - 在 `SchedulerActor::run_scheduler_loop()` 中，每个调度 tick 计算 `ratio = backlog_cpu / capacity_cpu`，当 `ratio < DAFT_AUTOSCALING_DOWNSCALE_RATIO_THRESHOLD`（默认 0.75）时自增 `downscale_below_threshold_ticks`，否则归零。
  - 当发生扩容请求（autoscaling_request 触发）时，显式将 `downscale_below_threshold_ticks` 归零并打印日志，体现互斥与协同关系。
- 之前未生效的原因：
  - 计数器虽有计算，但未被调度主循环使用（未触发任何缩容决策或回收动作）。
- 修复后的使用：
  - 当 `downscale_below_threshold_ticks ≥ DAFT_AUTOSCALING_DOWNSCALE_STABLE_TICKS`（默认 10 tick）且活跃 worker 数超过最小幸存数时，进入缩容判定：
    - 计算最多可回收数量：不超过 `DAFT_AUTOSCALING_MAX_DOWNSCALE_FRACTION`（默认 10%），但至少回收 1 个；同时不低于 `DAFT_AUTOSCALING_MIN_SURVIVOR_WORKERS`（默认 1）。
    - 调用 `WorkerManager.retire_idle_workers(max_to_retire)`，仅回收“无在途任务且闲置时间 ≥ `DAFT_AUTOSCALING_DOWNSCALE_IDLE_SECONDS`”（默认 120s）的 worker，优先回收“最长闲置”。

## 完整调度流程中的关系与协同（扩/缩互斥 + Ray 协同）

- 主循环链路：
  1) 更新 worker snapshots（含每个 worker 的 total/active 资源、在途任务明细）。
  2) 计算 backlog/capacity 比值与打印关键变量（ratio、backlog CPU/GPU、当前总容量、downscale ticks）。
  3) 派发当前可调度任务到各 worker。
  4) 伸缩判定：
     - 扩容：当 `DefaultScheduler` 返回 autoscaling_request 时，聚合打印请求明细，调用 `try_autoscale`，并将 `downscale_below_threshold_ticks` 归零。
     - 缩容：当 ratio 低于阈值且稳定（ticks 触达稳定窗口）时，计算候选与计划回收数，调用 `retire_idle_workers`，并在完成后归零稳定计数。
  5) 并发等待：新任务、任务完成或下一 tick。
- 与 Ray 的协同机制：
  - 扩容：通过 `request_resources` 扩展集群；Ray 会根据 bundles 增配资源。
  - 缩容：通过主动关闭与注销闲置 `RaySwordfishWorker` 所在 actor，释放节点资源；Ray 会在检测到节点空闲后执行下扩。
  - 注意：Ray 不支持“负请求”的主动缩容 API，故需要我们在 Daft 层主动 retire 闲置 actor 来驱动下扩。

## 关键参数与默认值

- DAFT_AUTOSCALING_THRESHOLD（默认 1.25）：扩容判定阈值，待调度任务数 ÷ 总 CPU 容量 > 阈值时触发扩容；仅影响 scale-up。
- DAFT_AUTOSCALING_DOWNSCALE_ENABLED（默认 true）：是否启用缩容逻辑。
- DAFT_AUTOSCALING_DOWNSCALE_RATIO_THRESHOLD（默认 0.75）：当 backlog/capacity 比值低于该值时开始累计稳定 tick。
- DAFT_AUTOSCALING_DOWNSCALE_STABLE_TICKS（默认 10）：缩容稳定窗口长度（tick 数）。
- DAFT_AUTOSCALING_MIN_SURVIVOR_WORKERS（默认 1）：缩容后至少保留的最小幸存 worker 数量。
- DAFT_AUTOSCALING_MAX_DOWNSCALE_FRACTION（默认 0.10）：单次缩容的最大比例上限（对活跃 worker 数的比例），但至少回收 1 个。
- DAFT_AUTOSCALING_DOWNSCALE_IDLE_SECONDS（默认 120）：候选 worker 最小闲置时长（只回收无在途任务且闲置时间达到该阈值的 worker）。

## 日志观测点与调试建议

- 调度主循环（Rust：`scheduler_actor.rs`）
  - 每 tick：`ratio`、backlog CPU/GPU、当前总容量 CPU/GPU、`downscale_below_threshold_ticks`。
    - 例：`[scheduler_tick] ratio=0.123 backlog_cpu=8.00 backlog_gpu=0.00 capacity_cpu=64.00 capacity_gpu=0.00 downscale_ticks=3`
  - 扩容触发：聚合请求的 CPU/GPU/内存与任务数，打印并重置 `downscale_below_threshold_ticks`。
    - 例：`[scale_up] tasks=128 req_cpu=128.00 req_gpu=0.00 req_mem_bytes=0 cause=pending>threshold downscale_below_threshold_ticks reset`
  - 缩容判定：候选数量、阈值参数与最终计划回收数量；完成回收后打印回收数量（详细列表由 WorkerManager 打印）。
    - 例：`[downscale_check] ratio=0.12 candidates=5 threshold_ratio=0.75 stable_ticks=10/10 fraction_limit=0.10 min_survivors=1 plan_retire=1`
- WorkerManager（Rust：`RayWorkerManager`）
  - `retire_idle_workers`：打印实际被回收的 worker 列表与回收原因（idle 秒数）。
    - 例：`[retire_idle_workers] threshold_idle_secs=120 retiring=1 -> ["abcd1234:845s"]`
    - 每个回收：`[retire] worker_id=abcd1234 idle_secs=845`
- RaySwordfishWorker（Rust：`worker.rs`）
  - `mark_task_finished`：打印更新后的 `last_task_finished_at`。
- Python 侧（`flotilla.try_autoscale`）
  - 调用前后打印请求明细，并强调“该 API 仅扩容、不会缩容”。
    - 例：`[flotilla.try_autoscale] Scale-up only; bundles=[{"CPU": 64, "GPU": 0, "memory": 0}]`

## 工程建议

- 设定合理的扩缩容阈值：
  - 扩容阈值建议 > 1，避免短期抖动；缩容阈值建议 < 1（默认 0.75），且结合稳定窗口防抖。
- 最小幸存与比例上限：
  - 默认为保守值，确保不会过度缩容引发冷热启动成本；可根据工作负载峰谷形态调优。
- 闲置检测的正确性：
  - 只回收“无在途任务”的 worker，任务完成路径需正确维护 `last_task_finished_at`；建议在任务完成高频场景下观察日志，确保不误杀正在执行的 worker。
- Ray 协同的理解：
  - Ray 的 `request_resources` 仅扩容；下扩需要通过主动 retire actor 来让 Ray 感知空闲以做缩容。二者互斥但协同：扩容触发时应重置缩容稳定计数，缩容动作完成后也应重置计数。

---

如需进一步验证端到端行为，建议在开发或 CI 环境搭建最小 Ray 集群，设定上述环境变量，观察日志中各关键打印的变化与回收行为是否符合预期。
