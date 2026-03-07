# Daft Skills

Daft ships with built-in skills that help AI coding agents work effectively with Daft.

## Available skills

### [daft-udf-tuning](https://github.com/Eventual-Inc/Daft/blob/main/.claude/skills/daft-udf-tuning/SKILL.md)

Optimize User-Defined Functions for performance.

**Triggers:** GPU inference setup, slow UDFs, async or batch processing questions.

**What it helps with:**

- Choosing between `@daft.func` (stateless), `@daft.cls` (stateful), and `@daft.func.batch` (vectorized)
- Async I/O patterns for web APIs
- GPU batch inference with PyTorch/model loading via `@daft.cls(gpus=N)`
- Tuning `max_concurrency`, `batch_size`, `gpus`, and `into_batches(N)`

### [daft-distributed-scaling](https://github.com/Eventual-Inc/Daft/blob/main/.claude/skills/daft-distributed-scaling/SKILL.md)

Scale single-node workflows to distributed execution.

**Triggers:** Performance optimization, handling large datasets, distributed execution.

**What it helps with:**

- Choosing between shuffle (`repartition(N)`) and streaming (`into_batches(N)`) strategies
- Calculating optimal partition counts for your cluster
- Avoiding OOM errors with heavy data (images, tensors)
- Advanced tuning formulas for repartitioning and streaming

### [daft-docs-navigation](https://github.com/Eventual-Inc/Daft/blob/main/.claude/skills/daft-docs-navigation/SKILL.md)

Navigate Daft's documentation for APIs, concepts, and examples.

**Triggers:** General questions about Daft APIs, concepts, or searching the docs.

**What it helps with:**

- Navigating key doc locations: API reference, optimization, distributed, UDFs, connectors
- Searching for API usage in the `docs/` directory
- Browsing the docs structure via `docs/SUMMARY.md`

## Installation

### Already cloned Daft (automatic)

If you're working inside a Daft clone, the skills are already available. No setup needed - Claude Code picks them up from `.claude/skills/`.

### Install globally with `npx skills add`

To use Daft skills outside the repo (across 30+ agents including Claude Code, Cursor, Codex, Gemini CLI, Copilot, and more):

```bash
# Install all skills
npx skills add Eventual-Inc/Daft

# Install a specific skill
npx skills add Eventual-Inc/Daft --skill daft-udf-tuning
```

This creates symlinks so the skills are available in any project.

### Claude Code plugin

Daft is also available as a Claude Code plugin, which can bundle MCP servers and hooks alongside skills.

```bash
claude plugin add Eventual-Inc/Daft
```
