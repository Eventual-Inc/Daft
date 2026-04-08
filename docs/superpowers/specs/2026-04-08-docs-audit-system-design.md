# Docs Audit System Design

**Date**: 2026-04-08
**Status**: Approved

## Problem

The Daft documentation has 100+ issues across 70+ pages: broken code examples, missing API documentation, inconsistent patterns, stale content, and gaps that prevent AI coding agents from generating correct code. There is no automated system to catch these issues before they ship or to prevent regressions.

## Solution

A two-tier automated documentation audit system:

- **Tier 1** (Daft repo, CI): Deterministic scripts + pytest-based code block validation, run on every PR touching docs or API surface.
- **Tier 2** (separate repo, weekly): Agent-based task runner that gives Claude hard tasks with only docs as context, executes the generated code against local fixtures with mock AI providers, and reports pass/fail.

## Tier 1: Deterministic Checks (Daft Repo)

### Location

All audit infrastructure lives in `docs/audit/` within the Daft repo.

```
docs/audit/
  check_api_coverage.py     # public API vs what's documented
  check_consistency.py      # import patterns, decorators, param names
  check_completeness.py     # nav coverage, placeholders, stale content
  conftest.py               # pytest fixtures: mock AI providers, data paths
  __init__.py
```

### Scripts

#### `check_api_coverage.py`

Uses `griffe` (already a dependency) to introspect the live `daft` module, then checks what's documented.

**Checks:**
1. **Public functions** -- all `daft.read_*`, `daft.from_*`, `daft.set_*` vs what's listed on `api/io.md` and other API pages (parsed from `:::` directives and markdown content).
2. **DataFrame methods** -- all public methods on `daft.DataFrame` vs what's rendered in `api/dataframe.md`.
3. **Write methods** -- all `DataFrame.write_*` vs what's on `api/io.md` and `connectors/index.md`.
4. **Install extras** -- all keys in `pyproject.toml` `[project.optional-dependencies]` vs what's shown on the install page.
5. **AI function parameters** -- introspect `prompt`, `embed_text`, `classify_text` signatures via `inspect.signature()` vs what's documented on AI function pages.
6. **UDF decorator parameters** -- introspect `@daft.func`, `@daft.cls`, `@daft.method` signatures vs what's documented on custom-code pages.

**Output:** Coverage report with documented/undocumented/total per category. Exit code 1 if coverage drops below configurable threshold (default: 90%).

#### `check_consistency.py`

Cross-references docs against each other and against source code.

**Checks:**
1. **Import patterns** -- extracts all `from daft.* import ...` and `import daft.*` from markdown code blocks. Flags when the same function is imported differently across pages (e.g., `from daft.functions import prompt` vs `from daft.functions.ai import prompt`).
2. **Decorator usage** -- flags legacy `@daft.udf(` in pages that aren't explicitly about the legacy API. Flags inconsistent `@daft.cls()` vs `@daft.cls` style.
3. **Parameter name validation** -- for parameters used in code examples, checks they match the actual function signature via `inspect.signature()`. Catches issues like `partition_on=` when the real param is `partition_col=`.
4. **Provider setup patterns** -- flags multiple different provider setup patterns across non-provider-comparison pages.

**Output:** List of inconsistencies with file:line locations and severity.

#### `check_completeness.py`

Verifies structural completeness of the docs.

**Checks:**
1. **Nav coverage** -- every `.md` file in `docs/` is either referenced in `SUMMARY.md` or on an explicit exclusion list.
2. **Connector index** -- every connector with a dedicated page in `docs/connectors/` is listed in `connectors/index.md`.
3. **Placeholder pages** -- flags pages with fewer than 50 words of non-frontmatter content (catches "Coming soon!" stubs).
4. **Commented-out content** -- flags HTML comments containing substantial content (> 200 chars), which may be accidentally hidden documentation.
5. **Stale roadmap items** -- extracts claims like "not yet supported", "on the roadmap", "doesn't exist" from docs, then checks if the referenced API/feature actually exists in the codebase.

**Output:** List of completeness gaps with file:line locations.

### pytest-markdown-docs Integration

Uses the `pytest-markdown-docs` package to extract and execute Python code blocks from markdown files.

**`docs/audit/conftest.py`** provides:
1. **Mock AI provider** -- a `MockProvider` class registered via `daft.set_provider()` that returns deterministic responses for `prompt`, `embed_text`, `classify_text`. Registered as an autouse session fixture.
2. **Test data paths** -- constants pointing to existing fixtures in `tests/assets/` (CSV, Parquet, JSON, images, audio, video). No duplication of test data.
3. **Environment setup** -- sets `OPENAI_API_KEY=mock-key` and similar env vars so code blocks that reference them don't fail on missing credentials.

**Fixture data (reused from existing tests):**
- `tests/assets/mvp.csv` -- simple CSV
- `tests/assets/parquet-data/mvp.parquet` -- basic Parquet
- `tests/assets/json-data/sample1.json` -- JSON array of objects
- `tests/cookbook/assets/images/` -- JPEG, PNG, TIFF images
- `tests/assets/sample_audio.mp3` -- audio file
- `tests/assets/sample_video.mp4` -- video file

**Execution strategy:**
- Code blocks in tutorial/example pages are executed with shared state (`memory=True` equivalent via fixture markers).
- Code blocks in API reference pages are syntax-checked only (they use `:::` mkdocstrings directives, not fenced Python).
- Code blocks with known external dependencies (private S3 buckets, specific model downloads) are marked with `<!--pytest-markdown-docs:skip-->`.

### mkdocs Strict Mode

Enable `strict: true` in `mkdocs.yml`. This catches broken mkdocstrings cross-references and autorefs links at build time, which already runs in CI on every PR. Zero new dependencies.

### interrogate Integration

Add `interrogate` to the CI pipeline to enforce docstring presence on all public APIs in the `daft` Python package. Configured in `pyproject.toml`:

```toml
[tool.interrogate]
ignore-init-module = true
ignore-init-method = true
ignore-magic = true
ignore-private = true
fail-under = 80
exclude = ["tests", "docs", "benchmarking"]
```

### CI Workflow

**`.github/workflows/docs-audit.yml`**

```yaml
name: Docs Audit
on:
  pull_request:
    paths:
      - 'docs/**'
      - 'daft/**'
      - 'pyproject.toml'
  push:
    branches: [main]
    paths:
      - 'docs/**'
      - 'daft/**'
      - 'pyproject.toml'

jobs:
  tier1-audit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - name: Install dependencies
        run: |
          pip install uv
          uv pip install -e ".[docs,all]"
          uv pip install pytest-markdown-docs interrogate
      - name: Build Daft
        run: maturin develop --uv
      - name: Check API coverage
        run: python docs/audit/check_api_coverage.py
      - name: Check consistency
        run: python docs/audit/check_consistency.py
      - name: Check completeness
        run: python docs/audit/check_completeness.py
      - name: Validate code blocks
        run: pytest --markdown-docs docs/ -v
      - name: Check docstring coverage
        run: interrogate daft/
```

## Tier 2: Agent Task Runner (Separate Repo)

### Repository

New private repo: `Eventual-Inc/daft-docs-audit`.

```
daft-docs-audit/
  tasks.yaml                    # task definitions
  run_agent_tasks.py            # runner script
  reports/                      # generated audit reports
    YYYY-MM-DD.md
  .github/workflows/
    weekly-audit.yml
  pyproject.toml
  README.md
```

**Secrets required:** `ANTHROPIC_API_KEY`

### Task Definitions (`tasks.yaml`)

Each task represents a hard, multi-step workflow that a real user would attempt. Tasks are designed to exercise known documentation weak spots.

```yaml
tasks:
  - id: gpu-image-classifier-pipeline
    name: "Build a GPU image classification pipeline with UDFs"
    category: multi-step-pipeline
    execute: true
    docs_context:
      - custom-code/func.md
      - custom-code/cls.md
      - modalities/images.md
      - api/udf.md
    prompt: |
      Using only the Daft documentation provided, write a Python script that:
      1. Reads image URLs from a CSV file at "{data_csv}"
      2. Downloads and decodes the images
      3. Writes a @daft.cls class UDF that runs an image classifier
         (use a simple mock model, no real ML framework needed)
      4. Specify GPU resource requests on the UDF
      5. Collect and print results
      The script must be complete and runnable.

  - id: iceberg-s3-auth
    name: "Read Iceberg table from S3 with IAM credentials"
    category: connector-auth
    execute: true
    docs_context:
      - connectors/iceberg.md
      - connectors/aws.md
      - api/io.md
    prompt: |
      Using only the Daft documentation provided, write a Python script that:
      1. Configures S3 credentials (access key, secret key, region)
      2. Loads a PyIceberg catalog
      3. Reads an Iceberg table with a snapshot_id filter
      4. Filters and selects columns
      5. Writes results to a local Parquet file
      Use placeholder credentials. The script must be syntactically correct
      and use real Daft APIs.

  - id: complex-sql-query
    name: "Write a complex Daft SQL query"
    category: sql
    execute: true
    docs_context:
      - sql/index.md
      - sql/statements/select.md
      - sql/window_functions.md
      - configuration/sessions-usage.md
    prompt: |
      Using only the Daft documentation provided, write a Python script that:
      1. Creates two DataFrames (orders and customers) from dicts
      2. Registers them as tables in a Daft session
      3. Writes a SQL query that joins the tables, uses a CTE,
         applies a window function (ROW_NUMBER), and filters with HAVING
      4. Collects and prints results
      The script must be complete and runnable.

  - id: migrate-legacy-udf
    name: "Migrate a legacy UDF to the new API"
    category: migration
    execute: true
    docs_context:
      - custom-code/migration.md
      - custom-code/func.md
      - custom-code/cls.md
    prompt: |
      Using only the Daft documentation provided, migrate the following
      legacy UDF to the new API. Preserve GPU resource requests, concurrency
      settings, and error handling:

      ```python
      @daft.udf(return_dtype=daft.DataType.string(), num_gpus=1)
      def classify(images: daft.Series):
          model = load_model()
          return [model.predict(img) for img in images.to_pylist()]
      ```

      Write the complete migrated code as a runnable script that creates
      a test DataFrame and applies the UDF.

  - id: async-api-udf-with-retries
    name: "Write an async UDF that calls an API with error handling"
    category: cross-cutting
    execute: true
    docs_context:
      - custom-code/func.md
      - custom-code/cls.md
      - ai-functions/overview.md
      - optimization/memory.md
    prompt: |
      Using only the Daft documentation provided, write a Python script that:
      1. Defines an async @daft.func that calls an external HTTP API
      2. Configures max_concurrency, max_retries, and on_error handling
      3. Creates a test DataFrame with 10 rows
      4. Applies the UDF and collects results
      The script must be complete and runnable (mock the HTTP calls).

  - id: scaling-oom-fix
    name: "Fix an OOM pipeline with proper configuration"
    category: scaling
    execute: true
    docs_context:
      - optimization/memory.md
      - optimization/partitioning.md
      - optimization/join-strategies.md
      - distributed/ray.md
    prompt: |
      Using only the Daft documentation provided, a user has a pipeline
      that OOMs when joining two large tables. Write a Python script that:
      1. Creates two moderately-sized DataFrames (use from_pydict)
      2. Configures execution settings to handle memory pressure
         (batch size, shuffle algorithm, flight shuffle dirs)
      3. Performs the join
      4. Collects results
      Use the documented configuration APIs. The script must be runnable.

  - id: batch-inference-structured-output
    name: "Set up batch inference with structured output"
    category: ai-functions
    execute: true
    docs_context:
      - ai-functions/prompt.md
      - ai-functions/providers.md
      - quickstart.md
    prompt: |
      Using only the Daft documentation provided, write a Python script that:
      1. Configures an OpenAI provider
      2. Creates a DataFrame with product descriptions
      3. Uses daft.functions.prompt() with a Pydantic return_format
         to extract structured data (name, category, price)
      4. Uses on_error handling for failed API calls
      5. Collects and prints results
      The script must be complete and runnable.

  - id: embed-and-search
    name: "Build a text embedding and semantic search pipeline"
    category: ai-functions
    execute: true
    docs_context:
      - ai-functions/embed.md
      - modalities/embeddings.md
      - api/ai.md
    prompt: |
      Using only the Daft documentation provided, write a Python script that:
      1. Creates a DataFrame with 10 text documents
      2. Embeds them using embed_text with a specified model and dimensions
      3. Embeds a query string
      4. Computes cosine distance between query and all documents
      5. Returns top 3 most similar
      The script must be complete and runnable.
```

Tasks start at ~8, expand over time as new doc pages are added or new weak spots are identified.

### Runner Script (`run_agent_tasks.py`)

1. **Clones Daft** at the latest main commit, builds it.
2. **Loads `tasks.yaml`**.
3. For each task:
   a. Reads the specified `docs_context` markdown files from the Daft clone.
   b. Calls Claude API with system prompt:
      ```
      You are a Python developer using the Daft framework. You have access
      ONLY to the documentation provided below. Do not use any knowledge
      about Daft beyond what is in these docs. Write complete, runnable
      Python code to accomplish the task. If the documentation is
      insufficient to complete any step, explain exactly what is missing.
      ```
   c. Injects the doc pages as user context.
   d. Extracts the code block from Claude's response.
   e. Prepends the mock preamble (imports mock providers from the Daft repo's `docs/audit/conftest.py`, sets up fixture data paths).
   f. Executes in a subprocess with 30-second timeout.
   g. Records result: `pass` (ran successfully), `partial` (ran but agent noted doc gaps), `fail` (crashed or agent couldn't complete).
   h. Captures any "documentation gaps" commentary from the agent.

4. **Generates report** at `reports/YYYY-MM-DD.md`.

### Report Format

```markdown
# Docs Audit Report - YYYY-MM-DD

Daft commit: <sha>
Tasks: N | Pass: X | Partial: Y | Fail: Z
Score: XX%

## Failures

### <task-id> (fail)
**Error:** <error message or agent commentary>
**Root cause:** <which doc page is missing what>
**Generated code:** <collapsed code block>

## Partial Passes

### <task-id> (partial)
**Gaps noted:** <agent's commentary on missing docs>

## Trends
| Date | Pass | Partial | Fail | Score |
|------|------|---------|------|-------|
| YYYY-MM-DD | X | Y | Z | XX% |
```

### Weekly CI Workflow

```yaml
name: Weekly Docs Audit
on:
  schedule:
    - cron: '0 6 * * 0'  # Sunday 6am UTC
  workflow_dispatch: {}

jobs:
  agent-audit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Clone Daft
        run: git clone --depth 1 https://github.com/Eventual-Inc/Daft.git
      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - name: Install Daft
        run: |
          cd Daft
          pip install uv
          uv pip install -e ".[all]"
          pip install maturin
          maturin develop --uv
      - name: Install runner deps
        run: pip install anthropic pyyaml
      - name: Run agent tasks
        env:
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
        run: python run_agent_tasks.py
      - name: Commit report
        run: |
          git config user.name "Docs Audit Bot"
          git config user.email "noreply@eventual.ai"
          git add reports/
          git commit -m "docs audit: $(date +%Y-%m-%d)" || true
          git push
```

## Mock AI Provider

Lives in `docs/audit/conftest.py` in the Daft repo. Used by both Tier 1 (pytest-markdown-docs) and Tier 2 (agent task runner).

```python
class MockProvider:
    """Deterministic mock provider for doc testing."""

    def prompt(self, messages, **kwargs):
        if kwargs.get("return_format"):
            # Return a valid JSON matching the Pydantic schema
            return '{"result": "mock"}'
        return "Mock response"

    def embed_text(self, text, **kwargs):
        # Return a deterministic embedding vector
        return [0.1] * (kwargs.get("dimensions", 1536))

    def classify_text(self, text, labels, **kwargs):
        # Return the first label
        return labels[0]
```

The above is a sketch -- the real implementation must conform to Daft's actual provider interface (the `Provider` base class and its `get_prompter`/`get_text_embedder`/`get_classifier` methods). The mock will be implemented by studying the existing provider implementations (e.g., `daft/ai/openai/`).

Registered as a session-scoped autouse pytest fixture and importable by the Tier 2 runner.

## Local Development

**Makefile targets in Daft repo:**
- `make docs-audit` -- runs all Tier 1 checks (no API key needed)
- `make docs-audit-code` -- runs only pytest-markdown-docs code block validation
- `make docs-audit-coverage` -- runs only API coverage check

## Implementation Order

1. Enable `strict: true` in `mkdocs.yml` (immediate win, zero effort)
2. Write `check_api_coverage.py` (highest value -- catches the 13 missing I/O methods etc.)
3. Write `check_consistency.py` (catches the 11 broken code examples)
4. Write `check_completeness.py` (catches placeholder pages, hidden nav items)
5. Set up `conftest.py` with mock providers and fixture paths
6. Integrate `pytest-markdown-docs` for code block execution
7. Add `interrogate` to CI
8. Create `.github/workflows/docs-audit.yml`
9. Create `Eventual-Inc/daft-docs-audit` repo
10. Write `tasks.yaml` and `run_agent_tasks.py`
11. Set up weekly CI workflow
