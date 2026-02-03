# Skill: Navigating Daft Documentation

This guide provides practical ways to query and navigate Daft's documentation for general usage questions, tailored for terminal-driven workflows.

## Purpose

To enable an AI programming assistant or developer to quickly find relevant information about Daft's APIs, concepts, and examples using both the live documentation site and the source repository.

## How It Works: Documentation Structure

Daft's documentation lives in two main places:

1.  **Live Documentation Site (`https://docs.daft.ai`):**
    -   Built with `mkdocs`.
    -   Searchable, user-friendly, and always up-to-date with the latest release.
    -   The best source for tutorials, API references, and conceptual explanations.
2.  **Source Repository (`Daft/docs/`):**
    -   Contains the Markdown source files for the live docs.
    -   Useful for offline access and for `grep`-based searching when you know exactly what you're looking for.
    -   The structure is defined by `Daft/mkdocs.yml` and the navigation by `Daft/docs/SUMMARY.md`.

## Quick Recipes

### Recipe 1: Finding API Usage with `grep`

**Goal:** Find how to use a specific function, e.g., `daft.from_glob`.

**Steps:**
1.  Use `grep_search` to search for the function name in the `Daft/docs/` directory.
2.  Prioritize results from the `api/` and `examples/` subdirectories.

**Terminal-Friendly Guidance:**

```bash
# Search for the function in the docs folder
grep_search(pattern='from_glob', path='Daft/docs/')

# A good search often reveals the main API page and usage examples.
# From the results, you might find:
# - Daft/docs/api/io.md (the official API definition)
# - Daft/docs/examples/querying-images.md (a practical usage example)
```

### Recipe 2: Understanding a Concept (e.g., Partitioning)

**Goal:** Learn about a core Daft concept like "partitioning".

**Steps:**
1.  Use the live docs search bar at `https://docs.daft.ai`. It's the most effective way.
2.  Alternatively, use `grep` in the `Daft/docs/` directory. Conceptual pages are often in the `optimization/` or `distributed/` sections.

**Terminal-Friendly Guidance:**

**Option A: Open the docs site (preferred)**
> "Open the URL `https://docs.daft.ai` and search for 'partitioning'."

**Option B: `grep` the source**
```bash
# Search for "partitioning" with context
grep_search(pattern='partitioning', path='Daft/docs/', mode='content', lines_after=3)

# This might point you to `Daft/docs/optimization/partitioning.md`,
# which is the main page on this topic.
```

### Recipe 3: Exploring All Available Documentation Topics

**Goal:** Get a high-level overview of all documentation sections.

**Steps:**
1.  Read the `Daft/docs/SUMMARY.md` file. This file defines the navigation structure of the live docs site.

**Terminal-Friendly Guidance:**

```bash
read(file_path='Daft/docs/SUMMARY.md')
```
This will show you the entire table of contents, helping you locate the relevant section for your query (e.g., "Connectors", "Custom Code (UDFs)", "Distributed Execution").

## Relevant Pages for Common Topics

Here are direct pointers to key documentation sections within the `Daft/docs/` folder:

-   **Partitioning:** `optimization/partitioning.md`
-   **Runners (Native vs. Ray):** `distributed/ray.md`, `distributed/kubernetes.md`
-   **UDFs (v2 - modern):** `custom-code/func.md` (for `@daft.func`), `custom-code/cls.md` (for `@daft.cls`)
-   **UDFs (v1 - legacy):** `custom-code/udfs.md`
-   **Connectors (S3, Lance, etc.):** `connectors/` directory. See `connectors/index.md` for an overview.
-   **Data Reading/Writing:** `api/io.md`
-   **Configuration:** `api/config.md`

## Example Prompts for ClaudeCODE

-   "Where can I find information on how to read Parquet files in Daft? Search the docs."
-   "Find an example of how to use a stateful UDF with a class in Daft." (This would lead to `custom-code/cls.md`)
-   "What configuration options are available in Daft? Show me the documentation page." (This would lead to `api/config.md`)
-   "List all the available documentation sections in Daft." (This calls for reading `docs/SUMMARY.md`)

## Pitfalls & Checks

-   **Searching the Entire Repo:** Searching the entire Daft repository for a function can be noisy. Limit your initial search to `Daft/docs/` to find usage guidance first.
-   **Relying Only on `grep`:** The live docs search (`https://docs.daft.ai`) is often superior as it understands context and ranks results. Use it when possible.
-   **Outdated Local Docs:** If you are not on the `main` branch, the local docs in `Daft/docs/` may not reflect the latest features. When in doubt, refer to the live site.

## References

-   **[Live Documentation Site](https://docs.daft.ai)**
-   **[Docs Source Directory](./)**
-   **[Docs Navigation File](./SUMMARY.md)**
