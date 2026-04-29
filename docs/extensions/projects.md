# Built on Daft

This page highlights projects that depend on or build on top of
[Daft](https://github.com/Eventual-Inc/Daft).

These projects are different from [Community Extensions](community.md). An
extension directly adds reusable Daft APIs, functions, datatypes, or distributed
operators. A project built on Daft may instead use Daft as a query engine,
DataFrame runtime, compute backend, or pipeline framework.

To propose a project for this page, open a PR with a short description and a
link to the relevant Daft usage.

## Projects Building on Daft

| Project | Description | How It Uses Daft |
|---|---|---|
| [DeltaCat](https://github.com/ray-project/deltacat) | Portable multimodal lakehouse on Ray, Arrow, and Daft. | Daft is a core dependency: `dc.read()` returns a Daft DataFrame by default, and Daft is a first-class compute backend for reading and writing lake data. |
| [SmooSense](https://github.com/SmooSenseAI/smoosense) | Web-based tool for interactively exploring large-scale multimodal tabular data. | Accepts Daft DataFrames as input in the Jupyter widget (`Sense(daft_df)`) and converts them for display alongside Pandas DataFrames. |
| [eai-taxonomy](https://github.com/Essential-AI/eai-taxonomy) | Annotation tools and dataset filters from the Essential-Web project. | Uses Daft in annotation pipelines to read Parquet annotation files and compute inter-annotator agreement. |
| [GraphReduce](https://github.com/wesmadrigal/GraphReduce) | Feature engineering library for large graphs of tabular data. | Supports Daft as a compute backend via `pip install "graphreduce[daft]"`. |
| [Archetype](https://github.com/VangelisTech/archetype) | DataFrame-first, append-only ECS runtime for simulations and AI agents. | Uses Daft DataFrames as the native data structure for world state and processor logic. |
| [hypergraph](https://github.com/gilad-rubin/hypergraph) | Python workflow orchestration framework for DAG pipelines and agentic workflows. | Provides a `DaftRunner` that compiles a hypergraph DAG into a Daft query plan using Daft UDFs. |
| [Sashimi4Talent](https://github.com/colin-ho/Sashimi4Talent) | Data pipeline and web app for discovering and ranking GitHub contributors. | Uses Daft as the core processing engine for repository search, commits, contributor analysis, and dataset merges. |
| [daft-image-playground](https://github.com/peckjon/daft-image-playground) | Flask-based intelligent image search engine. | Uses Daft to discover images and run native image decoding, resizing, and encoding. |
| [SMiR](https://github.com/togethercomputer/SMiR) | Synthetic data pipeline for multi-image reasoning. | Uses Daft with the Ray runner to read Parquet from S3, download images, and run GPU-accelerated embedding UDFs. |
| [Derezz](https://github.com/rchowell/Derezz) | CLI tool for video indexing and semantic search. | Uses Daft to read and write an S3 Tables-backed video frame index and query it. |
| [teraflopai-daft](https://github.com/teraflop-ai/teraflopai-daft) | Daft plugin library for Teraflop AI NLP services. | Exposes custom Daft expressions such as `segment_text` and `search_text` that call the Teraflop AI provider API. |
| [daft-sql-adapter](https://github.com/RaghwendraSingh/daft-sql-adapter) | CLI adapter that runs Spark SQL queries against Databricks Unity Catalog tables through Daft. | Uses Daft as the execution engine for SQL queries and table writes. |
| [zarr-daft-datasource](https://github.com/liaoruoxue/zarr-daft-datasource) | Prototype compute-storage-separation architecture using Zarr, LanceDB, and Daft. | Implements a custom Daft `DataSource` that reads Zarr arrays into lazy Daft DataFrames. |
| [pdf-document-processing-daft](https://github.com/doronkatz/pdf-document-processing-daft) | Demonstration pipeline for scalable PDF document processing. | Uses Daft DataFrames to parallelize PDF parsing, text chunking, embedding generation, and Parquet writes. |
| [daft-ai-driving](https://github.com/legendPerceptor/daft-ai-driving) | Project for processing the KITTI autonomous driving dataset. | Uses Daft DataFrames for data loading, filtering, aggregation, and Parquet export. |
| [sigil](https://github.com/junaidrahim/sigil) | CLI tool for collecting and analyzing AI coding session data. | Uses Daft to write local Parquet session data and query it with filtering and aggregation. |
| [embed_qwen_k8s](https://github.com/jaychia/embed_qwen_k8s) | Kubernetes pipeline for Common Crawl text processing and embeddings. | Uses Daft to read WARC/Parquet data, apply UDFs for chunking and embedding, and write results to S3. |

## Usage Patterns and Examples

| Project | Description | How It Uses Daft |
|---|---|---|
| [daft-examples](https://github.com/Eventual-Inc/daft-examples) | Official Daft examples repository. | Shows idiomatic Daft usage across `daft.read_*`, UDFs, AI functions, multimodal data, SQL analytics, vector search, and integrations. |
| [workflow-eventual-inc-daft-distributed-udf-processing](https://github.com/leeroopedia/workflow-eventual-inc-daft-distributed-udf-processing) | Workflow demonstrating custom Python UDFs for ML inference and GPU transforms. | Uses `@daft.func` and `@daft.cls` with lazy and distributed execution. |
| [workflow-eventual-inc-daft-sql-query-analytics](https://github.com/leeroopedia/workflow-eventual-inc-daft-sql-query-analytics) | Workflow for SQL analytics with Daft sessions and catalogs. | Creates sessions, attaches catalogs, registers tables, executes SQL, and exports results. |
| [workflow-eventual-inc-daft-data-lakehouse-etl](https://github.com/leeroopedia/workflow-eventual-inc-daft-data-lakehouse-etl) | ETL pipeline for Iceberg, Delta Lake, and Hudi lakehouse formats. | Uses Daft lakehouse readers, DataFrame transformations, and write APIs. |
| [workflow-eventual-inc-daft-multimodal-ai-batch-inference](https://github.com/leeroopedia/workflow-eventual-inc-daft-multimodal-ai-batch-inference) | Workflow for AI inference over multimodal datasets. | Applies Daft AI functions and embedding workflows to Hugging Face, Parquet, and CSV datasets. |

## Daft as a Chosen Backend

| Project | Description | How It Uses Daft |
|---|---|---|
| [cuallee](https://github.com/canimus/cuallee) | DataFrame-agnostic data quality check library. | Implements a Daft validation backend and test suite for quality checks against Daft DataFrames. |
| [Atlan application-sdk](https://github.com/atlanhq/application-sdk) | SDK for building data catalog integration applications. | Uses Daft to lazily read and filter JSON table metadata files during incremental sync. |
| [narwhals-daft](https://github.com/narwhals-dev/narwhals-daft) | Narwhals plugin for Daft. | Wraps Daft DataFrames so the Narwhals expression API can execute lazily on Daft. |
| [jett](https://github.com/ddeutils/jett) | ETL template framework supporting multiple DataFrame engines. | Lists Daft as a supported engine backend for YAML-defined ETL pipelines. |

## Ecosystem and Content

| Project | Description | How It Uses Daft |
|---|---|---|
| [AWS MCP servers](https://github.com/awslabs/mcp) | Collection of open-source MCP servers for AWS services. | The S3 Tables MCP server uses Daft as the SQL query engine for read-only queries against AWS S3 Tables. |
| [Fabric_Notebooks_Demo](https://github.com/djouallah/Fabric_Notebooks_Demo) | Microsoft Fabric notebook demos. | Includes ETL notebooks using Daft to load and transform CSVs from Azure storage and write Delta Lake tables. |
| [lakevision](https://github.com/lakevision-project/lakevision) | Web tool for exploring Apache Iceberg lakehouses. | Uses Daft to query and display sample data from Iceberg tables. |
| [general-demos](https://github.com/jaehyeon-kim/general-demos) | Data engineering demo projects. | Includes a `daft-quickstart` folder demonstrating Daft with Apache Iceberg. |
| [DaftHudi](https://github.com/dipankarmazumdar/DaftHudi) | Demo dashboard application using Apache Hudi, Daft, and Streamlit. | Uses Daft to read Apache Hudi tables and power analytical queries. |
| [Engineering Lakehouses with Open Table Formats](https://github.com/PacktPublishing/Engineering-Lakehouses-with-Open-Table-Formats) | Code samples for a lakehouse engineering book. | Includes a notebook demonstrating reading and writing Apache Iceberg tables with Daft. |
| [databricks-demos](https://github.com/david-hurley/databricks-demos) | Databricks demo notebooks. | Includes a notebook using Daft with Unity Catalog to write data to Delta tables. |
| [knee-deep-in-the-lake](https://github.com/Cumulocity-IoT/knee-deep-in-the-lake) | Hands-on training repository for IoT data lake technologies. | Uses Daft in notebooks alongside PyArrow and Pandas for querying Parquet files with SQL. |
