# Extensions

Daft extensions are reusable libraries that add domain-specific functionality
on top of Daft without requiring every specialized function, datatype,
integration, or workflow to live directly in Daft core.

There are two broad ways to build extensions:

- **Python UDF-based extensions**: Python packages built on Daft's
  [custom-code APIs](../custom-code/func.md), including
  [`@daft.func`](../api/udf.md), [`@daft.func.batch`](../api/udf.md),
  [`@daft.cls`](../api/udf.md), and [`@daft.method.batch`](../api/udf.md).
- **Native ABI extensions**: shared-library-backed packages that use Daft's
  Arrow C Data Interface ABI for lower-level, vectorized native execution.

Both models can expose clean Python APIs that feel like ordinary Daft
expression functions. The implementation details can stay hidden behind
functions, classes, and expressions.

## Python UDF-Based Extensions

The fastest way to build a Daft extension is often pure Python. Using
[`@daft.func`](../api/udf.md), [`@daft.func.batch`](../api/udf.md),
[`@daft.cls`](../api/udf.md), and [`@daft.method.batch`](../api/udf.md),
contributors can package reusable Python logic that runs inside Daft's
distributed execution engine.

This is ideal for libraries that orchestrate existing Python ecosystems,
external services, ML models, GPUs, data maintenance tasks, or domain-specific
workflows. These extensions do not require a native shared library or the
Arrow C ABI. They are normal Python packages that expose higher-level Daft
APIs.

[`daft-lance`](https://github.com/daft-engine/daft-lance) is an example of
this model. It extends Daft with Lance-specific distributed operations such as
file compaction, scalar indexing, column merging, and REST catalog operations.
Internally, it uses Daft's Python UDF and class-UDF APIs to distribute Lance
tasks across a Daft query, while users interact with functions like
`compact_files`, `create_scalar_index`, and `merge_columns_df`.

This pattern also exists inside Daft itself. The
[`daft.functions.ai`](../api/ai.md) module exposes high-level functions like
[`prompt`](../api/functions/prompt.md),
[`embed_text`](../api/functions/embed_text.md),
[`embed_image`](../api/functions/embed_image.md),
[`classify_text`](../api/functions/classify_text.md), and
[`classify_image`](../api/functions/classify_image.md). To users, these look
like normal Daft expression functions. Under the hood, they use Daft's UDF and
class-UDF machinery, including batching, concurrency controls, retries, and
[GPU resource hints](../custom-code/gpu.md).

The file APIs follow a similar product pattern. [`daft.File`](../api/datatypes/file_types.md)
values and helpers like [`file`](../api/functions/file.md),
[`audio_file`](../api/functions/audio_file.md),
[`video_file`](../api/functions/video_file.md),
[`file_path`](../api/functions/file_path.md), and
[`file_size`](../api/functions/file_size.md) give users expression-level
building blocks for working with files. Those file objects can then flow into
Python UDFs, model pipelines, and domain-specific libraries without users
needing to think about execution details.

## Native ABI Extensions

For contributors who need lower-level, vectorized performance, Daft also
supports native extensions through a C ABI based on the
[Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html).

Native ABI extensions can add high-performance scalar functions, aggregate
functions, Python expression wrappers, and extension-backed datatypes. They
ship as pip-installable Python packages that bundle a native shared library.
Users import the package, load it into a Daft [`Session`](../api/sessions.md),
and call ordinary Python wrappers in their DataFrame expressions.

Rust currently has the most ergonomic SDK through
[`daft-ext`](https://github.com/Eventual-Inc/Daft/tree/main/src/daft-ext),
while C++ is demonstrated through the raw ABI. Other systems languages are
possible if they can produce a shared library, export the expected C ABI, and
read/write Arrow C Data Interface arrays.

Daft's own examples include:

- [`examples/hello`](https://github.com/Eventual-Inc/Daft/tree/main/examples/hello):
  a minimal Rust native extension that registers a `greet` scalar function.
- [`examples/dvector`](https://github.com/Eventual-Inc/Daft/tree/main/examples/dvector):
  a pgvector-style native extension for vector distance functions such as
  `l2_distance`, `cosine_distance`, `inner_product`, and `jaccard_distance`.
- [`examples/hello_cpp`](https://github.com/Eventual-Inc/Daft/tree/main/examples/hello_cpp):
  a pure C++ native extension using Apache Arrow C++ and the raw Daft C ABI.

## What Can Contributors Build?

Contributors can build:

- Pure-Python UDF extension libraries using [`@daft.func`](../api/udf.md),
  [`@daft.func.batch`](../api/udf.md), [`@daft.cls`](../api/udf.md), and
  [`@daft.method.batch`](../api/udf.md)
- Distributed task libraries like
  [`daft-lance`](https://github.com/daft-engine/daft-lance)
- GPU-backed model inference extensions
- AI, file, media, and multimodal processing libraries that hide UDFs behind
  expression APIs
- Native scalar functions
- Native aggregate functions / UDAFs
- Python expression wrappers
- Extension-backed logical datatypes such as
  [`DataType.extension(...)`](../api/datatypes/all_datatypes.md)

## Where to Start

- Browse installable projects in [Community Extensions](community.md).
- Learn how to build a native extension in the [Authoring Guide](authoring.md).
- Browse projects using Daft in [Built on Daft](projects.md).
- Learn more about Daft's [Python UDFs](../custom-code/func.md).
