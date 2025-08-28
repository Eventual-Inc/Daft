# Overview

Daft is a high-performance data engine providing simple and reliable data processing for any modality and scale.

<style>
  .daft-pipeline-component {
    --ink: #f4f7ff;
    --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    --sans: Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial, "Apple Color Emoji","Segoe UI Emoji";

    display: block;
    color: var(--ink);
    font-family: var(--sans);
    margin: 20px 0;
    box-sizing: border-box;
  }

  .daft-pipeline-component .container {
    width: 100%;
    max-width: 1200px;
    margin: 0 auto;
    display: flex;
    flex-direction: column;
    gap: 12px;
  }

  .daft-pipeline-component .stage-box {
    padding: 6px;
  }

  .daft-pipeline-component .row {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 40px;
    align-items: center;
  }

  .daft-pipeline-component .stage-header {
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: .08em;
    color: #b9c8ff;
    font-size: clamp(12px, 1.2vw, 14px);
    margin-bottom: 12px;
  }

  .daft-pipeline-component .description {
    padding: 20px;
    display: flex;
    flex-direction: column;
    justify-content: center;
  }

  .daft-pipeline-component .description h2 {
    margin: 0 0 20px 0;
    font-size: 24px;
    font-weight: 700;
    color: #e8edff;
  }

  .daft-pipeline-component .description p {
    margin: 0 0 12px 0;
    line-height: 1.5;
    color: var(--ink);
    font-size: 15px;
  }

  .daft-pipeline-component .description p:last-child {
    margin: 0;
    font-size: 13px;
    color: rgba(185, 200, 255, 0.7);
    font-weight: 400;
    line-height: 1.4;
    font-style: italic;
  }

  .daft-pipeline-component .description ul {
    margin: 12px 0;
    padding-left: 18px;
  }

  .daft-pipeline-component .description li {
    margin: 6px 0;
    line-height: 1.4;
    color: var(--ink);
    font-size: 14px;
  }

  .daft-pipeline-component .pipeline-item {
    background: rgba(255,255,255,.02);
    border: 1px solid rgba(255,255,255,.08);
    border-radius: 8px;
    padding: 16px 20px;
    display: flex;
    align-items: center;
    min-height: 60px;
    font-family: var(--mono);
    font-size: clamp(13px, 1.8vw, 16px);
    color: var(--ink);
  }









  .daft-pipeline-component .type {
    display: inline-block;
    white-space: normal;
    position: relative;
    word-wrap: break-word;
  }

  .daft-pipeline-component .cursor {
    color: #ff00ff;
    animation: daft-caret .9s steps(1,end) infinite;
  }

  @keyframes daft-caret {
    50% { opacity: 0; }
  }

  .daft-pipeline-component .fade-in {
    animation: daft-enter .45s ease both;
  }

  @keyframes daft-enter {
    from { opacity: 0; transform: translateY(6px); }
    to { opacity: 1; transform: translateY(0); }
  }

  @media (max-width: 720px) {
    .daft-pipeline-component .stage-box {
      padding: 8px;
      margin-bottom: 4px;
    }
    .daft-pipeline-component .row {
      grid-template-columns: 1fr;
      gap: 20px;
    }
    .daft-pipeline-component .pipeline-item {
      min-height: 50px;
    }
    .daft-pipeline-component .description p:last-child {
      font-size: 12px;
      margin-top: 4px;
    }
  }
</style>

<div class="daft-pipeline-component">
  <div class="container">
    <div class="stage-box">
      <div class="row">
        <div class="description">
          <div class="stage-header">READ</div>
          <p>Daft reads raw unstructured and multimodal data collected from application systems</p>
          <p>Object Store (AWS S3, GCS, R2)<br>Event Bus (Kafka)<br>Data Lake (Iceberg, Deltalake)</p>
        </div>
        <div class="pipeline-item">
          <span id="read" class="swap"></span>
        </div>
      </div>
    </div>

    <div class="stage-box">
      <div class="row">
        <div class="description">
          <div class="stage-header">TRANSFORM</div>
          <p>Build efficient Daft data pipelines involving heavy transformations</p>
          <p>GPU models<br>User-provided Python code<br>External LLM APIs</p>
        </div>
        <div class="pipeline-item">
          <span id="xform" class="swap"></span>
        </div>
      </div>
    </div>

    <div class="stage-box">
      <div class="row">
        <div class="description">
          <div class="stage-header">WRITE</div>
          <p>Daft lands data into specialized data systems for downstream use-cases</p>
          <p>Search (fulltext, vector)<br>Applications (SQL database)<br>Analytics (data warehouse)<br>Model Training (TFRecords on object storage)</p>
        </div>
        <div class="pipeline-item">
          <span id="write" class="swap"></span>
        </div>
      </div>
    </div>
  </div>
</div>

<script>

// Hierarchical data buckets by modality with transform-to-write mappings
const MODALITIES = {
  "Images (JPEG)": {
    transforms: {
      "OCR for text extraction": ["Elasticsearch"],
      "Image captioning with LLM": ["PostgreSQL", "MongoDB"],
      "Object detection": ["PostgreSQL", "MySQL"],
      "Generate embeddings": ["Turbopuffer", "LanceDB"]
    }
  },
  "PDFs": {
    transforms: {
      "OCR for text extraction": ["Elasticsearch"],
      "Structured data extraction": ["BigQuery", "Snowflake", "Databricks"],
      "Generate embeddings": ["Turbopuffer", "LanceDB"],
      "PII detection": ["BigQuery", "Snowflake", "Databricks"],
      "Chunking + deduplication": ["Parquet"]
    }
  },
  "Video (MP4/MKV)": {
    transforms: {
      "Video captioning": ["PostgreSQL"],
      "Scene detection": ["AWS S3"],
      "Audio transcription": ["PostgreSQL", "MongoDB", "Elasticsearch"],
      "Generate embeddings": ["Turbopuffer", "LanceDB"]
    }
  },
  "Audio (WAV/MP3/FLAC)": {
    transforms: {
      "Transcription with Whisper": ["PostgreSQL", "MongoDB", "Elasticsearch"],
      "Speaker identification": ["PostgreSQL", "MySQL"],
      "Emotion detection": ["BigQuery", "Snowflake", "Databricks"],
      "Generate embeddings": ["Turbopuffer", "LanceDB"]
    }
  },
  "AI Agent Logs": {
    transforms: {
      "LLM summarization": ["PostgreSQL", "MySQL"],
      "Generate embeddings": ["Turbopuffer", "LanceDB"]
    }
  }
};

// Helpers
const q = (id) => document.getElementById(id);
const els = { read: q("read"), xform: q("xform"), write: q("write") };

function pick(list, last) {
    if (list.length < 2) return list[0];
    let choice;
    do choice = list[(Math.random() * list.length) | 0];
    while (choice === last);
    return choice;
}

// Typewriter effect
async function typeTo(el, text) {
    const speed = 12 + Math.random() * 10;
    el.classList.remove("fade-in");
    el.innerHTML = "";
    const span = document.createElement("span");
    span.className = "type";
    el.appendChild(span);

    for (let i = 0; i <= text.length; i++) {
        span.innerHTML = text.slice(0, i) + '<span class="cursor">█</span>';
        await new Promise(r => setTimeout(r, speed));
    }
    el.classList.add("fade-in");
}

// Cycle logic
let last = { read: null, xform: null, write: null };
async function shuffleAll() {
    const modalities = Object.keys(MODALITIES);
    const read = pick(modalities, last.read);
    const modality = MODALITIES[read];
    const transforms = Object.keys(modality.transforms);
    const xform = pick(transforms, last.xform);
    const writes = modality.transforms[xform];
    const write = pick(writes, last.write);
    last = { read, xform, write };

    await Promise.all([
        typeTo(els.read, read),
        typeTo(els.xform, xform),
        typeTo(els.write, write)
    ]);
}

// Auto-advance with delay
async function runCycle() {
    await shuffleAll();
    await new Promise(resolve => setTimeout(resolve, 3000));
}

// Start the cycle
runCycle();
setInterval(runCycle, 4600);
</script>

## Why Daft?

**:octicons-image-24: Unified multimodal data processing**

Break down data silos with a single framework that handles structured tables, unstructured text, and rich media like images—all with the same intuitive API. Why juggle multiple tools when one can do it all?

**:material-language-python: Python-native, no JVM required**

Built for modern AI/ML workflows with Python at its core and Rust under the hood. Skip the JVM complexity, version conflicts, and memory tuning to achieve 20x faster start times—get the performance without the Java tax.

**:fontawesome-solid-laptop: Seamless scaling, from laptop to cluster**

Start local, scale global—without changing a line of code. Daft's Rust-powered engine delivers blazing performance on a single machine and effortlessly extends to distributed clusters when you need more horsepower.

## Key Features

* **Native Multimodal Processing**: Process any data type—from structured tables to unstructured text and rich media—with native support for images, embeddings, and tensors in a single, unified framework.

* **Rust-Powered Performance**: Experience breakthrough speed with our Rust foundation delivering vectorized execution and non-blocking I/O that processes the same queries with 5x less memory while consistently outperforming industry standards by an order of magnitude.

* **Seamless ML Ecosystem Integration**: Slot directly into your existing ML workflows with zero friction—whether you're using [PyTorch](https://pytorch.org/), [NumPy](https://numpy.org/), [Pandas](https://pandas.pydata.org/), or [HuggingFace models](https://huggingface.co/models), Daft works where you work.

* **Universal Data Connectivity**: Access data anywhere it lives—cloud storage ([S3](https://aws.amazon.com/s3/), [Azure](https://azure.microsoft.com/en-us/), [GCS](https://cloud.google.com/storage)), modern table formats ([Apache Iceberg](https://iceberg.apache.org/), [Delta Lake](https://delta.io/), [Apache Hudi](https://hudi.apache.org/)), or enterprise catalogs ([Unity](https://www.unitycatalog.io/), [AWS Glue](https://aws.amazon.com/glue/))—all with zero configuration.

* **Push your code to your data**: Bring your Python functions directly to your data with zero-copy UDFs powered by [Apache Arrow](https://arrow.apache.org/), eliminating data movement overhead and accelerating processing speeds.

* **Out of the Box reliability**: Deploy with confidence—intelligent memory management prevents OOM errors while sensible defaults eliminate configuration headaches, letting you focus on results, not infrastructure.

!!! tip "Looking to get started with Daft ASAP?"

    The Daft Guide is a useful resource to take deeper dives into specific Daft concepts, but if you are ready to jump into code you may wish to take a look at these resources:

    1. [Quickstart](quickstart.md): Itching to run some Daft code? Hit the ground running with our 10 minute quickstart notebook.

    2. [API Documentation](api/index.md): Searchable documentation and reference material to Daft’s public API.

## Contribute to Daft

If you're interested in hands-on learning about Daft internals and would like to contribute to our project, join us [on GitHub](https://github.com/Eventual-Inc/Daft) 🚀

Take a look at the many issues tagged with `good first issue` in our repo. If there are any that interest you, feel free to chime in on the issue itself or join us in our [Distributed Data Slack Community](https://join.slack.com/t/dist-data/shared_invite/zt-2e77olvxw-uyZcPPV1SRchhi8ah6ZCtg) and send us a message in #daft-dev. Daft team members will be happy to assign any issue to you and provide any guidance if needed!

<!-- ## Frequently Asked Questions

todo(docs - jay): Add answers to each and more questions if necessary

??? quote "What does Daft do well? (or What should I use Daft for?)"

    todo(docs): this is from 10 min quickstart, filler answer for now

    Daft is the right tool for you if you are working with:

    - **Large datasets** that don't fit into memory or would benefit from parallelization
    - **Multimodal data types** such as images, JSON, vector embeddings, and tensors
    - **Formats that support data skipping** through automatic partition pruning and stats-based file pruning for filter predicates
    - **ML workloads** that would benefit from interact computation within a DataFrame (via UDFs)

??? quote "What should I *not* use Daft for?"

??? quote "How do I know if Daft is the right framework for me?"

    See [DataFrame Comparison](resources/dataframe_comparison.md)

??? quote "What is the difference between Daft and Ray?"

??? quote "What is the difference between Daft and Spark?"

??? quote "How does Daft perform at large scales vs other data engines?"

    See [Benchmarks](resources/benchmarks/tpch.md)

??? quote "What is the technical architecture of Daft?"

    See [Technical Architecture](resources/architecture.md)

??? quote "Does Daft perform any telemetry?"

    See [Telemetry](resources/telemetry.md) -->
