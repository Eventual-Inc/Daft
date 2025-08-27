# Overview

Daft is a high-performance data engine providing simple and reliable data processing for any modality and scale.

Raw data collected from application systems is messy, unstructured and multimodal.

Daft is used to build efficient data pipelines involving heavy transformations:

* Models on GPUs
* Rate-limited external APIs
* Custom user logic

Data lands in specialized data systems, which can then power workloads such as:

* Distributed model training (AWS S3 Object Storage + `.tfrecord` files)
* Analytics (Data Warehouses/Lakehouses)
* Search (Vector Databases, Fulltext Search Systems)
* Web Applications (SQL Databases)

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

  .daft-pipeline-component .pipeline {
    width: 100%;
    max-width: 1100px;
    padding: 28px;
    margin: 0 auto;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 14px;
  }

  .daft-pipeline-component .label {
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: .1em;
    color: #b9c8ff;
    font-size: clamp(10px, 1.2vw, 12px);
  }

  .daft-pipeline-component .token {
    position: relative;
    width: 200px;
    height: 80px;
    border-radius: 14px;
    padding: 12px 16px;
    background: linear-gradient(180deg, rgba(255,255,255,.04), rgba(255,255,255,.02));
    border: 1px solid rgba(255,255,255,.1);
    font-family: var(--mono);
    font-size: clamp(14px, 2vw, 18px);
    line-height: 1.4;
    color: var(--ink);
    overflow: hidden;
    word-wrap: break-word;
    white-space: normal;
    display: flex;
    align-items: center;
  }

  .daft-pipeline-component .arrow {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 40px;
    height: 40px;
    border-radius: 999px;
    background: linear-gradient(180deg,#21306b,#1b2656);
    border: 1px solid rgba(255,255,255,.12);
    flex: 0 0 40px;
    box-shadow: inset 0 1px 0 rgba(255,255,255,.08);
  }

  .daft-pipeline-component .arrow svg {
    width: 20px;
    height: 20px;
    fill: #cfe0ff;
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
    .daft-pipeline-component .pipeline {
      flex-direction: column;
      gap: 20px;
      align-items: stretch;
    }
    .daft-pipeline-component .token {
      width: 100%;
      max-width: none;
    }
    .daft-pipeline-component .arrow {
      display: none;
    }
  }
</style>

<div class="daft-pipeline-component">
  <div class="pipeline" id="pipe" role="region" aria-label="Animated data pipeline">
      <div class="group">
        <div class="label">Read</div>
        <div class="token"><span id="read" class="swap"></span></div>
      </div>

      <span class="arrow" aria-hidden="true">
        <svg viewBox="0 0 24 24"><path d="M12 4l1.41 1.41L8.83 10H20v2H8.83l4.58 4.59L12 18l-8-8 8-8z" transform="rotate(180 12 12)"/></svg>
      </span>

      <div class="group">
        <div class="label">Transform</div>
        <div class="token"><span id="xform" class="swap"></span></div>
      </div>

      <span class="arrow" aria-hidden="true">
        <svg viewBox="0 0 24 24"><path d="M12 4l1.41 1.41L8.83 10H20v2H8.83l4.58 4.59L12 18l-8-8 8-8z" transform="rotate(180 12 12)"/></svg>
      </span>

      <div class="group">
        <div class="label">Write</div>
        <div class="token"><span id="write" class="swap"></span></div>
      </div>
    </div>
</div>

<script>

// Hierarchical data buckets by modality with transform-to-write mappings
const MODALITIES = {
  "Images (JPEG)": {
    transforms: {
      "OCR for text extraction": ["Fulltext Search"],
      "Image captioning with LLM": ["SQL Database"],
      "Object detection": ["SQL Database"],
      "Generate embeddings": ["Vector DB"]
    }
  },
  "PDFs": {
    transforms: {
      "OCR for text extraction": ["Fulltext Search"],
      "Structured data extraction": ["Data Warehouse"],
      "Generate embeddings": ["Vector DB"],
      "PII detection": ["Data Warehouse"],
      "Chunking + deduplication": ["Data Lake"]
    }
  },
  "Video (MP4/MKV)": {
    transforms: {
      "Video captioning": ["SQL Database"],
      "Scene detection": ["S3 Object Store"],
      "Audio transcription": ["SQL Database"],
      "Generate embeddings": ["Vector DB"]
    }
  },
  "Audio (WAV/MP3/FLAC)": {
    transforms: {
      "Transcription with Whisper": ["Fulltext Search"],
      "Speaker identification": ["SQL Database"],
      "Emotion detection": ["Data Warehouse"],
      "Generate embeddings": ["Vector DB"]
    }
  },
  "AI Agent Logs": {
    transforms: {
      "LLM summarization": ["SQL Database"],
      "Generate embeddings": ["Vector DB"]
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
