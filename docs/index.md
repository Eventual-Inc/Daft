# Introduction

Daft is a high-performance data engine providing simple and reliable data processing for any modality and scale.

<style>
  .daft-pipeline-component {
    color: #1a1a1a;
    font-family: Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial, "Apple Color Emoji", "Segoe UI Emoji";
    margin: 20px 0;
  }

  /* Dark mode overrides */
  @media (prefers-color-scheme: dark) {
    .daft-pipeline-component {
      color: #f4f7ff;
    }
    .daft-pipeline-component .stage-header {
      color: #c7d2fe;
    }
    .daft-pipeline-component .description p {
      color: #f4f7ff;
    }
    .daft-pipeline-component .description p:last-child {
      color: rgba(199, 210, 254, 0.7);
    }
    .daft-pipeline-component .source-comment {
      color: rgba(199, 210, 254, 0.7);
    }
    .daft-pipeline-component .pipeline-item {
      background: rgba(255,255,255,.02);
      border-color: rgba(255,255,255,.08);
      color: #f4f7ff;
    }
  }

  /* Material for MkDocs dark mode */
  [data-md-color-scheme="slate"] .daft-pipeline-component {
    color: #f4f7ff;
  }
  [data-md-color-scheme="slate"] .daft-pipeline-component .stage-header {
    color: #c7d2fe;
  }
  [data-md-color-scheme="slate"] .daft-pipeline-component .description p {
    color: #f4f7ff;
  }
  [data-md-color-scheme="slate"] .daft-pipeline-component .description p:last-child {
    color: rgba(199, 210, 254, 0.7);
  }
  [data-md-color-scheme="slate"] .daft-pipeline-component .source-comment {
    color: rgba(199, 210, 254, 0.7);
  }
  [data-md-color-scheme="slate"] .daft-pipeline-component .pipeline-item {
    background: rgba(255,255,255,.02);
    border-color: rgba(255,255,255,.08);
    color: #f4f7ff;
  }

  .daft-pipeline-component .container {
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
    color: #6366f1;
    font-size: clamp(12px, 1.2vw, 14px);
    margin-bottom: 12px;
  }

  .daft-pipeline-component .description {
    padding: 20px;
    display: flex;
    flex-direction: column;
    justify-content: center;
  }

  .daft-pipeline-component .description p {
    margin: 0 0 12px 0;
    line-height: 1.5;
    color: #1a1a1a;
    font-size: 15px;
  }

  .daft-pipeline-component .description p:last-child {
    margin: 0;
    font-size: 13px;
    color: rgba(75, 85, 99, 0.8);
    font-style: italic;
  }

  .daft-pipeline-component .source-comment {
    color: rgba(75, 85, 99, 0.8);
  }

  .daft-pipeline-component .pipeline-item {
    background: rgba(0,0,0,.04);
    border: 1px solid rgba(0,0,0,.15);
    border-radius: 8px;
    padding: 16px 20px;
    display: flex;
    align-items: center;
    min-height: 60px;
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    font-size: clamp(13px, 1.8vw, 16px);
    color: #1a1a1a;
  }

  .daft-pipeline-component .type {
    display: inline-block;
    white-space: normal;
    word-wrap: break-word;
  }

  .daft-pipeline-component .cursor {
    color: #ff00ff;
    animation: daft-caret .9s steps(1,end) infinite;
  }

  @keyframes daft-caret {
    50% { opacity: 0; }
  }

  .daft-pipeline-component .cursor-fade {
    color: #ff00ff;
    animation: daft-cursor-fade 400ms ease-out forwards;
    transform-origin: bottom right;
    display: inline-block;
  }

  @keyframes daft-cursor-fade {
    0% {
      opacity: 1;
      transform: rotate(0deg);
    }
    100% {
      opacity: 0;
      transform: rotate(10deg);
    }
  }

  @media (max-width: 720px) {
    .daft-pipeline-component .stage-box {
      padding: 8px;
    }
    .daft-pipeline-component .row {
      grid-template-columns: 1fr;
      gap: 20px;
    }
    .daft-pipeline-component .pipeline-item {
      min-height: 80px;
      padding: 12px 16px;
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
          <div class="stage-header">READ ANYTHING</div>
          <p>Daft reads raw unstructured and multimodal data collected from application systems</p>
          <p>Object storage (AWS S3, GCS, R2)<br>Event bus (Kafka)<br>Data lake (Iceberg, Deltalake)</p>
        </div>
        <div class="pipeline-item">
          <span id="read" class="swap"></span>
        </div>
      </div>
    </div>

    <div class="stage-box">
      <div class="row">
        <div class="description">
          <div class="stage-header">EXPENSIVE TRANSFORMATIONS</div>
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
          <p>Daft lands data into specialized data systems for downstream use cases</p>
          <p>Search (full-text search and vector DBs)<br>Applications (SQL/NoSQL databases)<br>Analytics (data warehouses)<br>Model training (S3 object storage)</p>
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
  "Images": {
    sources: ["*.jpeg files in S3", "URLs in database", "*.parquet on Huggingface"],
    transforms: {
      "OCR for text extraction": {
        details: ["# use Tesseract OCR engine", "# use Azure Computer Vision API"],
        destinations: {
          "Elasticsearch": "# for full-text search"
        }
      },
      "Image captioning with LLM": {
        details: ["# use qwen model on H100 GPU"],
        destinations: {
          "PostgreSQL": "# for querying by webapps",
          "MongoDB": "# for querying by webapps"
        }
      },
      "Object detection": {
        details: ["# use YOLOv8 model on GPU", "# use Azure Object Detection APIs"],
        destinations: {
          "PostgreSQL": "# for querying by webapps",
          "MySQL": "# for querying by webapps"
        }
      },
      "Generate embeddings": {
        details: ["# use CLIP model on GPU", "# use OpenAI text-embedding-3"],
        destinations: {
          "Turbopuffer": "# for vector search",
          "LanceDB": "# for vector search"
        }
      }
    }
  },
  "Documents": {
    sources: ["*.pdf files in S3", "*.docx files in GCS", "*.html files in R2", "*.parquet on Huggingface"],
    transforms: {
      "OCR for text extraction": {
        details: ["# use Tesseract OCR engine", "# use EasyOCR with GPU acceleration", "# use Azure Computer Vision API"],
        destinations: {
          "Elasticsearch": "# for full-text search"
        }
      },
      "Structured data extraction": {
        details: ["# use OpenAI's API for gpt-4o", "# use Azure Form Recognizer APIs"],
        destinations: {
          "BigQuery": "# for analytics",
          "Snowflake": "# for analytics",
          "Databricks": "# for analytics"
        }
      },
      "Generate embeddings": {
        details: ["# use OpenAI's API for text-embedding-3", "# use sentence-transformers on GPU"],
        destinations: {
          "Turbopuffer": "# for vector search",
          "LanceDB": "# for vector search"
        }
      },
      "PII detection": {
        details: ["# use spaCy NER model", "# use Azure PII detection"],
        destinations: {
          "BigQuery": "# for analytics",
          "Snowflake": "# for analytics",
          "Databricks": "# for analytics"
        }
      },
      "Chunking + deduplication": {
        details: ["# use daft default splitting"],
        destinations: {
          "Parquet": "# for data lake storage"
        }
      }
    }
  },
  "Video": {
    sources: ["*.mp4 files in S3", "URLs in CSVs", "*.parquet on Huggingface"],
    transforms: {
      "Video captioning": {
        details: ["# custom Python code: extract audio and transcribe"],
        destinations: {
          "PostgreSQL": "# for querying by webapps"
        }
      },
      "Scene detection": {
        details: ["# use OpenCV scene detection", "# use PySceneDetect library"],
        destinations: {
          "AWS S3": "# for object storage"
        }
      },
      "Audio transcription": {
        details: ["# use Whisper model on GPU", "# use Azure Speech Services API"],
        destinations: {
          "PostgreSQL": "# for querying by webapps",
          "MongoDB": "# for querying by webapps",
          "Elasticsearch": "# for full-text search"
        }
      },
      "Generate embeddings": {
        details: ["# use CLIP model on CPU", "# use CLIP model on GPU"],
        destinations: {
          "Turbopuffer": "# for vector search",
          "LanceDB": "# for vector search"
        }
      }
    }
  },
  "Audio (WAV/MP3/FLAC)": {
    sources: ["*.wav files in S3", "URLs in database", "*.parquet on Huggingface"],
    transforms: {
      "Transcription with Whisper": {
        details: ["# use Whisper.cpp on CPU", "# use Azure Speech Services API"],
        destinations: {
          "PostgreSQL": "# for querying by webapps",
          "MongoDB": "# for querying by webapps",
          "Elasticsearch": "# for full-text search"
        }
      },
      "Speaker identification": {
        details: ["# use custom Python code with pyannote.audio", "# use Azure Speaker Recognition API"],
        destinations: {
          "PostgreSQL": "# for querying by webapps",
          "MySQL": "# for querying by webapps"
        }
      },
      "Emotion detection": {
        details: ["# use custom Python code with wav2vec2", "# use Azure Emotion API"],
        destinations: {
          "BigQuery": "# for analytics",
          "Snowflake": "# for analytics",
          "Databricks": "# for analytics"
        }
      },
      "Generate embeddings": {
        details: ["# use custom Python code with wav2vec2", "# use OpenAI's endpoint for text-embedding-3"],
        destinations: {
          "Turbopuffer": "# for vector search",
          "LanceDB": "# for vector search"
        }
      }
    }
  },
  "AI Agent Logs": {
    sources: ["JSON logs in Kafka", "JSON-lines in S3"],
    transforms: {
      "LLM summarization": {
        details: ["# use OpenAI gpt-4o endpoint", "# use Claude 3.5 Sonnet API endpoint", "# use custom summarization model on GPUs"],
        destinations: {
          "PostgreSQL": "# for querying by webapps",
          "MySQL": "# for querying by webapps"
        }
      },
      "Generate embeddings": {
        details: ["# use OpenAI's endpoint for text-embedding-3", "# use sentence-transformers on GPUs", "# use BERT model on GPUs"],
        destinations: {
          "Turbopuffer": "# for vector search",
          "LanceDB": "# for vector search"
        }
      }
    }
  }
};

// Helpers
const q = (id) => document.getElementById(id);

// Wait for DOM to be ready
function waitForElements() {
    const els = { read: q("read"), xform: q("xform"), write: q("write") };
    if (els.read && els.xform && els.write) {
        return els;
    }
    return null;
}

function pick(list, last) {
    if (list.length < 2) return list[0];
    let choice;
    do choice = list[(Math.random() * list.length) | 0];
    while (choice === last);
    return choice;
}

// Typewriter effect
async function typeTo(el, text) {
    if (!el) return; // Safety check for null elements

    const speed = 12 + Math.random() * 10;
    el.innerHTML = "";
    const span = document.createElement("span");
    span.className = "type";
    el.appendChild(span);

    for (let i = 0; i <= text.length; i++) {
        const currentText = text.slice(0, i);
        const lines = currentText.split('\n');
        const formattedLines = lines.map(line => {
            if (line.startsWith('# ')) {
                return `<span class="source-comment">${line}</span>`;
            }
            return line;
        });
        span.innerHTML = formattedLines.join('\n') + '<span class="cursor">█</span>';
        await new Promise(r => setTimeout(r, speed));
    }

    // Let cursor blink for a moment, then fade away
    const cursor = span.querySelector('.cursor');
    if (cursor) {
        setTimeout(() => {
            cursor.classList.remove('cursor');
            cursor.classList.add('cursor-fade');
        }, 2200); // Keep blinking for N seconds before fading
    }
}

// Cycle logic
let last = { read: null, xform: null, write: null, source: null, detail: null };
async function shuffleAll() {
    const els = waitForElements();
    if (!els) return; // Exit if elements aren't ready

    const modalities = Object.keys(MODALITIES);
    const read = pick(modalities, last.read);
    const modality = MODALITIES[read];
    const source = pick(modality.sources, last.source);
    const transforms = Object.keys(modality.transforms);
    const xform = pick(transforms, last.xform);
    const transformData = modality.transforms[xform];
    const detail = pick(transformData.details, last.detail);
    const destinations = Object.keys(transformData.destinations);
    const write = pick(destinations, last.write);
    const writeUseCase = transformData.destinations[write];
    last = { read, xform, write, source, detail };

    await Promise.all([
        typeTo(els.read, read + "\n" + "# " + source),
        typeTo(els.xform, xform + "\n" + detail),
        typeTo(els.write, write + "\n" + writeUseCase)
    ]);
}

// Auto-advance with delay
async function runCycle() {
    await shuffleAll();
    await new Promise(resolve => setTimeout(resolve, 8000));
}

// Start the cycle
runCycle();
setInterval(runCycle, 4600);
</script>

## Why Daft?

**:octicons-image-24: Unified multimodal data processing**

While traditional dataframes struggle with anything beyond tables, Daft natively handles tables, images, text, and embeddings through a single Python API. No more stitching together specialized tools for different data types.

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

    2. [Examples](examples/index.md): See Daft in action with use cases across text, images, audio, and more.

    3. [API Documentation](api/index.md): Searchable documentation and reference material to Daft's public API.

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
