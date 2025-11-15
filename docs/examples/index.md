# Examples


<div class="container">

  <!-- Feature Card -->
  <a href="./voice_ai_analytics" class="card feature-card">
      <div class="card-header">
          <div class="card-tag">TRANSCRIPTION</div>
          <div class="card-spacer"></div>
      </div>
      <div class="card-title">
          <p>Voice AI Analytics with Faster-Whisper and embed_text</p>
      </div>
      <div class="card-body"></div>
      <div class="card-description">
          <p>Transcribe audio files into segments with timestamps and embed content. </p>
      </div>
  </a>

  <!-- HTML/TEXT Card -->
  <a href="./minhash-dedupe" class="card html-text-card">
      <div class="card-header">
          <div class="card-tag">HTML/TEXT</div>
          <div class="card-spacer"></div>
      </div>
      <div class="card-title card-title-standard">
          <p>MinHash Deduplication on Common Crawl</p>
      </div>
      <div class="card-description card-description-standard">
          <p>Clean web text at scale with MinHash, LSH Banding, and Connected Components.</p>
      </div>
  </a>

  <!-- Documents Card -->
  <a href="./common-crawl-daft-tutorial" class="card documents-card">
      <div class="card-header">
          <div class="card-tag">COMMON CRAWL</div>
          <div class="card-spacer"></div>
      </div>
      <div class="card-title card-title-standard">
          <p>Getting Started with Common Crawl</p>
      </div>
      <div class="card-description card-description-standard">
          <p>Daft provides a simple, performant, and responsible way to access Common Crawl data.</p>
      </div>
  </a>

  <!-- Audio Card -->
  <a href="./audio-transcription" class="card audio-card">
      <div class="card-header">
          <div class="card-tag">AUDIO</div>
          <div class="card-spacer"></div>
      </div>
      <div class="card-title card-title-standard">
          <p>Audio Transcription with Whisper</p>
      </div>
      <div class="card-description card-description-standard">
          <p>Effortlessly transcribe audio to text at scale</p>
      </div>
  </a>

  <!-- Text Embeddings Card -->
  <a href="./text-embeddings" class="card text-embeddings-card">
      <div class="card-header">
          <div class="card-tag">TEXT EMBEDDINGS</div>
          <div class="card-spacer"></div>
      </div>
      <div class="card-title card-title-standard">
          <p>Build a 100% GPU Utilization Text Embedding Pipeline featuring spaCy and Turbopuffer</p>
      </div>
      <div class="card-description card-description-standard">
          <p>Generate and store millions of text embeddings in vector databases using distributed GPU processing and state-of-the-art models.</p>
      </div>
  </a>

  <!-- Inference Card -->
  <a href="./image-generation" class="card inference-card">
      <div class="card-header">
          <div class="card-tag">INFERENCE</div>
          <div class="card-spacer"></div>
      </div>
      <div class="card-title card-title-standard">
          <p>Generate Images with Stable Diffusion</p>
      </div>
      <div class="card-description card-description-standard">
          <p>Open Source image generation model on your own GPUs using Daft UDFs</p>
      </div>
  </a>

  <!-- Window Functions Card -->
  <a href="./window-functions" class="card window-functions-card">
      <div class="card-header">
          <div class="card-tag">WINDOW FUNCTIONS</div>
          <div class="card-spacer"></div>
      </div>
      <div class="card-title card-title-standard">
          <p>Window Functions: The Great Chocolate Race</p>
      </div>
      <div class="card-description card-description-standard">
          <p>Transforming complex analytical challenges into elegant solutions</p>
      </div>
  </a>

  <!-- Datasets Card -->
  <a href="./llms-red-pajamas" class="card datasets-card">
      <div class="card-header">
          <div class="card-tag">DATASETS</div>
          <div class="card-spacer"></div>
      </div>
      <div class="card-title card-title-standard">
          <p>Running LLMs on the Red Pajamas Dataset</p>
      </div>
      <div class="card-description card-description-standard">
          <p>Perform similarity search on Stack Exchange questions using language models and embeddings.</p>
      </div>
  </a>

</div>

<style>
    * {
        margin: 0;
        padding: 0;
        box-sizing: border-box;
    }

    body {
        background: rgba(255, 255, 255, 0.05);
        min-height: 100vh;
        padding: 12px;
    }

    .container {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        grid-template-rows: repeat(4, minmax(0, 1fr));
        gap: 24px;
        width: 100%;
        height: calc(100vh - 24px);
    }

    .card {
        background: #141519;
        border: 2px solid magenta;
        position: relative;
        display: flex;
        flex-direction: column;
        gap: 10px;
        overflow: hidden;
        text-decoration: none;
        color: inherit;
        transition: transform 0.2s ease;
    }

    .card:hover {
        transform: translateY(-2px);
        text-decoration: none;
    }

    .card-header {
        display: flex;
        align-items: stretch;
        gap: 10px;
        flex-shrink: 0;
        width: 100%;
        overflow: hidden;
    }

    .card-tag {
        background: magenta;
        padding: 15px 12px;
        font-family: 'Roboto Mono', monospace;
        font-size: 16px;
        color: black;
        white-space: nowrap;
        line-height: 0;
        display: flex;
        align-items: center;
    }

    .card-spacer {
        flex: 1;
        min-width: 1px;
        min-height: 1px;
    }

    .card-title {
        padding: 10px 24px;
        flex-shrink: 0;
        display: flex;
        flex-direction: column;
        gap: 10px;
    }

    .card-title p {
        font-family: 'Roboto Mono', monospace;
        font-weight: 400;
        font-size: 20px;
        color: white;
        line-height: normal;
    }

    .card-description {
        padding: 10px 24px;
        flex-shrink: 0;
        display: flex;
        flex-direction: column;
        gap: 10px;
    }

    .card-description p {
        font-family: 'Roboto Mono', monospace;
        font-weight: 400;
        font-size: 16px;
        color: #bababa;
        line-height: normal;
    }

    /* Specific card sizing */
    .card-title-standard {
        height: 181px;
    }

    .card-description-standard {
        height: 100px;
    }

    /* Feature card specific styles */
    .feature-card {
        grid-area: 1 / 1 / span 2 / span 2;
    }

    .feature-card .card-title p {
        font-size: 28px;
    }

    .feature-card .card-body {
        flex: 1;
        min-height: 1px;
        min-width: 1px;
    }

    .feature-card .card-description {
        height: 80px;
        justify-content: space-between;
    }

    .feature-card .card-description p {
        flex: 1;
        min-height: 1px;
        min-width: 1px;
    }

    /* Grid positioning */
    .audio-card {
        grid-area: 3 / 1;
    }

    .text-embeddings-card {
        grid-area: 3 / 2 / auto / span 2;
    }

    .documents-card {
        grid-area: 2 / 3;
    }

    .html-text-card {
        grid-area: 1 / 3;
    }

    .inference-card {
        grid-area: 4 / 1;
    }

    .window-functions-card {
        grid-area: 4 / 2;
    }

    .datasets-card {
        grid-area: 4 / 3;
    }

    /* Responsive */
    @media (max-width: 1024px) {
        .container {
            grid-template-columns: 1fr;
            grid-template-rows: auto;
            height: auto;
            padding: 12px 12px;
        }

        .card {
            grid-area: auto !important;
        }

        .feature-card .card-title p {
            font-size: 24px;
        }

        .card-title p {
            font-size: 18px;
        }
    }
</style>


!!! Note
    For more examples, check out our new [daft-examples](https://github.com/Eventual-Inc/daft-examples) repository!
