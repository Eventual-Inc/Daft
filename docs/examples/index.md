# Examples

<div class="examples-grid">
  <div class="example-card">
    <a href="./document-processing" class="example-image-link">
      <div class="example-image">
        <img src="../img/document-processing-cover.jpg" alt="Document Processing">
        <div class="example-overlay">
          <h3>Document Processing</h3>
          <p>Load PDFs from S3, extract text, run layout analysis, and compute embeddings</p>
        </div>
      </div>
    </a>
  </div>

  <div class="example-card">
    <a href="./audio-transcription" class="example-image-link">
      <div class="example-image">
        <img src="../img/audio-transcription-cover.jpg" alt="Audio Transcription">
        <div class="example-overlay">
          <h3>Audio Transcription with Whisper</h3>
          <p>Effortlessly convert audio to text at scale</p>
        </div>
      </div>
    </a>
  </div>

  <div class="example-card">
    <a href="./text-embeddings" class="example-image-link">
      <div class="example-image">
        <img src="../img/text-embeddings-cover.jpg" alt="Text Embeddings" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
        <div class="example-placeholder" style="display: none; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);">
          <span>📊</span>
        </div>
        <div class="example-overlay">
          <h3>Text Embeddings for Turbopuffer</h3>
          <p>Generate embeddings on text to store in vector databases</p>
        </div>
      </div>
    </a>
  </div>

  <div class="example-card">
    <a href="./llms-red-pajamas" class="example-image-link">
      <div class="example-image">
        <img src="../img/llms-red-pajamas-cover.jpg" alt="LLMs on Red Pajamas" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
        <div class="example-placeholder" style="display: none; background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);">
          <span>🤖</span>
        </div>
        <div class="example-overlay">
          <h3>LLMs on Hugging Face Datasets</h3>
          <p>Load Red Pajamas dataset and perform similarity search</p>
        </div>
      </div>
    </a>
  </div>

  <div class="example-card">
    <a href="./image-generation" class="example-image-link">
      <div class="example-image">
        <img src="../img/image-generation-cover.jpg" alt="Image Generation" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
        <div class="example-placeholder" style="display: none; background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);">
          <span>🎨</span>
        </div>
        <div class="example-overlay">
          <h3>Generate Images with Stable Diffusion</h3>
          <p>Using text prompts with deep learning models</p>
        </div>
      </div>
    </a>
  </div>

  <div class="example-card">
    <a href="./querying-images" class="example-image-link">
      <div class="example-image">
        <img src="../img/querying-images-cover.jpg" alt="Image Querying" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
        <div class="example-placeholder" style="display: none; background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);">
          <span>🔍</span>
        </div>
        <div class="example-overlay">
          <h3>Query Images</h3>
          <p>Retrieve the top N "reddest" images from the Open Images dataset</p>
        </div>
      </div>
    </a>
  </div>

  <div class="example-card">
    <a href="./mnist" class="example-image-link">
      <div class="example-image">
        <img src="../img/mnist-cover.jpg" alt="MNIST Classification" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
        <div class="example-placeholder" style="display: none; background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);">
          <span>🔢</span>
        </div>
        <div class="example-overlay">
          <h3>MNIST Digit Classification</h3>
          <p>Run classification with deep learning</p>
        </div>
      </div>
    </a>
  </div>

  <div class="example-card">
    <a href="./window-functions" class="example-image-link">
      <div class="example-image">
        <img src="../img/window-functions-cover.jpg" alt="Window Functions" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
        <div class="example-placeholder" style="display: none; background: linear-gradient(135deg, #ffecd2 0%, #fcb69f 100%);">
          <span>📈</span>
        </div>
        <div class="example-overlay">
          <h3>Window Functions</h3>
          <p>Efficient window functions for ranking, computing deltas, and tracking cumulative sums</p>
        </div>
      </div>
    </a>
  </div>
</div>

<style>
.examples-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 0.5rem;
  margin: 0.5rem 0;
}

.example-card {
  border-radius: 12px;
  overflow: hidden;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
  transition: transform 0.2s ease, box-shadow 0.2s ease;
  background: white;
}

.example-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.15);
}

.example-image {
  position: relative;
  height: 380px;
  overflow: hidden;
}

.example-image img {
  width: 100%;
  height: 100%;
  object-fit: cover;
  transition: transform 0.3s ease;
}

.example-placeholder {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 3rem;
  color: white;
  text-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
}

.example-image::after {
  content: "";
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  z-index: 1;
  pointer-events: none;
}

.example-card:hover .example-image img {
  transform: scale(1.05);
}

.example-image-link {
  display: block;
  text-decoration: none;
  color: inherit;
  cursor: pointer;
}

.example-image-link:hover {
  text-decoration: none;
  color: inherit;
}

.example-overlay {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  background: linear-gradient(to top, rgba(0, 0, 0, 0.8) 12%, rgba(0, 0, 0, 0.4) 35%, rgba(0, 0, 0, 0) 65%);
  color: white;
  padding: 1.5rem 1rem 1rem;
  display: flex;
  flex-direction: column;
  justify-content: flex-end;
  z-index: 2;
}

.example-overlay h3 {
  margin: 0 0 0.5rem 0;
  font-size: 1rem;
  font-weight: 600;
}

.example-overlay p {
  margin: 0;
  font-size: 0.8rem;
  opacity: 0.9;
  line-height: 1.4;
}

@media (max-width: 768px) {
  .examples-grid {
    grid-template-columns: 1fr;
    gap: 1.5rem;
  }

  .example-image {
    height: 180px;
  }

  .example-overlay {
    padding: 1rem 0.75rem 0.75rem;
  }

  .example-overlay h3 {
    font-size: 1rem;
  }

  .example-overlay p {
    font-size: 0.65rem;
  }
}
</style>
