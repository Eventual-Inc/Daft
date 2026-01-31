# Modalities Overview

**Daft is designed to work with any modality.**

Artificial Intelligence now natively understands text, images, audio, video, and documents, but legacy engines were never designed to feed these formats to large models. Daft closes that gap, giving you one distributed engine that processes any modality, respects memory limits, and keeps GPUs fed so you can build the pipeline once and scale it anywhere.


<div class="grid cards" markdown>

- 🔠 [**Text**](text.md)

    Normalize, chunk, dedupe, prompt, and embed text data.

- 🌄 [**Images**](images.md)

    Work with visual data and image processing.

- 🔉 [**Audio**](audio.md)

    Read, extract metadata, resample audio files.

- 🎥 [**Video**](videos.md)

    Working with video files and metadata.

- 📄 [**Documents**](documents.md)

    Extract text and image data from PDF documents.

- {} [**JSON and Nested Data**](json.md)

    Parse, query, and manipulate semi-structured and hierarchical data.

- ⊹ [**Embeddings**](embeddings.md)

    Generate vector representations for RAG and AI search.

- 📁 [**Generic Files and URLs**](files.md)

    Take advantage of Daft's built-in URL functions and `daft.File` types

</div>

### **[Custom Modalities](custom.md)**

The most important modality might be one we haven't explored yet. Daft makes it easy to define your own modality with [custom connectors](../connectors/custom.md) to read and write any kind of data, and use [user-defined functions](../custom-code/index.md) to process custom Python code efficiently and reliably at scale.
