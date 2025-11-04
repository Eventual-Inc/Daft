# Daft is built to work with **any modality**.

In modern AI systems, data isn’t just numbers in tables anymore - it shows up as text, images, audio, PDFs, embeddings, and beyond. **Modalities** are not *types* of data, but data with different *meanings* and *intentions* around what you want to do with the data.

> An image isn’t just pixel values — it's visual meaning you can extract features from, or transform into embeddings.

> A PDF isn’t just a blob of bytes — it's structured text and images.

> A URL isn’t just a string — it's a pointer to data stored somewhere.

Handling diverse modalities unlocks more value from more sources. And by using a single engine, you can process them all—seamlessly and efficiently—in a single pipeline. Easy to develop, and even easier to run at scale.

Some of Daft's supported modalities include:

- **[Text](text.md)**: Summarize, Embed,
- **[Images](images.md)**: Work with visual data and image processing.
- **[Audio](audio.md)**: Transcribe audio files speech with ease
- **[Videos](videos.md)**: Working with videos.
- **[PDFs](../examples/document-processing.md)**: Extract text and image data from PDF documents.
- **[JSON and Nested Data](json.md)**: Parse, query, and manipulate semi-structured and hierarchical data.
- **[Paths, URLs, & Files](urls.md)**: Discover, download, and read files from URLs and paths from remote resources.
- **Embeddings** (User Guide Coming Soon): Generate vector representations for similarity search and machine learning.
- **Tensors and Sparse Tensors** (User Guide Coming Soon): Multi-dimensional numerical data for deep learning workflows.


## **[Custom Modalities](custom.md)**

The most important modality might be one we haven’t explored yet. Daft makes it easy to define your own modality with [custom connectors](../connectors/custom.md) to read and write any kind of data, and use [custom Python code](../custom-code/index.md) to process it efficiently and reliably, even at scale.
