# Working with Different Modalities in Daft

Data isn’t just numbers in tables anymore. In modern AI systems, it shows up as text, images, audio, PDFs, embeddings, and more. We call these **modalities**: not just different *types* of data, but data with different *meanings* and *intentions* around what you want to do with the data.

> An image isn’t just pixel values—it's visual meaning you can extract features from, or transform into embeddings.

>A PDF isn’t just a blob of bytes—it's structured text and images.

>A URL isn’t just a string—it's a pointer to data stored somewhere.

Daft is built to work with **any modality**. That means not just loading the data, but helping you *do what you want* with the data.

Handling diverse modalities unlocks more value from more sources. And by using a single engine, you can process them all—seamlessly and efficiently—in a single pipeline. Easy to develop, and even easier to run at scale.

Some of Daft's supported modalities include:

- **[Custom Modalities](custom.md)**: The most important modality might be one we haven’t explored yet—but *you* can. Daft makes it easy to define your own modality: build [custom connectors](../connectors/custom.md) to read and write any kind of data, and use [custom Python code](../custom-code/index.md) to process it efficiently and reliably, even at scale.
- **[URLs and Files](urls.md)**: Handle file paths, URLs, and remote resources.
- **[Text](text.md)**: Process and analyze textual content.
- **[Images](images.md)**: Work with visual data and image processing.
- **[Videos](videos.md)**: Working with videos.
- **[JSON and Nested Data](json.md)**: Handle semi-structured and hierarchical data.
- **PDFs (user guide coming soon)**: Extract text and image data from PDF documents.
- **Embeddings (user guide coming soon)**: Generate vector representations for similarity search and machine learning.
- **Tensors and Sparse Tensors (user guide coming soon)**: Multi-dimensional numerical data for deep learning workflows.
