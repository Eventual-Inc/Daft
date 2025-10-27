# Working with Custom Modalities

> Working with 3D point clouds? Audio files? RNA sequences? Regardless of what kind of data you're working with, Daft can help you bring it into your pipeline.

AI isn’t just about having data—it's about doing the *right thing* with your data. That means treating your domain-specific formats with the same semantic richness as more common modalities like images or text.

Custom modalities let you define:

- How data is loaded or saved to storage

- How it is processed or transformed within your pipeline

- What it means, and how to act on it programmatically

With Daft, you can make your unique data behave like a first-class citizen in your pipeline in a scalable, debuggable, and composable way.

## Two Ways to Define Custom Modalities in Daft

Daft supports custom modalities through two complementary mechanisms.

### 🔌 Custom Connectors

Define how your data should be read from or written to any source. Whether you're pulling from a proprietary format or working with specialized domain-specific data, custom connectors let you integrate this data seamlessly into your pipeline.

See the guide on writing [Custom Connectors](../connectors/custom.md).

### 🐍 Running Custom Python Code

Once your data is in the pipeline, use Daft’s support for custom Python transformations to process it however you want. These user-defined functions (UDFs) are:

- Seamlessly parallelizable
- Dynamically sizable based on data volumes and memory consumption
- Allow you to work easily with GPUs
- Make calls to external APIs

See the guide on [Running Custom Python Code](../custom-code/index.md).

<!-- ### A Concrete Example (coming soon) -->

### Real-World Example: Storing Sparse Image Tensors

In [this article by Mobileye](https://medium.com/@sageahrac/cracking-the-code-a-smarter-way-to-store-sparse-data-23d28363829b), the team uses Daft to store and process sparse tensors derived from self-driving camera data. This custom modality helped them achieve a **500×** reduction in storage size, and a **12.45x** improvement in read throughput.

### Your Modality, Your Pipeline

Daft is built to be open-ended. We don’t assume your data fits into something that anyone else has seen before.

Whether you're working with:

- Sparse embeddings from a biology lab

- Multichannel audio with overlapping sources

- Raw scientific instrumentation logs

Daft gives you the tools to treat that data as a real modality, not just another weird binary column. As a wise man once said:

> Be like water making its way through cracks. Do not be assertive, but adjust to the object, and you shall find a way around or through it. If nothing within you stays rigid, outward things will disclose themselves.
>
> Empty your mind, be formless. Shapeless, like water.
>
> If you put water into a cup, it becomes the cup.
>
> You put water into a bottle and it becomes the bottle.
>
> You put it in a teapot, it becomes the teapot.
>
> Now, water can flow or it can crash. Be water, my friend.
>
> — Bruce Lee
