# Using Daft to Generate Images from Text with Stable Diffusion

[Run this tutorial on Google Colab](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/text_to_image/text_to_image_generation.ipynb)

In this tutorial, we will be using the Stable Diffusion model to generate images from text. We will explore how to use GPUs with Daft to accelerate computations.

To run this tutorial, you will need access to a GPU. If you are on Google Colab, you may switch to a GPU runtime by going to the menu `Runtime -> Change runtime type -> Hardware accelerator -> GPU -> Save`.

Let's get started!

## Setting Up

First, let's install some dependencies.

```bash
pip install daft --pre --extra-index-url https://pypi.anaconda.org/daft-nightly/simple
pip install transformers diffusers accelerate torch Pillow
```

Next, let's load a Parquet file into Daft. This particular file is hosted in HuggingFace at a https URL.

```python
import daft

# Flip this flag if you want to see the performance of running on CPU vs GPU
USE_GPU = True
IO_CONFIG = daft.io.IOConfig(
    s3=daft.io.S3Config(anonymous=True, region_name="us-west-2")
)  # Use anonymous-mode for accessing AWS S3
PARQUET_PATH = "s3://daft-public-data/tutorials/laion-parquet/train-00000-of-00001-6f24a7497df494ae.parquet"

parquet_df = daft.read_parquet(PARQUET_PATH, io_config=IO_CONFIG)
```

Let's go ahead and `.collect()` this DataFrame. This will download the Parquet file and materialize the data in memory so that all our subsequent operations will be cached!

```python
parquet_df.collect()
parquet_df = parquet_df.select(parquet_df["URL"], parquet_df["TEXT"], parquet_df["AESTHETIC_SCORE"])
```

## Downloading Images

Like many datasets, instead of storing the actual images in the dataset's files it looks like the Dataset authors have instead opted to store a URL to the image.

Let's use Daft's builtin functionality to download the images and open them as PIL Images - all in just a few lines of code!

```python
# Filter for images with longer descriptions
parquet_df_with_long_strings = parquet_df.where(parquet_df["TEXT"].str.length() > 50)

# Download images
images_df = (
    parquet_df_with_long_strings.with_column(
        "image",
        parquet_df_with_long_strings["URL"].url.download(on_error="null").image.decode(on_error="null"),
    )
    .limit(5)
    .where(daft.col("image").not_null())
)
images_df.show(5)
```

Great! Now we have a pretty good idea of what our dataset looks like.

## Running the Stable Diffusion model on a GPU using Daft UDFs

Let's now run the Stable Diffusion model over the `"TEXT"` column, and generate images for those texts!

Using GPUs with Daft UDFs is simple. Just specify `num_gpus=N`, where `N` is the number of GPUs that your UDF is going to use.

```python
import torch
from diffusers import StableDiffusionPipeline


@daft.udf(return_dtype=daft.DataType.python())
class GenerateImageFromText:
    def __init__(self):
        model_id = "runwayml/stable-diffusion-v1-5"
        self.pipe = StableDiffusionPipeline.from_pretrained(
            model_id,
            torch_dtype=torch.float32,
        )
        self.pipe.enable_attention_slicing(1)

    def generate_image(self, prompt):
        return self.pipe(prompt, num_inference_steps=20, height=512, width=512).images[0]

    def __call__(self, text_col):
        return [self.generate_image(t) for t in text_col]


if USE_GPU:
    GenerateImageFromText = GenerateImageFromText.override_options(num_gpus=1)

images_df.with_column(
    "generated_image",
    GenerateImageFromText(images_df["TEXT"]),
).show(1)
```
