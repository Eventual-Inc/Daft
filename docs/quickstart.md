# Quickstart

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/tools%2Fdocs-to-notebook-converter/docs/notebooks/quickstart.ipynb)

<!--
todo(docs - jay): Incorporate SQL examples

todo(docs): Add link to notebook to DIY (notebook is in mkdocs dir, but idk how to host on colab)

todo(docs): What does the actual output look like for some of these examples? should we update it visually?
-->

Daft is the best multimodal data processing engine that allows you to load data from anywhere, transform it with a powerful DataFrame API and AI functions, and store it in your destination of choice. In this quickstart, you'll see what this looks like in practice with a realistic e-commerce data workflow.

### Requirements

Daft requires **Python 3.10 or higher**.

### Install Daft

You can install Daft using `pip`. Run the following command in your terminal or notebook:

```bash
pip install -U "daft[openai]"  # Includes OpenAI extras needed for this quickstart
```

Additionally, install these packages for image processing (used later in this quickstart):

```bash
pip install numpy pillow
```

<!-- For more advanced installation options, please see [Installation](install.md). -->

### Load Your Data

Let's start by loading an e-commerce dataset from Hugging Face. [This dataset](https://huggingface.co/datasets/calmgoose/amazon-product-data-2020) contains 10,000+ Amazon products from diverse categories including electronics, toys, home goods, and more. Each product includes details like names, prices, descriptions, technical specifications, and product images.

```python
import daft

df_original = daft.read_huggingface("calmgoose/amazon-product-data-2020")
```

!!! note "Load from anywhere"

    Daft can load data from many sources including [S3](connectors/aws.md), [Iceberg](connectors/iceberg.md), [Delta Lake](connectors/delta_lake.md), [Hudi](connectors/hudi.md), and [more](connectors/index.md). We're using Hugging Face here as a demonstration.

### Inspect Your Data

Now let's take a look at what we loaded. You can inspect the DataFrame by simply printing it:

```python
df_original
```

``` {title="Output"}
╭─────────┬──────────────┬──────────┬────────────┬──────────┬─────────────┬──────────────────╮
│ Uniq Id ┆ Product Name ┆ Category ┆      …     ┆ Variants ┆ Product Url ┆ Is Amazon Seller │
│ ---     ┆ ---          ┆ ---      ┆            ┆ ---      ┆ ---         ┆ ---              │
│ String  ┆ String       ┆ String   ┆ (9 hidden) ┆ String   ┆ String      ┆ String           │
╰─────────┴──────────────┴──────────┴────────────┴──────────┴─────────────┴──────────────────╯

(No data to display: Dataframe not materialized, use .collect() to materialize)
```

You see the above output because **Daft is lazy by default** - it displays the schema (column names and types) but doesn't actually load or process your data until you explicitly tell it to. This allows Daft to optimize your entire workflow before executing anything.

To actually view your data, you have two options:

**Option 1: Preview with `.show()`** - View the first few rows:

```python
df_original.show(2)
```

``` {title="Output"}
╭──────────────────┬──────────────────┬──────────────────┬────────────┬──────────────────┬─────────────────┬───────────╮
│ Uniq Id          ┆ Product Name     ┆ Category         ┆      …     ┆ Variants         ┆ Product Url     ┆ Is Amazon │
│ ---              ┆ ---              ┆ ---              ┆            ┆ ---              ┆ ---             ┆ Seller    │
│ String           ┆ String           ┆ String           ┆ (9 hidden) ┆ String           ┆ String          ┆ ---       │
│                  ┆                  ┆                  ┆            ┆                  ┆                 ┆ String    │
╞══════════════════╪══════════════════╪══════════════════╪════════════╪══════════════════╪═════════════════╪═══════════╡
│ 4c69b61db1fc16e7 ┆ DB Longboards    ┆ Sports &         ┆ …          ┆ https://www.amaz ┆ https://www.ama ┆ Y         │
│ 013b43fc926e5…   ┆ CoreFlex Crossb… ┆ Outdoors |       ┆            ┆ on.com/DB-Lon…   ┆ zon.com/DB-Lon… ┆           │
│                  ┆                  ┆ Outdoor R…       ┆            ┆                  ┆                 ┆           │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ 66d49bbed043f5be ┆ Electronic Snap  ┆ Toys & Games |   ┆ …          ┆ None             ┆ https://www.ama ┆ Y         │
│ 260fa9f7fbff5…   ┆ Circuits Mini…   ┆ Learning & Edu…  ┆            ┆                  ┆ zon.com/Electr… ┆           │
╰──────────────────┴──────────────────┴──────────────────┴────────────┴──────────────────┴─────────────────┴───────────╯

(Showing first 2 rows)
```

This materializes and displays just the first 2 rows, which is perfect for quickly inspecting your data without loading the entire dataset.

**Option 2: Materialize with `.collect()`** - Load the entire dataset:

```python
# df_original.collect()
```

This would materialize the entire DataFrame (all 10,000+ rows in this case) into memory. Use `.collect()` when you need to work with the full dataset in memory.

### Working with a Smaller Dataset

For quick experimentation, let's create a smaller, simplified version of the dataframe with just the essential columns:

```python
# Select only the columns we need and limit to 5 rows for faster iteration
df = df_original.select("Product Name", "About Product", "Image").limit(5)
```

Now we have a manageable dataset of 5 products with just the product name, description, and image URLs. This simplified dataset lets us explore Daft's features without the overhead of unnecessary columns.

### Downloading Images

Let's extract and download product images. The `Image` column contains pipe-separated URLs. We'll extract the first URL and download it:

```python
# Extract the first image URL from the pipe-separated list
# The pattern captures everything before the first pipe or the entire string if no pipe
df = df.with_column(
    "first_image_url",
    daft.functions.regexp_extract(
        df["Image"],
        r"^([^|]+)",  # Extract everything before the first pipe
        1  # Get the first capture group
    )
)

# Download the image data
df = df.with_column(
    "image_data",
    daft.functions.download(df["first_image_url"], on_error="null")
)

# Decode images for visual display (in Jupyter notebooks, this shows actual images!)
df = df.with_column(
    "image",
    daft.functions.decode_image(df["image_data"], on_error="null")
)

# Check what we have - in Jupyter notebooks, the 'image' column shows actual images!
df.select("Product Name", "first_image_url", "image_data", "image").show(3)
```

``` {title="Output"}
╭────────────────────────────────┬────────────────────────────────┬────────────────────────────────┬──────────────╮
│ Product Name                   ┆ first_image_url                ┆ image_data                     ┆ image        │
│ ---                            ┆ ---                            ┆ ---                            ┆ ---          │
│ String                         ┆ String                         ┆ Binary                         ┆ Image[MIXED] │
╞════════════════════════════════╪════════════════════════════════╪════════════════════════════════╪══════════════╡
│ DB Longboards CoreFlex Crossb… ┆ https://images-na.ssl-images-… ┆ b"\xff\xd8\xff\xe0\x00\x10JFI… ┆ <Image>      │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Electronic Snap Circuits Mini… ┆ https://images-na.ssl-images-… ┆ b"\xff\xd8\xff\xe0\x00\x10JFI… ┆ <Image>      │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 3Doodler Create Flexy 3D Prin… ┆ https://images-na.ssl-images-… ┆ b"\xff\xd8\xff\xe0\x00\x10JFI… ┆ <Image>      │
╰────────────────────────────────┴────────────────────────────────┴────────────────────────────────┴──────────────╯

(Showing first 3 rows)
```

!!! note "Visual Display in Notebooks"

    In Jupyter notebooks, the `image` column will display actual thumbnail images instead of `<Image>` text.

This demonstrates Daft's multimodal capabilities:

- **Native regex support**: Use `regexp_extract()` to parse structured text with Rust-powered regex
- **URL handling**: Download content directly with`daft.functions.download()`
- **Image decoding**: Convert binary data to images with `decode_image()` for visual display

The decoded images are now ready for further processing.

### Batch AI Inference on Images

Let's use AI to analyze product materials at scale. Daft automatically parallelizes AI operations across your local machine's cores, making it efficient to process multiple images concurrently.

Let's suppose you want to create a new column that shows if each product is made of wood or not. This might be useful for, for example, a filtering feature on your website.

If you're running this in Google Colab or Jupyter, run the following cell to set your OpenAI API key. In Colab, first add your key to Secrets (🔑 icon in the left sidebar) with the name `OPENAI_API_KEY`. In Jupyter, you'll be prompted to enter your key and the input will be hidden.

```python
import os
try:
    from google.colab import userdata
    os.environ["OPENAI_API_KEY"] = userdata.get("OPENAI_API_KEY")
except ImportError:
    from getpass import getpass
    if "OPENAI_API_KEY" not in os.environ:
        os.environ["OPENAI_API_KEY"] = getpass("Enter your OpenAI API key: ")
```

```python
from pydantic import BaseModel, Field
from daft.functions import prompt

# Define a simple structured output model
class WoodAnalysis(BaseModel):
    is_wooden: bool = Field(description="Whether the product appears to be made of wood")

# Run AI inference on each image - Daft automatically batches and parallelizes this
df = df.with_column(
    "wood_analysis",
    prompt(
        ["Is this product made of wood? Look at the material.", df["image"]],
        return_format=WoodAnalysis,
        model="gpt-4o-mini",  # Using mini for cost-efficiency
        provider="openai",
        # api_key="your-key-here",  # Use OPENAI_API_KEY env var, or uncomment to set manually
    )
)

# Extract the boolean value from the structured output
# The result is a struct, so we extract the 'is_wooden' field
df = df.with_column(
    "is_wooden",
    df["wood_analysis"]["is_wooden"]
)

# Materialize the dataframe to compute all transformations
df = df.collect()

# View results
df.select("Product Name", "image", "is_wooden").show()
```

``` {title="Output"}
╭────────────────────────────────┬──────────────┬───────────╮
│ Product Name                   ┆ image        ┆ is_wooden │
│ ---                            ┆ ---          ┆ ---       │
│ String                         ┆ Image[MIXED] ┆ Bool      │
╞════════════════════════════════╪══════════════╪═══════════╡
│ DB Longboards CoreFlex Crossb… ┆ <Image>      ┆ true      │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ Electronic Snap Circuits Mini… ┆ <Image>      ┆ false     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ Guillow Airplane Design Studio ┆ <Image>      ┆ false     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ Woodstock- Collage 500 pc Puz… ┆ <Image>      ┆ false     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ 3Doodler Create Flexy 3D Prin… ┆ <Image>      ┆ false     │
╰────────────────────────────────┴──────────────┴───────────╯

(Showing first 5 of 5 rows)
```

The AI analyzes each product image to determine if it's made of wood. Notice that the longboard is identified as wooden (true), while the electronic circuits, design studio, puzzle, and 3D printing filament are identified as not wooden (false).

!!! note "Improving Accuracy"

    Looking at the actual product data, the longboard is made of bamboo and fiberglass, not wood. However, this is exactly what a human might categorize from the image alone! To improve accuracy, you could feed additional context to the AI like the product name, category, and description alongside the image. This example demonstrates how to get started with image-based analysis.

### Expanding the Analysis

Now, suppose you're satisfied with the results from your small subset and want to scale up. Instead of analyzing just 5 products, let's run the same analysis on 100 products to get more meaningful insights:

```python
from pydantic import BaseModel, Field
from daft.functions import prompt

# Define a simple structured output model (same as before)
class WoodAnalysis(BaseModel):
    is_wooden: bool = Field(description="Whether the product appears to be made of wood")

# Start fresh with the first 100 products
df_large = df_original.select("Product Name", "About Product", "Image").limit(100)

# Apply the same image processing pipeline
# 1. Extract first image URL
df_large = df_large.with_column(
    "first_image_url",
    daft.functions.regexp_extract(
        df_large["Image"],
        r"^([^|]+)",
        1
    )
)

# 2. Download images
df_large = df_large.with_column(
    "image_data",
    daft.functions.download(df_large["first_image_url"], on_error="null")
)

# 3. Decode images
df_large = df_large.with_column(
    "image",
    daft.functions.decode_image(df_large["image_data"], on_error="null")
)

# 4. Run AI analysis on all 100 products
df_large = df_large.with_column(
    "wood_analysis",
    prompt(
        ["Is this product made of wood? Look at the material.", df_large["image"]],
        return_format=WoodAnalysis,
        model="gpt-4o-mini",  # Using mini for cost-efficiency
        provider="openai",
        # api_key="your-key-here",  # Use OPENAI_API_KEY env var, or uncomment to set manually
    )
)

# 5. Extract the boolean value
df_large = df_large.with_column(
    "is_wooden",
    df_large["wood_analysis"]["is_wooden"]
)

# Materialize the dataframe to compute all transformations
df_large = df_large.collect()

# Count wooden products
wooden_count = df_large.where(df_large["is_wooden"] == True).count_rows()
total_count = df_large.count_rows()

print(f"Out of {total_count} products analyzed:")
print(f"  - {wooden_count} are made of wood")
print(f"  - {total_count - wooden_count} are not made of wood")
print(f"  - Percentage of wooden products: {(wooden_count / total_count * 100):.1f}%")
```

``` {title="Output"}
Out of 100 products analyzed:
  - 4 are made of wood
  - 96 are not made of wood
  - Percentage of wooden products: 4.0%
```

!!! note "Results May Vary"

    AI models are non-deterministic, so you may see slightly different numbers when running this analysis.

### Storing Your Results

After processing your data, you'll often want to save it for later use. Let's store our analyzed dataset as Parquet files:

```python
# Write the analyzed data to local Parquet files
df_large.write_parquet("product_analysis", write_mode="overwrite")
```

This writes your data to the `product_analysis/` directory. Daft automatically handles file naming using UUIDs to prevent conflicts. The `write_mode="overwrite"` parameter ensures that any existing data in the directory is replaced.

!!! note "Write anywhere"

    Just like reading, Daft can write data to many destinations including [S3](connectors/aws.md), [Iceberg](connectors/iceberg.md), [Delta Lake](connectors/delta_lake.md), and [more](connectors/index.md).

### Loading Your Stored Data

Let's verify the stored data by loading it back from those Parquet files:

```python
# Read the data back from Parquet files
df_loaded = daft.read_parquet("product_analysis/*.parquet")

# Verify the data loaded correctly
df_loaded.show(5)
```

``` {title="Output"}
╭────────────────────┬────────────────────┬───────────────────┬───────────────────┬────────────┬──────────────┬───────────────────┬───────────╮
│ Product Name       ┆ About Product      ┆ Image             ┆ first_image_url   ┆      …     ┆ image        ┆ wood_analysis     ┆ is_wooden │
│ ---                ┆ ---                ┆ ---               ┆ ---               ┆            ┆ ---          ┆ ---               ┆ ---       │
│ String             ┆ String             ┆ String            ┆ String            ┆ (1 hidden) ┆ Image[MIXED] ┆ Struct[is_wooden: ┆ Bool      │
│                    ┆                    ┆                   ┆                   ┆            ┆              ┆ Bool]             ┆           │
╞════════════════════╪════════════════════╪═══════════════════╪═══════════════════╪════════════╪══════════════╪═══════════════════╪═══════════╡
│ Flash Furniture    ┆ Collaborative      ┆ https://images-na ┆ https://images-na ┆ …          ┆ <Image>      ┆ {is_wooden:       ┆ false     │
│ 25''W x 45''L…     ┆ Trapezoid Acti…    ┆ .ssl-images-…     ┆ .ssl-images-…     ┆            ┆              ┆ false,            ┆           │
│                    ┆                    ┆                   ┆                   ┆            ┆              ┆ }                 ┆           │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ DB Longboards      ┆ Make sure this     ┆ https://images-na ┆ https://images-na ┆ …          ┆ <Image>      ┆ {is_wooden: true, ┆ true      │
│ CoreFlex Crossb…   ┆ fits by enteri…    ┆ .ssl-images-…     ┆ .ssl-images-…     ┆            ┆              ┆ }                 ┆           │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ DC Cover Girls:    ┆ Make sure this     ┆ https://images-na ┆ https://images-na ┆ …          ┆ <Image>      ┆ {is_wooden:       ┆ false     │
│ Black Canary …     ┆ fits by enteri…    ┆ .ssl-images-…     ┆ .ssl-images-…     ┆            ┆              ┆ false,            ┆           │
│                    ┆                    ┆                   ┆                   ┆            ┆              ┆ }                 ┆           │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ The Complete       ┆ Make sure this     ┆ https://images-na ┆ https://images-na ┆ …          ┆ <Image>      ┆ {is_wooden:       ┆ false     │
│ Common Core: Sta…  ┆ fits by enteri…    ┆ .ssl-images-…     ┆ .ssl-images-…     ┆            ┆              ┆ false,            ┆           │
│                    ┆                    ┆                   ┆                   ┆            ┆              ┆ }                 ┆           │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ Rubie's Child's    ┆ Make sure this     ┆ https://images-na ┆ https://images-na ┆ …          ┆ <Image>      ┆ {is_wooden:       ┆ false     │
│ Pokemon Delux…     ┆ fits by enteri…    ┆ .ssl-images-…     ┆ .ssl-images-…     ┆            ┆              ┆ false,            ┆           │
│                    ┆                    ┆                   ┆                   ┆            ┆              ┆ }                 ┆           │
╰────────────────────┴────────────────────┴───────────────────┴───────────────────┴────────────┴──────────────┴───────────────────┴───────────╯

(Showing first 5 rows)
```

### What's Next?

Now that you have a basic sense of Daft's functionality and features, here are some more resources to help you get the most out of Daft:

!!! tip "Scaling Further"

    This same pipeline can process thousands or millions of products by leveraging Daft's distributed computing capabilities. Check out our [distributed computing guide](distributed/index.md) to run this analysis at scale on Ray or Kubernetes clusters. Alternatively, [Daft Cloud](https://www.daft.ai/cloud) provides a fully managed serverless experience.

**Work with your favorite table and catalog formats:**

<div class="grid cards" markdown>

- [Apache Hudi](connectors/hudi.md)
- [Apache Iceberg](connectors/iceberg.md)
- [AWS Glue](connectors/glue.md)
- [AWS S3 Tables](connectors/s3tables.md)
- [Delta Lake](connectors/delta_lake.md)
- [Hugging Face Datasets](connectors/huggingface.md)
- [Unity Catalog](connectors/unity_catalog.md)
<!-- - [**LanceDB**](io/lancedb.md) -->

</div>

<!-- **Coming from?**

<div class="grid cards" markdown>

- [:simple-dask: **Dask Migration Guide**](migration/dask_migration.md)

</div> -->

**Explore our [Examples](examples/index.md) to see Daft in action:**

<div class="grid cards" markdown>

- [:material-image-edit: MNIST Digit Classification](examples/mnist.md)
- [:octicons-search-16: Running LLMs on the Red Pajamas Dataset](examples/llms-red-pajamas.md)
- [:material-image-search: Querying Images with UDFs](examples/querying-images.md)
- [:material-image-sync: Image Generation on GPUs](examples/image-generation.md)
- [:material-window-closed-variant: Window Functions in Daft](examples/window-functions.md)

</div>
