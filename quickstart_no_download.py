from __future__ import annotations

import daft

# Load the e-commerce dataset from Hugging Face
print("Loading dataset from Hugging Face...")
df = daft.read_huggingface("UniqueData/asos-e-commerce-dataset")

# Inspect the schema
print("\nDataFrame schema:")
print(df)

# Preview first few rows
print("\nPreview of data:")
df.show(2)

# Create a smaller dataset for faster iteration
print("\nLimiting to 5 rows for faster processing...")
df = df.limit(5)

# Extract the first image URL from the string using regex
print("\nExtracting image URLs...")
df = df.with_column(
    "first_image_url",
    daft.functions.regexp_extract(
        df["images"],
        r"'([^']+)'",  # Extract content between single quotes
        1,  # Get the first capture group
    ),
)

# Download the image data
print("\nDownloading images...")
# df = df.with_column("image_data", daft.functions.download(df["first_image_url"], on_error="null"))

# Collect the dataframe to materialize everything
print("\nCollecting dataframe...")
df = df.collect()

print("\nDone! DataFrame collected with downloaded image data.")
