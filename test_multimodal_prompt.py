from __future__ import annotations

import os

import daft
from daft.functions import prompt

# Get the actual user home directory
home = os.path.expanduser("~")

df = daft.from_pydict(
    {
        "prompt": ["What's in this image and file?"],
        "my_image": [f"{home}/Downloads/daft_test_files/sample_image.jpg"],
        "my_file": [f"{home}/Downloads/daft_test_files/sample.pdf"],
    }
)

# Read the image file directly as bytes
df = df.with_column("my_image", daft.functions.file(daft.col("my_image")))

# Read the PDF file as bytes
df = df.with_column("my_file", daft.functions.file(daft.col("my_file")))

# Prompt Usage - pass the messages as a list
df = df.with_column(
    "result",
    prompt(
        messages=[daft.col("prompt"), daft.col("my_image"), daft.col("my_file")],
        system_message="You are a helpful assistant.",
        model="gpt-5-2025-08-07",
        provider="openai",
        reasoning={"effort": "high"},  # Adjust reasoning level
        tools=[{"type": "web_search"}],  # Leverage internal OpenAI Tools
    ),
)

# Show the results
df.show()
