#!/usr/bin/env python3
"""
Example demonstrating the OpenAI text embedder functionality in Daft.
"""

import os
from daft import DataFrame
from daft.ai.openai import OpenAIProvider

def main():
    # Set up OpenAI API key (you'll need to set this environment variable)
    if not os.getenv("OPENAI_API_KEY"):
        print("Please set OPENAI_API_KEY environment variable")
        return
    
    # Create sample text data
    texts = [
        "The food was delicious and the waiter was very friendly.",
        "I love this new restaurant in downtown.",
        "The service was slow but the food made up for it.",
        "This place has the best pizza I've ever tasted.",
        "The atmosphere is cozy and the staff is welcoming."
    ]
    
    # Create a DataFrame
    df = DataFrame.from_pydict({"text": texts})
    print("Original DataFrame:")
    print(df)
    print()
    
    # Create OpenAI provider
    provider = OpenAIProvider()
    
    # Get text embedder descriptor
    embedder_descriptor = provider.get_text_embedder(model="text-embedding-ada-002")
    print(f"Using model: {embedder_descriptor.get_model()}")
    print(f"Embedding dimensions: {embedder_descriptor.get_dimensions().size}")
    print()
    
    # Create embeddings
    embedder = embedder_descriptor.instantiate()
    embeddings = embedder.embed_text(texts)
    
    print(f"Generated {len(embeddings)} embeddings")
    print(f"Each embedding has {len(embeddings[0])} dimensions")
    print(f"First embedding (first 10 values): {embeddings[0][:10]}")
    print()
    
    # Add embeddings to DataFrame
    df_with_embeddings = df.with_column("embedding", embeddings)
    print("DataFrame with embeddings:")
    print(df_with_embeddings)
    print()
    
    # Show embedding dimensions
    print("Embedding column info:")
    print(df_with_embeddings.schema())

if __name__ == "__main__":
    main() 