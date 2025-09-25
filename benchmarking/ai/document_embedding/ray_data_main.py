from __future__ import annotations

import pymupdf
import ray
import ray.data
import torch
from langchain.text_splitter import RecursiveCharacterTextSplitter
from sentence_transformers import SentenceTransformer

EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
NUM_GPU_NODES = 8
INPUT_PATH = "s3://daft-public-datasets/digitalcorpora_metadata"
OUTPUT_PATH = (
    "s3://eventual-dev-benchmarking-results/ai-benchmark-results/document-embedding-results"
)
MAX_PDF_PAGES = 100
CHUNK_SIZE = 2048
CHUNK_OVERLAP = 200
EMBEDDING_BATCH_SIZE = 10

ray.init()


def extract_text_from_pdf(row):
    try:
        doc = pymupdf.Document(stream=row["bytes"], filetype="pdf")
        if len(doc) > MAX_PDF_PAGES:
            print(f"Skipping PDF {row["path"]} because it has {len(doc)} pages")
            return
        for page in doc:
            row["page_text"] = page.get_text()
            row["page_number"] = page.number
            yield row
    except Exception as e:
        print(f"Error extracting text from PDF {row["path"]}: {e}")
        return


def chunker(row):
    splitter = RecursiveCharacterTextSplitter(chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP)
    chunk_iter = splitter.split_text(row["page_text"])
    for chunk_index, text in enumerate(chunk_iter):
        row["chunk"] = text
        row["chunk_id"] = chunk_index
        yield row


class Embedder:
    def __init__(self):
        device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = SentenceTransformer(EMBED_MODEL_ID, device=device)
        self.model.compile()

    def __call__(self, batch):
        embedding = self.model.encode(
            batch["chunk"],
        )
        batch["embedding"] = embedding
        return batch


file_paths = ray.data.read_parquet(INPUT_PATH).filter(lambda row: row["file_name"].endswith(".pdf")).take_all()
file_paths = [row["uploaded_pdf_path"] for row in file_paths]

ds = ray.data.read_binary_files(file_paths, include_paths=True)
ds = ds.flat_map(extract_text_from_pdf)
ds = ds.flat_map(chunker)
ds = ds.map_batches(Embedder, concurrency=NUM_GPU_NODES, num_gpus=1.0, batch_size=EMBEDDING_BATCH_SIZE)
ds = ds.select_columns(["path", "page_number", "chunk_id", "chunk", "embedding"])
ds.write_parquet(OUTPUT_PATH)
