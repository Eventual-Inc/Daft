from __future__ import annotations

import pymupdf
import torch
from langchain.text_splitter import RecursiveCharacterTextSplitter

import daft
from daft import col
from daft.io import IOConfig, S3Config

EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
NUM_GPU_NODES = 8
INPUT_PATH = "s3://daft-public-datasets/digitalcorpora_metadata"
OUTPUT_PATH = "s3://eventual-dev-benchmarking-results/ai-benchmark-results/document-embedding-results"
MAX_PDF_PAGES = 100
CHUNK_SIZE = 2048
CHUNK_OVERLAP = 200
EMBEDDING_BATCH_SIZE = 10


from daft import DataType as dt

daft.context.set_runner_ray()
extract_text_type = dt.list(dt.struct({"text": dt.string(), "page_number": dt.int64()}))
chunk_type = dt.list(dt.struct({"text": dt.string(), "chunk_id": dt.int64()}))


@daft.func(return_dtype=extract_text_type)
def extract_text_from_parsed_pdf(bytes):
    try:
        doc = pymupdf.Document(stream=bytes, filetype="pdf")
        if len(doc) > MAX_PDF_PAGES:
            print(f"Skipping PDF because it has {len(doc)} pages")
            return None
        return [{"text": page.get_text(), "page_number": page.number} for page in doc]

    except Exception as e:
        print(f"Error extracting text from PDF {e}")
        return None


@daft.func(return_dtype=chunk_type)
def chunk(text):
    splitter = RecursiveCharacterTextSplitter(chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP)
    chunk_iter = splitter.split_text(text)

    return [
        {
            "text": text,
            "chunk_id": chunk_index,
        }
        for chunk_index, text in enumerate(chunk_iter)
    ]


@daft.udf(
    return_dtype=daft.DataType.fixed_size_list(daft.DataType.float32(), EMBEDDING_DIM),
    concurrency=NUM_GPU_NODES,
    num_gpus=1.0,
    batch_size=EMBEDDING_BATCH_SIZE,
)
class Embedder:
    def __init__(self):
        from sentence_transformers import SentenceTransformer

        device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = SentenceTransformer(EMBED_MODEL_ID, device=device)
        self.model.compile()

    def __call__(self, text_col):
        if len(text_col) == 0:
            return []
        embeddings = self.model.encode(
            text_col.to_pylist(),
        )
        return embeddings


df = daft.read_parquet(INPUT_PATH)
df = df.where(daft.col("file_name").str.endswith(".pdf"))

df = df.with_column(
    "pdf_bytes",
    daft.functions.download(df["uploaded_pdf_path"], io_config=IOConfig(s3=S3Config.from_env())),
)
df = df.with_column("pages", extract_text_from_parsed_pdf(df["pdf_bytes"]).explode())
df = df.with_columns({"page_text": col("pages")["text"], "page_number": col("pages")["page_number"]})
df = df.where(daft.col("page_text").not_null())
df = df.with_column("chunks", chunk(df["page_text"]).explode())
df = df.with_columns({"chunk": col("chunks")["text"], "chunk_id": col("chunks")["chunk_id"]})
df = df.where(daft.col("chunk").not_null())
df = df.with_column("embedding", Embedder(df["chunk"]))
df = df.select("uploaded_pdf_path", "page_number", "chunk_id", "chunk", "embedding")
df.write_parquet(OUTPUT_PATH)
