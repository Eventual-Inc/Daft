from __future__ import annotations

import pymupdf
import torch
import ray
from langchain.text_splitter import RecursiveCharacterTextSplitter
import time
import ray

import daft
from daft import col

EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
NUM_GPU_NODES = 8
INPUT_PATH = "s3://daft-public-datasets/digitalcorpora_metadata"
OUTPUT_PATH = "s3://eventual-dev-benchmarking-results/ai-benchmark-results/document-embedding-results"
MAX_PDF_PAGES = 100
CHUNK_SIZE = 2048
CHUNK_OVERLAP = 200
EMBEDDING_BATCH_SIZE = 10

daft.context.set_runner_ray()

# Wait for Ray cluster to be ready
@ray.remote
def warmup():
    pass
ray.get([warmup.remote() for _ in range(64)])


def extract_text_from_parsed_pdf(pdf_bytes):
    try:
        doc = pymupdf.Document(stream=pdf_bytes, filetype="pdf")
        if len(doc) > MAX_PDF_PAGES:
            print(f"Skipping PDF because it has {len(doc)} pages")
            return None
        page_texts = [{"text": page.get_text(), "page_number": page.number} for page in doc]
        return page_texts
    except Exception as e:
        print(f"Error extracting text from PDF {e}")
        return None


def chunk(text):
    splitter = RecursiveCharacterTextSplitter(chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP)
    chunk_iter = splitter.split_text(text)
    chunks = []
    for chunk_index, text in enumerate(chunk_iter):
        chunks.append(
            {
                "text": text,
                "chunk_id": chunk_index,
            }
        )
    return chunks


@daft.cls(max_concurrency=NUM_GPU_NODES, gpus=1)
class Embedder:
    def __init__(self):
        from sentence_transformers import SentenceTransformer

        device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = SentenceTransformer(EMBED_MODEL_ID, device=device)
        self.model.compile()

    @daft.method.batch(
        return_dtype=daft.DataType.fixed_size_list(daft.DataType.float32(), EMBEDDING_DIM),
        batch_size=EMBEDDING_BATCH_SIZE,
    )
    def __call__(self, text_col):
        if len(text_col) == 0:
            return []
        embeddings = self.model.encode(
            text_col.to_pylist(),
        )
        return embeddings

daft.set_planning_config(default_io_config=daft.io.IOConfig(s3=daft.io.S3Config.from_env()))

start_time = time.time()
df = daft.read_parquet(INPUT_PATH)
df = df.where(daft.col("file_name").str.endswith(".pdf"))
df = df.with_column("pdf_bytes", df["uploaded_pdf_path"].url.download())
df = df.with_column(
    "pages",
    df["pdf_bytes"].apply(
        extract_text_from_parsed_pdf,
        return_dtype=daft.DataType.list(daft.DataType.struct({"text": daft.DataType.string(), "page_number": daft.DataType.int64()})),
    ),
)
df = df.explode("pages")
df = df.with_columns({"page_text": col("pages")["text"], "page_number": col("pages")["page_number"]})
df = df.where(daft.col("page_text").not_null())
df = df.with_column(
    "chunks",
    df["page_text"].apply(chunk, return_dtype=daft.DataType.list(daft.DataType.struct({"text": daft.DataType.string(), "chunk_id": daft.DataType.int64()})),
    ),
)
df = df.explode("chunks")
df = df.with_columns({"chunk": col("chunks")["text"], "chunk_id": col("chunks")["chunk_id"]})
df = df.where(daft.col("chunk").not_null())
df = df.with_column("embedding", Embedder()(df["chunk"]))
df = df.select("uploaded_pdf_path", "page_number", "chunk_id", "chunk", "embedding")
df.write_parquet(OUTPUT_PATH)

end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")
