from __future__ import annotations

import pymupdf
import ray
from langchain.text_splitter import RecursiveCharacterTextSplitter
import time

import daft
from daft.functions.ai import embed_text

EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
INPUT_PATH = "s3://daft-public-datasets/digitalcorpora_metadata"
OUTPUT_PATH = "s3://eventual-dev-benchmarking-results/ai-benchmark-results/document-embedding-results"
MAX_PDF_PAGES = 100
CHUNK_SIZE = 2048
CHUNK_OVERLAP = 200

daft.context.set_runner_ray()


# Wait for Ray cluster to be ready
@ray.remote
def warmup():
    pass


ray.get([warmup.remote() for _ in range(64)])


@daft.func(
    return_dtype=daft.DataType.struct(
        {"text": daft.DataType.string(), "page_number": daft.DataType.int64()}
    ),
    unnest=True,
)
def extract_text_from_parsed_pdf(pdf_bytes: bytes):
    try:
        doc = pymupdf.Document(stream=pdf_bytes, filetype="pdf")
        if len(doc) > MAX_PDF_PAGES:
            print(f"Skipping PDF because it has {len(doc)} pages")
            return
        for page in doc:
            yield {
                "text": page.get_text(),
                "page_number": page.number,
            }
    except Exception as e:
        print(f"Error extracting text from PDF {e}")
        return


@daft.func(
    return_dtype=daft.DataType.struct(
        {"chunk": daft.DataType.string(), "chunk_id": daft.DataType.int64()}
    ),
    unnest=True,
)
def chunk(text: str):
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP
    )
    chunk_iter = splitter.split_text(text)
    for chunk_index, text in enumerate(chunk_iter):
        yield {
            "chunk": text,
            "chunk_id": chunk_index,
        }


start_time = time.time()
df = daft.read_parquet(INPUT_PATH)
df = df.where(df["file_name"].str.endswith(".pdf"))
df = df.with_column("pdf_bytes", df["uploaded_pdf_path"].url.download())
df = df.select(
    "*",
    extract_text_from_parsed_pdf(df["pdf_bytes"]),
)
df = df.where(df["text"].not_null())
df = df.select(
    "*",
    chunk(df["text"]),
)
df = df.where(df["chunk"].not_null())
df = df.with_column(
    "embedding",
    embed_text(df["chunk"], provider="sentence_transformers", model=EMBED_MODEL_ID),
)
df = df.select("uploaded_pdf_path", "page_number", "chunk_id", "chunk", "embedding")
df.write_parquet(OUTPUT_PATH)

end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")
