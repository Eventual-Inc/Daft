from __future__ import annotations

import daft
import pymupdf
import torch
from daft import col
from langchain.text_splitter import RecursiveCharacterTextSplitter

EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
NUM_GPU_NODES = 8
INPUT_PATH = "s3://eventual-data-test-bucket/digitalcorpora/metadata/original_file_name_to_s3_path_10k"
OUTPUT_PATH = "s3://desmond-test/colin-test/document-embedding-results"
MAX_PDF_PAGES = 100
CHUNK_SIZE = 2048
CHUNK_OVERLAP = 200
EMBEDDING_BATCH_SIZE = 10

daft.context.set_runner_ray()


def extract_text_from_parsed_pdf(pdf_bytes):
    try:
        doc = pymupdf.Document(stream=pdf_bytes, filetype="pdf")
        if len(doc) > MAX_PDF_PAGES:
            print(f"Skipping PDF because it has {len(doc)} pages")
            return None
        page_texts = [
            {"text": page.get_text(), "page_number": page.number} for page in doc
        ]
        return page_texts
    except Exception as e:
        print(f"Error extracting text from PDF {e}")
        return None


def chunk(text):
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP
    )
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
            convert_to_tensor=True,
            torch_dtype=torch.bfloat16,
        )
        return embeddings.cpu().numpy()


df = daft.read_parquet(INPUT_PATH)
df = df.where(daft.col("file_name").str.endswith(".pdf"))
df = df.with_column("pdf_bytes", df["uploaded_pdf_path"].url.download())
df = df.with_column(
    "pages",
    df["pdf_bytes"].apply(
        extract_text_from_parsed_pdf,
        return_dtype=list[{"text": str, "page_number": int}],
    ),
)
df = df.explode("pages")
df = df.with_columns(
    {"page_text": col("pages")["text"], "page_number": col("pages")["page_number"]}
)
df = df.where(daft.col("page_text").not_null())
df = df.with_column(
    "chunks",
    df["page_text"].apply(chunk, return_dtype=list[{"text": str, "chunk_id": int}]),
)
df = df.explode("chunks")
df = df.with_columns(
    {"chunk": col("chunks")["text"], "chunk_id": col("chunks")["chunk_id"]}
)
df = df.where(daft.col("chunk").not_null())
df = df.with_column("embedding", Embedder(df["chunk"]))
df = df.select("uploaded_pdf_path", "page_number", "chunk_id", "chunk", "embedding")
df.write_parquet(OUTPUT_PATH)
