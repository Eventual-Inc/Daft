from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from daft import DataType
from daft.io import DataSink
from daft.io.sink import WriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import ModuleType


class ChromaDataSink(DataSink[None]):
    """WriteSink for writing data to a Chroma collection."""

    def _import_chromadb(self) -> ModuleType:
        try:
            import chromadb

            return chromadb
        except ImportError:
            raise ImportError("chromadb is not installed. Please install chromadb using `pip install chromadb`")

    def __init__(
        self,
        persist_directory: str,
        collection_name: str,
        embedding_column: str | None = None,
        document_column: str = "document",
        id_column: str = "id",
        metadata_column: str | None = None,
        mode: Literal["create", "append"] = "append",
    ):
        chromadb = self._import_chromadb()
        client = chromadb.PersistentClient(path=persist_directory)
        self._persist_directory = persist_directory
        self._collection_name = collection_name
        self._embedding_column = embedding_column
        self._document_column = document_column
        self._id_column = id_column
        self._metadata_column = metadata_column
        self._mode = mode

        from chromadb.utils import embedding_functions

        sentence_transformer_ef = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name="all-MiniLM-L6-v2"
        )

        if mode == "create":
            try:
                client.delete_collection(name=collection_name)
            except Exception:
                pass
            client.create_collection(
                name=collection_name, configuration={"embedding_function": sentence_transformer_ef}
            )
        else:
            client.get_or_create_collection(name=collection_name)

        self._collection_name = collection_name

        self._schema = Schema._from_field_name_and_types([("rows_written", DataType.int64())])

    def name(self) -> str:
        return "Chroma DataSink"

    def schema(self) -> Schema:
        return self._schema

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[None]]:
        chromadb = self._import_chromadb()
        client = chromadb.PersistentClient(path=self._persist_directory)
        collection = client.get_collection(name=self._collection_name)
        for micropartition in micropartitions:
            batch_dict = micropartition.to_pydict()
            ids = batch_dict[self._id_column]
            documents = batch_dict[self._document_column]
            embeddings = batch_dict[self._embedding_column] if self._embedding_column else None
            metadatas = batch_dict[self._metadata_column] if self._metadata_column else None

            collection.add(
                ids=ids,
                documents=documents,
                embeddings=embeddings,
                metadatas=metadatas,
            )

            rows_written = len(ids)
            bytes_written = sum(len(str(doc).encode("utf-8")) for doc in documents)

            yield WriteResult(result=None, bytes_written=bytes_written, rows_written=rows_written)

    def finalize(self, write_results: list[WriteResult[None]]) -> MicroPartition:
        from daft.dependencies import pa

        total_rows = sum(wr.rows_written for wr in write_results)

        summary = {
            "rows_written": pa.array([total_rows], type=pa.int64()),
        }

        return MicroPartition.from_pydict(summary)
