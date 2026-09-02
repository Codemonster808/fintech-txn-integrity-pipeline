"""
Vector store adapter: VECTOR_BACKEND=chroma (default, free, local) or
=pinecone (real, uses PINECONE_API_KEY). Same interface either way.
"""

import os
from typing import Any, Protocol, cast


class VectorStore(Protocol):
    def upsert(
        self, ids: list[str], embeddings: list[list[float]], metadatas: list[dict]
    ) -> None: ...
    def query(
        self, embedding: list[float], top_k: int = 5, where: dict | None = None
    ) -> list[dict]: ...


class ChromaVectorStore:
    def __init__(self, collection_name: str = "default") -> None:
        import chromadb

        self._client = chromadb.PersistentClient(path=".chroma")
        self._collection = self._client.get_or_create_collection(collection_name)

    def upsert(self, ids: list[str], embeddings: list[list[float]], metadatas: list[dict]) -> None:
        # chromadb's stubs declare these parameters as invariant unions of
        # Sequence types; plain list[list[float]] / list[dict] are accepted
        # at runtime, so cast at the boundary rather than contort the
        # VectorStore interface every backend has to implement.
        self._collection.upsert(
            ids=ids, embeddings=cast(Any, embeddings), metadatas=cast(Any, metadatas)
        )

    def query(
        self, embedding: list[float], top_k: int = 5, where: dict | None = None
    ) -> list[dict]:
        # `where` scopes the search to a metadata subset (e.g. one document's
        # own chunks) BEFORE ranking — filtering top-k results after an
        # unscoped global search is not equivalent when many similar
        # documents are indexed together: a document's own best-matching
        # chunk can rank outside the global top-k, silently starving that
        # document's retrieval. Found by testing against 15 near-identical
        # synthetic reports, where exactly this happened.
        result = self._collection.query(
            query_embeddings=cast(Any, [embedding]), n_results=top_k, where=where
        )
        # chromadb types every QueryResult field except "ids" as Optional, and
        # returns empty outer lists when the collection is empty or the `where`
        # filter matched nothing. Indexing [0] unconditionally would raise
        # TypeError/IndexError there, so treat "no results" as an empty answer.
        ids = result.get("ids") or []
        metadatas = result.get("metadatas") or []
        distances = result.get("distances") or []
        if not ids or not metadatas or not distances:
            return []
        return [
            {"id": id_, "metadata": meta, "distance": dist}
            for id_, meta, dist in zip(ids[0], metadatas[0], distances[0], strict=False)
        ]


class PineconeVectorStore:
    def __init__(self, index_name: str = "portfolio") -> None:
        from pinecone import Pinecone

        api_key = os.environ.get("PINECONE_API_KEY")
        if not api_key:
            raise RuntimeError("PINECONE_API_KEY not set — required for VECTOR_BACKEND=pinecone")
        self._index = Pinecone(api_key=api_key).Index(index_name)

    def upsert(self, ids: list[str], embeddings: list[list[float]], metadatas: list[dict]) -> None:
        self._index.upsert(vectors=list(zip(ids, embeddings, metadatas, strict=False)))

    def query(
        self, embedding: list[float], top_k: int = 5, where: dict | None = None
    ) -> list[dict]:
        result = self._index.query(
            vector=embedding, top_k=top_k, include_metadata=True, filter=where
        )
        return [
            {"id": m.id, "metadata": m.metadata, "distance": 1 - m.score} for m in result.matches
        ]


def get_vector_store(collection_name: str = "default") -> VectorStore:
    backend = os.environ.get("VECTOR_BACKEND", "chroma")
    if backend == "chroma":
        return ChromaVectorStore(collection_name)
    if backend == "pinecone":
        return PineconeVectorStore(collection_name)
    raise ValueError(f"Unknown VECTOR_BACKEND: {backend!r} (expected 'chroma' or 'pinecone')")
