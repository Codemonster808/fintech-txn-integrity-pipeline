"""
Vector store adapter: VECTOR_BACKEND=chroma (default, free, local) or
=pinecone (real, uses PINECONE_API_KEY). Same interface either way.
"""
import os
from typing import Protocol


class VectorStore(Protocol):
    def upsert(self, ids: list[str], embeddings: list[list[float]], metadatas: list[dict]) -> None: ...
    def query(self, embedding: list[float], top_k: int = 5) -> list[dict]: ...


class ChromaVectorStore:
    def __init__(self, collection_name: str = "default") -> None:
        import chromadb
        self._client = chromadb.PersistentClient(path=".chroma")
        self._collection = self._client.get_or_create_collection(collection_name)

    def upsert(self, ids, embeddings, metadatas) -> None:
        self._collection.upsert(ids=ids, embeddings=embeddings, metadatas=metadatas)

    def query(self, embedding, top_k: int = 5) -> list[dict]:
        result = self._collection.query(query_embeddings=[embedding], n_results=top_k)
        return [
            {"id": id_, "metadata": meta, "distance": dist}
            for id_, meta, dist in zip(result["ids"][0], result["metadatas"][0], result["distances"][0])
        ]


class PineconeVectorStore:
    def __init__(self, index_name: str = "portfolio") -> None:
        from pinecone import Pinecone
        api_key = os.environ.get("PINECONE_API_KEY")
        if not api_key:
            raise RuntimeError("PINECONE_API_KEY not set — required for VECTOR_BACKEND=pinecone")
        self._index = Pinecone(api_key=api_key).Index(index_name)

    def upsert(self, ids, embeddings, metadatas) -> None:
        self._index.upsert(vectors=list(zip(ids, embeddings, metadatas)))

    def query(self, embedding, top_k: int = 5) -> list[dict]:
        result = self._index.query(vector=embedding, top_k=top_k, include_metadata=True)
        return [{"id": m.id, "metadata": m.metadata, "distance": 1 - m.score} for m in result.matches]


def get_vector_store(collection_name: str = "default") -> VectorStore:
    backend = os.environ.get("VECTOR_BACKEND", "chroma")
    if backend == "chroma":
        return ChromaVectorStore(collection_name)
    if backend == "pinecone":
        return PineconeVectorStore(collection_name)
    raise ValueError(f"Unknown VECTOR_BACKEND: {backend!r} (expected 'chroma' or 'pinecone')")
