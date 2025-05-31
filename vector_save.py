import faiss
import pickle

# 1. Convert embeddings to float32 (FAISS requires float32)
embeddings = embeddings.astype('float32')

# 2. Create FAISS index - use IndexFlatIP for cosine similarity (with normalized vectors) or IndexFlatL2 for Euclidean
# Since MiniLM embeddings are cosine similarity friendly, we normalize vectors first:
faiss.normalize_L2(embeddings)

index = faiss.IndexFlatIP(embeddings.shape[1])  # Inner Product similarity

# 3. Add embeddings to the index
index.add(embeddings)
print(f"FAISS index has {index.ntotal} vectors")

# 4. Save index and metadata
faiss.write_index(index, "semantic_search.index")

with open("metadata.pkl", "wb") as f:
    pickle.dump(metadata, f)

print("Saved FAISS index and metadata.")

# === Usage example for searching ===
def search(query, top_k=5):
    # Embed query text using the same embed_texts function
    query_embedding = embed_texts([query])
    query_embedding = query_embedding.astype('float32')
    faiss.normalize_L2(query_embedding)

    D, I = index.search(query_embedding, top_k)
    # D: similarity scores, I: indexes of top results

    results = []
    for score, idx in zip(D[0], I[0]):
        item = metadata[idx].copy()
        item['score'] = float(score)
        results.append(item)
    return results

# Example search:
query = "example legal text to search"
results = search(query)
print(results)
