import pandas as pd
from transformers import AutoTokenizer, AutoModel
import torch
import numpy as np
import faiss
import pickle
import os
import glob
import re
from tqdm import tqdm

# Load model and tokenizer
model_name = "sentence-transformers/paraphrase-MiniLM-L6-v2"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModel.from_pretrained(model_name)


def get_csv_files(directory="."):
    """
    Get all CSV files that start with a number and end with .csv
    """
    pattern = os.path.join(directory, "*.csv")
    all_csv_files = glob.glob(pattern)

    # Filter files that start with a number
    valid_files = []
    for file_path in all_csv_files:
        filename = os.path.basename(file_path)
        if re.match(r'^\d+.*\.csv$', filename):
            valid_files.append(file_path)

    return valid_files


def load_all_csvs(csv_files):
    """
    Load and combine all CSV files into a single dataframe
    """
    all_dataframes = []

    for file_path in tqdm(csv_files, desc="Loading CSV files"):
        try:
            df = pd.read_csv(file_path)
            # Add source file information
            df['source_file'] = os.path.basename(file_path)
            all_dataframes.append(df)
            print(f"Loaded {len(df)} records from {os.path.basename(file_path)}")
        except Exception as e:
            print(f"Error loading {file_path}: {e}")

    if not all_dataframes:
        raise ValueError("No valid CSV files found or loaded")

    # Combine all dataframes
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"Total combined records: {len(combined_df)}")

    return combined_df


def embed_texts(text_list):
    embeddings = []
    batch_size = 16  # small batch size, adjust if needed

    for start_idx in tqdm(range(0, len(text_list), batch_size), desc="Generating embeddings"):
        batch_texts = text_list[start_idx:start_idx + batch_size]
        encoded_input = tokenizer(batch_texts, padding=True, truncation=True, return_tensors='pt')

        with torch.no_grad():
            model_output = model(**encoded_input)

        # Mean pooling
        token_embeddings = model_output.last_hidden_state  # (batch_size, seq_len, hidden_size)
        attention_mask = encoded_input['attention_mask'].unsqueeze(-1)  # (batch_size, seq_len, 1)
        masked_embeddings = token_embeddings * attention_mask
        summed = masked_embeddings.sum(dim=1)
        counts = attention_mask.sum(dim=1)
        mean_pooled = summed / counts

        embeddings.extend(mean_pooled.cpu().numpy())

    return np.array(embeddings)


def main():
    # Get all valid CSV files
    csv_files = get_csv_files()

    if not csv_files:
        print("No CSV files found that start with a number!")
        return

    print(f"Found {len(csv_files)} valid CSV files:")
    for file in csv_files:
        print(f"  - {os.path.basename(file)}")

    # Load and combine all CSV files
    df = load_all_csvs(csv_files)

    # Check if required columns exist
    if 'text' not in df.columns:
        print("Error: 'text' column not found in CSV files")
        return

    # Extract texts
    texts = df["text"].tolist()
    print(f"Processing {len(texts)} text entries...")

    # Get embeddings for all texts
    embeddings = embed_texts(texts)
    print(f"Generated {len(embeddings)} embeddings with shape {embeddings.shape}")

    # Prepare metadata - include all columns except 'text'
    metadata_columns = [col for col in df.columns if col != 'text']
    metadata = df[metadata_columns].to_dict(orient='records')

    # Convert embeddings to float32 and normalize for cosine similarity
    embeddings = embeddings.astype('float32')
    faiss.normalize_L2(embeddings)

    # Create FAISS index
    index = faiss.IndexFlatIP(embeddings.shape[1])  # Inner Product similarity
    index.add(embeddings)
    print(f"FAISS index created with {index.ntotal} vectors")

    # Save index and metadata
    faiss.write_index(index, "unified_semantic_search.index")

    with open("unified_metadata.pkl", "wb") as f:
        pickle.dump(metadata, f)

    print("Saved unified FAISS index and metadata.")
    print("Files created:")
    print("  - unified_semantic_search.index")
    print("  - unified_metadata.pkl")


def search(query, top_k=5, index_path="unified_semantic_search.index", metadata_path="unified_metadata.pkl"):
    """
    Search function for the unified vector database
    """
    # Load index and metadata
    index = faiss.read_index(index_path)
    with open(metadata_path, "rb") as f:
        metadata = pickle.load(f)

    # Embed query text
    query_embedding = embed_texts([query])
    query_embedding = query_embedding.astype('float32')
    faiss.normalize_L2(query_embedding)

    # Search
    D, I = index.search(query_embedding, top_k)

    # Format results
    results = []
    for score, idx in zip(D[0], I[0]):
        item = metadata[idx].copy()
        item['score'] = float(score)
        results.append(item)

    return results


if __name__ == "__main__":
    main()

    # Example usage after building the database:
    # query = "example legal text to search"
    # results = search(query)
    # print("Search results:")
    # for i, result in enumerate(results, 1):
    #     print(f"{i}. Score: {result['score']:.4f}")
    #     print(f"   Source: {result.get('source_file', 'Unknown')}")
    #     if 'paragraph_id' in result:
    #         print(f"   Paragraph ID: {result['paragraph_id']}")
    #     if 'law_act_id' in result:
    #         print(f"   Law Act ID: {result['law_act_id']}")
    #     print()