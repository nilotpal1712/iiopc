import re
import mysql.connector
from sentence_transformers import SentenceTransformer, util
from collections import defaultdict
from datetime import datetime
import numpy as np
import faiss
from config import DB_CONFIG

# =======================================
# 📦 Load Models and Resources
# =======================================
model = SentenceTransformer("all-mpnet-base-v2")

faiss_index = faiss.read_index("nco_faiss.index")

LOG_FILE = "nco_search_logs.jsonl"

# =======================================
# 🔌 MySQL Connection
# =======================================
def connect_mysql():
    return mysql.connector.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        ssl_ca=DB_CONFIG["ssl_ca"]
    )

conn = connect_mysql()
cursor = conn.cursor(dictionary=True)

# =======================================
# Preprocessing
# =======================================
def preprocess_query(query):
    return re.findall(r'\b\w+\b', query.lower())

# =======================================
# Expand Query using Synonyms (Simplified)
# =======================================
def expand_query(tokens):
    return " ".join([f"+{word}*" for word in tokens])

# =======================================
# Contradiction / Negation Detection
# =======================================
NEGATION_WORDS = ["not", "non", "except", "other than", "excluding"]
def is_contradictory(query, desc):
    for neg_word in NEGATION_WORDS:
        if neg_word in query.lower():
            after_neg = query.lower().split(neg_word, 1)[-1].strip().split()[0]
            if after_neg in desc.lower():
                return True
    emb_query = model.encode(query, convert_to_tensor=True)
    emb_desc = model.encode(desc, convert_to_tensor=True)
    sim = util.pytorch_cos_sim(emb_query, emb_desc)[0][0].item()
    return sim < 0.1

# =======================================
# Semantic Search
# =======================================
def semantic_search_faiss(query, codes, descs, emb_matrix):
    query_emb = model.encode(query).astype("float32").reshape(1, -1)
    D, I = faiss_index.search(query_emb, 10)

    results = []
    for i, dist in zip(I[0], D[0]):
        if is_contradictory(query, descs[i]):
            continue
        score = 1 - dist  # FAISS returns L2 distance, convert to similarity
        conf = round(score * 100, 2)
        results.append({
            "nco_2015": codes[i],
            "nco_description": descs[i],
            "nco_2004": "",
            "method": "SBERT_FAISS",
            "confidence": conf
        })

    if not results:
        return []
    top_conf = results[0]['confidence']
    return [r for r in results if abs(r['confidence'] - top_conf) <= 0.5][:5]

# =======================================
# Main Search
# =======================================
def search(query):
    tokens = preprocess_query(query)
    boolean_query = expand_query(tokens)

    cursor.execute("""
        SELECT nco_2015, nco_description, nco_2004,
        MATCH(nco_description) AGAINST (%s IN BOOLEAN MODE) AS score
        FROM nco_code
        WHERE MATCH(nco_description) AGAINST (%s IN BOOLEAN MODE)
        ORDER BY score DESC LIMIT 20
    """, (boolean_query, boolean_query))

    rows = cursor.fetchall()
    if rows:
        for row in rows:
            row["confidence"] = min(100, round(row["score"] * 10, 2))
            row["method"] = "Boolean"
        top_conf = rows[0]['confidence']
        return [r for r in rows if abs(r['confidence'] - top_conf) <= 0.5][:5]

    # Fallback to semantic search if no Boolean match
    desc_lines = [line.strip() for line in open("nco_2015_descriptions.txt", encoding="utf-8")]
    codes = [line.split(" ||| ")[0] for line in desc_lines]
    descs = [line.split(" ||| ")[1] for line in desc_lines]
    embeddings = np.load("nco_2015_embeddings.npy")
    return semantic_search_faiss(query, codes, descs, embeddings)

# =======================================
# Display Result
# =======================================
def color_code(conf):
    if conf > 65:
        return "\033[92m"  # Green
    elif conf >= 35:
        return "\033[93m"  # Yellow
    return "\033[91m"      # Red

# =======================================
# Interactive Run
# =======================================
def run_search(query):
    print(f"\n🔎 Searching for: {query}")
    results = search(query)

    if not results:
        print("❌ No result found.")
        return

    print("✅ Results:")
    for r in results:
        color = color_code(r['confidence'])
        print(f"{color}{r['nco_2015']} | {r['nco_description']} | {r['nco_2004']} | {r['method']} | {r['confidence']}%\033[0m")
    
if __name__ == "__main__":
    q = input("Enter NCO search query: ")
    run_search(q)