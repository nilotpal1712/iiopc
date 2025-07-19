# ========================================
# HSN Search Pipeline - Production Version
# ========================================

# 📦 Imports
import os
import re
import mysql.connector
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer, util
from sklearn.feature_extraction.text import CountVectorizer
from config import DB_CONFIG

# 🧠 Load SBERT model (all-mpnet-base-v2)
model = SentenceTransformer("all-mpnet-base-v2")

FAISS_INDEX = faiss.read_index("hsn_faiss.index")

with open("hsn_concat_descriptions.txt", "r", encoding="utf-8") as f:
    HSN_LINES = [line.strip() for line in f]

HSN_CODES = [line.split(" ||| ")[0] for line in HSN_LINES]
HSN_DESCS = [line.split(" ||| ")[1] for line in HSN_LINES]

# 📁 Load precomputed embeddings (assumed cached as .npy and .txt)
EMBEDDING_FILE = "hsn_embeddings.npy"
TEXT_FILE = "hsn_concat_descriptions.txt"

# 🔌 MySQL Connection
def connect_to_mysql():
    return mysql.connector.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        ssl_ca=DB_CONFIG["ssl_ca"]
    )

# 🔠 Normalize text for Boolean search
def normalize(text):
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    stopwords = set(["of", "and", "the", "with", "for", "in", "on", "to", "from", "by", "or", "not"])
    return " ".join([word for word in text.split() if word not in stopwords])

# 🧠 Boolean search
def boolean_search(query, descriptions):
    query_words = set(normalize(query).split())
    results = []
    for i, desc in enumerate(descriptions):
        desc_words = set(normalize(desc).split())
        match_score = len(query_words & desc_words) / max(1, len(query_words))
        if match_score > 0:
            results.append((i, match_score * 100))
    return sorted(results, key=lambda x: x[1], reverse=True)

# 🧠 Semantic search
def semantic_search(query_embedding, embeddings):
    cosine_scores = util.cos_sim(query_embedding, embeddings)[0]
    results = [(i, float(score)) for i, score in enumerate(cosine_scores)]
    return sorted(results, key=lambda x: x[1], reverse=True)

# 📥 Load embeddings and descriptions
def load_embeddings():
    if not os.path.exists(EMBEDDING_FILE) or not os.path.exists(TEXT_FILE):
        raise Exception("Embedding or description file not found.")
    descriptions = [line.strip() for line in open(TEXT_FILE, "r", encoding="utf-8")]
    vectors = np.load(EMBEDDING_FILE)
    return descriptions, vectors

# 🧱 Get full HSN hierarchy by national_code
def get_hsn_hierarchy(code8):
    conn = connect_to_mysql()
    cur = conn.cursor(dictionary=True)
    query = """
    SELECT 
        s.section_code, s.section_description,
        c.chapter_code, c.chapter_description,
        h.heading_code, h.heading_description,
        sh.subheading_code, sh.subheading_description,
        n.national_code, n.national_description
    FROM hsn_national n
    JOIN hsn_subheading sh ON n.subheading_code = sh.subheading_code
    JOIN hsn_heading h ON sh.heading_code = h.heading_code
    JOIN hsn_chapter c ON h.chapter_code = c.chapter_code
    JOIN hsn_section s ON c.section_code = s.section_code
    WHERE n.national_code = %s
    """
    cur.execute(query, (code8,))
    result = cur.fetchone()
    conn.close()
    return result

# 🎯 Main search function
def run_hsn_search(query):
    results = []
    descriptions, embeddings = load_embeddings()

    # Step 1: Boolean search on national_description
    bool_matches = boolean_search(query, descriptions)
    top_score = bool_matches[0][1] if bool_matches else 0

    if bool_matches and top_score >= 70:  # ✅ Only accept if confidence is high
        for i, score in bool_matches[:5]:
            code = descriptions[i].split(" ||| ")[0]
            hierarchy = get_hsn_hierarchy(code)
            results.append({
                "code": code,
                "description": descriptions[i].split(" ||| ")[1],
                "confidence": round(score, 2),
                "color": "GREEN" if score > 65 else "YELLOW" if score >= 35 else "RED",
                "source": "Boolean",
                **hierarchy
            })
        return {"results": results}


    # Step 2: SBERT search
    query_embedding = model.encode(normalize(query), convert_to_numpy=True).astype("float32")
    # 🔍 FAISS Search with Scaled Confidence
    D, I = FAISS_INDEX.search(np.array([query_embedding]), 5)

    SCALE = 50  # Tune this to shift confidence up/down
    for rank, idx in enumerate(I[0]):
        distance = D[0][rank]
        confidence = max(0.0, 100 - distance * SCALE)  # Convert L2 distance to proxy confidence

        code = HSN_CODES[idx]
        desc = HSN_DESCS[idx]
        hierarchy = get_hsn_hierarchy(code)

        results.append({
            "code": code,
            "description": desc,
            "confidence": round(confidence, 2),
            "color": "GREEN" if confidence > 65 else "YELLOW" if confidence >= 35 else "RED",
            "source": "SBERT_FAISS",
            **hierarchy
        })


    return {"results": results}

# 🧪 Optional test mode
if __name__ == "__main__":
    user_query = input("Enter your search query: ").strip()
    output = run_hsn_search(user_query)
    for res in output["results"]:
        print(f"\n🔹 CODE: {res['code']} ({res['confidence']}%) [{res['color']}]")
        print(f"Section {res['section_code']}: {res['section_description']}")
        print(f"Chapter {res['chapter_code']}: {res['chapter_description']}")
        print(f"Heading {res['heading_code']}: {res['heading_description']}")
        print(f"Subheading {res['subheading_code']}: {res['subheading_description']}")
        print(f"National: {res['national_code']}: {res['national_description']}")