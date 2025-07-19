# ========================================
# 📦 Imports
# ========================================
import re
import torch
import mysql.connector
from datetime import datetime
from sentence_transformers import SentenceTransformer, util
import numpy as np
from config import DB_CONFIG

# ========================================
# 🧠 Load Models and Resources
# ========================================
model = SentenceTransformer('all-mpnet-base-v2')

# ========================================
# 🔌 MySQL Connection
# ========================================
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

# ========================================
# 🚨 Negation Words
# ========================================
NEGATION_WORDS = ["not", "non", "except", "other than", "excluding"]

# ========================================
# Load Synonym and Section Mapping
# ========================================
def load_nic_synonyms():
    cursor.execute("SELECT word, synonym FROM nic_synonym")
    rows = cursor.fetchall()
    syn_map = {}
    for row in rows:
        key = row["word"].strip().lower()
        val = row["synonym"].strip().lower()
        syn_map.setdefault(key, set()).add(val)
    return syn_map

def load_keyword_to_section():
    cursor.execute("SELECT keyword, section_code FROM nic_kts")
    return {row["keyword"].strip().lower(): row["section_code"] for row in cursor.fetchall()}

synonym_dict = load_nic_synonyms()
keyword_to_section = load_keyword_to_section()

# ========================================
# Preprocessing
# ========================================
def preprocess_query(query):
    return re.findall(r'\b\w+\b', query.lower())

# ========================================
# Expand Query with Synonyms
# ========================================
def expand_query(tokens):
    expanded = set(tokens)
    for token in tokens:
        expanded.update(synonym_dict.get(token, []))
    return " ".join([f"+{word}*" for word in expanded])

# ========================================
# TEMPORARY: For NPCMS → NIC Mapping
# ========================================

def semantic_search_by_class(query, allowed_class_code):
    """Restrict semantic search to a specific NIC class code."""
    desc_lines = [line.strip() for line in open("nic_subclass_descriptions.txt", encoding="utf-8")]
    codes = [line.split(" ||| ")[0] for line in desc_lines]
    descs = [line.split(" ||| ")[1] for line in desc_lines]
    desc_embs = np.load("nic_subclass_embeddings.npy")
    query_emb = model.encode(query, convert_to_tensor=True)

    results = []
    for i, score in enumerate(util.pytorch_cos_sim(query_emb, desc_embs)[0]):
        if str(codes[i])[:4] != str(allowed_class_code):
            continue
        results.append({
            "code": codes[i],
            "description": descs[i],
            "confidence": score.item()
        })

    results.sort(key=lambda x: x["confidence"], reverse=True)
    return results[:1]  # Only best match

# ========================================
# Boolean Search
# ========================================
def boolean_search(query):
    sql = """
        SELECT subclass_code, subclass_description,
               MATCH(subclass_description) AGAINST (%s IN BOOLEAN MODE) AS score
        FROM nic_subclass
        WHERE MATCH(subclass_description) AGAINST (%s IN BOOLEAN MODE)
        ORDER BY score DESC
        LIMIT 20
    """
    cursor.execute(sql, (query, query))
    results = cursor.fetchall()

    formatted = []
    if results:
        max_s = max(r["score"] for r in results) or 1.0
        for r in results:
            conf = (r["score"] / max_s)
            formatted.append({
                "code": r["subclass_code"],
                "description": r["subclass_description"],
                "confidence": conf
            })

        formatted.sort(key=lambda x: x["confidence"], reverse=True)

    return formatted[:3]

# ========================================
# Semantic Search
# ========================================
def semantic_search(query, section_code=None):
    desc_lines = [line.strip() for line in open("nic_subclass_descriptions.txt", encoding="utf-8")]
    codes = [line.split(" ||| ")[0] for line in desc_lines]
    descs = [line.split(" ||| ")[1] for line in desc_lines]
    desc_embs = np.load("nic_subclass_embeddings.npy")
    query_emb = model.encode(query, convert_to_tensor=True)
    
    scores = util.pytorch_cos_sim(query_emb, desc_embs)[0]

    results = []
    for i, score in enumerate(scores):
        if section_code and str(codes[i])[0] != section_code:
            continue
        results.append({
            "code": codes[i],
            "description": descs[i],
            "confidence": score.item()
        })

    results.sort(key=lambda x: x["confidence"], reverse=True)
    return results[:3]

# ========================================
# Display Results
# ========================================
def display_results(results, method):
    for res in results:
        conf_pct = res["confidence"] * 100
        color = "GREEN" if conf_pct >= 65 else ("YELLOW" if conf_pct >= 35 else "RED")
        print(f"{res['code']} | {res['description']} | {method} | {conf_pct:.2f}% | {color}")

# ========================================
# Final Query Runner
# ========================================
def run_search(query):
    print(f"\n🔎 Query: {query}")
    
    # Step 1: Tokenize query
    tokens = preprocess_query(query)
    
    # Step 2: Build Boolean query with synonyms
    boolean_query = expand_query(tokens)
    
    # Step 3: Expand query text for SBERT
    expanded_tokens = set(tokens)
    for token in tokens:
        expanded_tokens.update(synonym_dict.get(token, []))
    expanded_query = " ".join(expanded_tokens)
    
    # Step 4: Run Boolean search
    boolean_results = boolean_search(boolean_query)
    if boolean_results:
        display_results(boolean_results, "BOOLEAN")
        return

    # Step 5: Try section-aware SBERT search
    section_hint = None
    for token in tokens:
        if token in keyword_to_section:
            section_hint = keyword_to_section[token]
            break

    print(f"🧭 Section hint: {section_hint if section_hint else 'None'}")

    semantic_results = semantic_search(expanded_query, section_hint)
    if not semantic_results and section_hint:
        print("🔁 No strong match in section. Retrying without section filter...")
        semantic_results = semantic_search(expanded_query)

    if semantic_results:
        display_results(semantic_results, "SBERT")
        return

    print("❌ No match found.")

if __name__ == "__main__":
    q = input("Enter NIC Query: ")
    run_search(q)
