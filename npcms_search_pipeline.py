# ========================================
# NPCMS Search Pipeline
# ========================================

# 📦 Imports
import re
import os
import mysql.connector
from datetime import datetime
from sentence_transformers import SentenceTransformer, util
import faiss
import numpy as np
from config import DB_CONFIG

# 🧠 Load Model and Resources
model = SentenceTransformer("all-mpnet-base-v2")

# 📥 Load FAISS + Descriptions
FAISS_INDEX = faiss.read_index("npcms_product_faiss.index")
with open("npcms_product_descriptions.txt", "r", encoding="utf-8") as f:
    PRODUCT_LINES = [line.strip() for line in f if " ||| " in line]

PRODUCT_CODES = [line.split(" ||| ")[0] for line in PRODUCT_LINES]
PRODUCT_DESCS = [line.split(" ||| ")[1] for line in PRODUCT_LINES]

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

conn = connect_to_mysql()
cursor = conn.cursor(dictionary=True)

# 🔄 Load Synonyms and Exceptions from MySQL Tables

# CPM Synonyms
cpm_synonym = {}
cursor.execute("SELECT product_code, synonym FROM npcms_cpm")
rows = cursor.fetchall()
for row in rows:
    syn = row["synonym"].lower()
    code = row["product_code"]
    cpm_synonym.setdefault(syn, []).append(code)

# Subclass-level exceptions
npcms_except = {}
cursor.execute("SELECT subclass_code, exclude_keyword FROM npcms_except")
rows = cursor.fetchall()
for row in rows:
    subclass = row["subclass_code"]
    kw = row["exclude_keyword"]
    npcms_except.setdefault(subclass, []).append(kw)

# Product-level exceptions
npcms_except_p = {}
cursor.execute("SELECT product_code, exclude_keyword FROM npcms_except_p")
rows = cursor.fetchall()
for row in rows:
    code_p = row["product_code"]
    kw = row["exclude_keyword"].lower()
    npcms_except_p.setdefault(code_p, set()).add(kw)

# 🚨 Negation Words
NEGATION_WORDS = ["not", "non", "except", "other than", "excluding"]

# Subclasses to avoid initially (parts/accessories etc.)
subclass_parts_accessories = {
    29600, 36960, 37196, 38160, 38360, 38530, 38922, 38995, 42341, 42342,
    43151, 43152, 43153, 43154, 43155, 43156, 43251, 43252, 43253, 43254,
    43331, 43332, 43430, 43570, 43941, 43942, 43943, 43944, 44115, 44139,
    44199, 44251, 44252, 44253, 44255, 44256, 44320, 44461, 44462, 44522,
    44523, 44640, 44760, 44831, 44832, 44833, 44921, 44922, 44923, 44929,
    45170, 45180, 45290, 46131, 46132, 46220, 46430, 46541, 46542, 46960,
    47171, 47172, 47173, 47401, 47402, 47403, 48281, 48282, 48283, 48284,
    48285, 48351, 48352, 48353, 48354, 49129, 49231, 49232, 49540, 49640,
    49941, 49942
}

# 🔧 Helper Functions
def build_mysql_boolean_query(terms):
    boolean_terms = []
    for term in terms:
        clean = re.sub(r"[^\w\s]", "", term.lower()).strip()
        if clean and len(clean) > 1:
            boolean_terms.append(f"+{clean}*")
    return " ".join(boolean_terms)

def expand_keywords_basic(user_query):
    tokens = re.findall(r"\b\w+\b", user_query.lower())
    terms = sorted(set(tokens))
    return build_mysql_boolean_query(terms), terms

def adjust_score(desc, raw_score, code, query=None):
    desc_low = desc.lower()
    score = raw_score
    for neg in ["except", "excluding", "other than", "not including"]:
        if neg in desc_low and query:
            after = desc_low.split(neg, 1)[-1]
            terms = re.findall(r"\b\w+\b", after)
            if any(t in re.findall(r"\b\w+\b", query.lower()) for t in terms):
                return score * 0.25
    if any(n in desc_low for n in [" not ", " non "]) and not desc_low.startswith("other ") and not str(code).endswith("9"):
        score *= 0.5
    return score

def should_exclude_product(code, description, query=None):
    def simple_stem(token):
        if token in {"its", "this", "was", "is"}:
            return token
        return token[:-1] if token.endswith("s") and len(token) > 3 else token

    code = str(code)
    desc = re.sub(r"[^\w\s]", " ", description.lower())
    desc_tokens = set(re.findall(r"\b\w+\b", desc))

    if query:
        query_tokens = set(re.findall(r"\b\w+\b", query.lower()))
        query_tokens |= set(simple_stem(w) for w in query_tokens)
    else:
        query_tokens = set()

    for kw in npcms_except_p.get(code, []):
        kw_tokens = set(re.findall(r"\b\w+\b", kw.lower()))
        kw_tokens |= set(simple_stem(w) for w in kw_tokens)

        if kw_tokens & desc_tokens and kw_tokens & query_tokens:
            print(f"⛔ Excluding {code} — matched keyword: {kw}")
            return True
    return False

def write_log(entry):
    with open("npcms_search_log.jsonl", "a", encoding="utf-8") as f:
        import json
        json.dump(entry, f, ensure_ascii=False)
        f.write("\n")

def semantic_search_faiss(query, k=5):
    query_vec = model.encode(query, convert_to_numpy=True).astype("float32").reshape(1, -1)
    D, I = FAISS_INDEX.search(query_vec, k)

    SCALE = 50
    results = []

    for idx, dist in zip(I[0], D[0]):
        code = PRODUCT_CODES[idx]
        desc = PRODUCT_DESCS[idx]
        conf = max(0.0, 100 - dist * SCALE)
        results.append({
            "product_code": code,
            "product_description": desc,
            "confidence": round(conf, 2),
            "source": "SBERT_FAISS"
        })

    return results

# ========================================
# PHASE-I: Category Selection
# ========================================
def run_npcms_search(query=None, category=None):
    if query is None:
        query = input("🔎 Enter your query: ").strip()
    if category is None:
        print("1️⃣ Chemical / Pharmaceutical / Medicinal")
        print("2️⃣ General Item (Manufactured goods)")
        category = input("➤ Enter 1 or 2: ").strip()
    if category == "1":
        return search_cpm_item(query)
    elif category == "2":
        return search_general_item(query)
    else:
        print("⚠️ Invalid category.")
        return None
# ========================================
# PHASE-II: Search CPM Items
# ========================================
def search_cpm_item(query, top_k=5):
    log = {"query": query, "category": "chemical", "results": []}
    tokens = re.findall(r"\b\w+\b", query.lower())

    # ✅ Step 1: Direct Synonym Match
    matching_codes = cpm_synonym.get(query.lower(), [])
    if matching_codes:
        code_placeholders = ','.join(['%s'] * len(matching_codes))
        sql = f"""
            SELECT product_code, product_description, unit
            FROM npcms_product
            WHERE is_cpm = 1 AND product_code IN ({code_placeholders})
        """
        cursor.execute(sql, matching_codes)
        results = cursor.fetchall()
        for r in results:
            if not should_exclude_product(r['product_code'], r['product_description'], query):
                print(f"{r['product_code']} | {r['product_description']} | {r['unit']}")
                print(f"🤖 Direct Synonym Match [GREEN]")
                log["results"].append({**r, "confidence": 100.0, "source": "synonym_direct"})
        write_log(log)
        return log

    # ✅ Step 2: Boolean Match
    boolean_query, _ = expand_keywords_basic(query)
    cursor.execute(
        """
        SELECT product_code, product_description, unit, 
               MATCH(product_description) AGAINST (%s IN BOOLEAN MODE) AS score
        FROM npcms_product 
        WHERE is_cpm = 1 
          AND MATCH(product_description) AGAINST (%s IN BOOLEAN MODE)
        ORDER BY score DESC LIMIT %s
        """, (boolean_query, boolean_query, top_k)
    )
    results = cursor.fetchall()

    if results:
        max_s = max(r['score'] for r in results) or 1.0
        scored_results = []
        for r in results:
            raw_conf = (r['score'] / max_s) * 100
            conf = adjust_score(r['product_description'], raw_conf, r['product_code'], query)
            if not should_exclude_product(r['product_code'], r['product_description'], query):
                scored_results.append({**r, "confidence": conf, "source": "boolean"})

        scored_results.sort(key=lambda x: x["confidence"], reverse=True)

        for r in scored_results:
            label = "GREEN" if r["confidence"] > 65 else "YELLOW" if r["confidence"] >= 35 else "RED"
            print(f"{r['product_code']} | {r['product_description']} | {r['unit']}")
            print(f"✅ Boolean Match Confidence: {r['confidence']:.2f}% [{label}]")
            log["results"].append(r)

        if log["results"]:
            write_log(log)
            return log

    # ✅ Step 3: SBERT FAISS Match (is_cpm = 1 only)
    print("🔍 No strong Boolean match. Trying semantic search (FAISS)...")
    emb_query = model.encode(query, convert_to_numpy=True).astype("float32").reshape(1, -1)
    D, I = FAISS_INDEX.search(emb_query, 25)  # Get more candidates for strict filtering

    SCALE = 50
    found_valid = False
    for idx, dist in zip(I[0], D[0]):
        code = PRODUCT_CODES[idx]
        desc = PRODUCT_DESCS[idx]

        # ✅ Check: this product must be is_cpm = 1
        cursor.execute("SELECT is_cpm FROM npcms_product WHERE product_code = %s", (code,))
        row = cursor.fetchone()
        if not row or row['is_cpm'] != 1:
            continue

        # ✅ Check: all query terms must appear in description
        if not all(t in desc.lower() for t in tokens):
            continue

        conf = max(0.0, 100 - dist * SCALE)
        print(f"{code} | {desc}")
        print(f"🤖 Semantic Match Confidence: {conf:.2f}%")
        log["results"].append({
            "product_code": code,
            "product_description": desc,
            "confidence": round(conf, 2),
            "source": "SBERT_FAISS"
        })
        found_valid = True

    if found_valid:
        write_log(log)
        return log

    # ✅ Step 4: Fallback response if no match passed all filters
    print("🧪 The exact product that you are looking for is not available in NPCMS.")
    fallback_items = [
        ("3423199", "Chemical elements not elsewhere classified.", "Tonne"),
        ("3527099", "Other pharmaceutical products not elsewhere classified", "Kg")
    ]
    for code, desc, unit in fallback_items:
        print(f"{code} | {desc} | {unit}")
        log["results"].append({
            "product_code": code,
            "product_description": desc,
            "unit": unit,
            "confidence": 0.0,
            "source": "cpm_fallback"
        })

    write_log(log)
    return log
# ========================================
# PHASE-III: Search General Items
# ========================================
def search_general_item(query, top_k=5):
    log = {"query": query, "category": "general", "results": []}
    boolean_query, terms = expand_keywords_basic(query)

    # Step 1: Boolean search
    cursor.execute(
        """
        SELECT product_code, product_description, unit, 
               MATCH(product_description) AGAINST (%s IN BOOLEAN MODE) AS score
        FROM npcms_product 
        WHERE is_cpm = 0 
          AND MATCH(product_description) AGAINST (%s IN BOOLEAN MODE)
        ORDER BY score DESC LIMIT %s
        """, (boolean_query, boolean_query, top_k)
    )
    results = cursor.fetchall()

    if results:
        max_s = max(r['score'] for r in results) or 1.0
        scored_results = []
        for r in results:
            raw_conf = (r['score'] / max_s) * 100
            conf = adjust_score(r['product_description'], raw_conf, r['product_code'], query)
            if not should_exclude_product(r['product_code'], r['product_description'], query):
                scored_results.append({**r, "confidence": conf, "source": "boolean"})

        scored_results.sort(key=lambda x: x["confidence"], reverse=True)

        for r in scored_results:
            label = "GREEN" if r["confidence"] > 65 else "YELLOW" if r["confidence"] >= 35 else "RED"
            print(f"{r['product_code']} | {r['product_description']} | {r['unit']}")
            print(f"✅ Boolean Match Confidence: {r['confidence']:.2f}% [{label}]")
            log["results"].append(r)

        if log["results"]:
            write_log(log)
            return log

    # Step 2: Relaxed fallback using LIKE
    cursor.execute("""
        SELECT product_code, product_description, unit
        FROM npcms_product
        WHERE is_cpm = 0 AND LOWER(product_description) LIKE %s
    """, (f"%{query.lower()}%",))
    relaxed_results = cursor.fetchall()
    if relaxed_results:
        print("🔁 Found match via relaxed LIKE search:")
        for r in relaxed_results:
            print(f"{r['product_code']} | {r['product_description']} | {r['unit']}")
            if not should_exclude_product(r['product_code'], r['product_description'], query):
                log['results'].append({**r, "confidence": 90.0, "source": "like_fallback"})
        if log["results"]:
            write_log(log)
            return log

    # Step 3: Fallback to subclass description
    print("🔍 No strong product match. Checking subclass descriptions...")
    excluded = set(npcms_except.keys())
    cursor.execute(
        """
        SELECT subclass_code, subclass_description, 
               MATCH(subclass_description) AGAINST (%s IN BOOLEAN MODE) AS score
        FROM npcms_subclass 
        WHERE MATCH(subclass_description) AGAINST (%s IN BOOLEAN MODE)
        ORDER BY score DESC LIMIT 10
        """, (boolean_query, boolean_query)
    )
    subclasses = cursor.fetchall()
    valid = None

    for s in subclasses:
        c = int(s['subclass_code'])
        if c not in subclass_parts_accessories and str(c) not in excluded:
            valid = s
            break
    if not valid:
        for s in subclasses:
            c = int(s['subclass_code'])
            if c in subclass_parts_accessories and str(c) not in excluded:
                valid = s
                break

    if valid:
        sc = valid['subclass_code']
        print(f"✅ Subclass identified: {sc} - {valid['subclass_description']}")
        cursor.execute(
            "SELECT product_code, product_description, unit FROM npcms_product WHERE subclass_code = %s",
            (sc,)
        )
        prods = cursor.fetchall()
        for p in prods:
            conf = adjust_score(p['product_description'], 100.0, p['product_code'], query)
            label = "GREEN" if conf > 65 else "YELLOW" if conf >= 35 else "RED"
            print(f"{p['product_code']} | {p['product_description']} | {p['unit']}")
            print(f"✅ Subclass Match Confidence: {conf:.2f}% [{label}]")
            if not should_exclude_product(p['product_code'], p['product_description'], query):
                log['results'].append({**p, "confidence": conf, "source": "subclass"})
        if log["results"]:
            write_log(log)
            return log

    # Step 4: SBERT-FAISS fallback
    print("🔍 No strong match. Trying semantic fallback via FAISS...")
    emb_query = model.encode(query, convert_to_numpy=True).astype("float32").reshape(1, -1)
    D, I = FAISS_INDEX.search(emb_query, 25)  # Get more candidates to allow filtering

    SCALE = 50
    for idx, dist in zip(I[0], D[0]):
        code = PRODUCT_CODES[idx]
        desc = PRODUCT_DESCS[idx]

        # ✅ Ensure this product is NOT a CPM item
        cursor.execute("SELECT is_cpm FROM npcms_product WHERE product_code = %s", (code,))
        row = cursor.fetchone()
        if not row or row['is_cpm'] != 0:
            continue

        conf = max(0.0, 100 - dist * SCALE)
        print(f"{code} | {desc}")
        print(f"🤖 Semantic Match Confidence: {conf:.2f}%")
        log["results"].append({
            "product_code": code,
            "product_description": desc,
            "confidence": round(conf, 2),
            "source": "SBERT_FAISS"
        })

    if log["results"]:
        write_log(log)
    else:
        print("❌ No matching product found. Please try rephrasing or manual search.")
        write_log(log)

    return log

if __name__ == "__main__":
    query = input("Enter your NPCMS query: ").strip()
    print("1️⃣ Chemical / Pharmaceutical / Medicinal")
      print("2️⃣ General Item (Manufactured goods)")
      category = input("➤ Enter 1 or 2: ").strip()
    results = run_npcms_search(query, category)
    if results and results.get("results"):
        for r in results["results"]:
            print(f"{r['product_code']} | {r['product_description']} | {r.get('unit', '')} | {r.get('confidence', '?')}% | {r.get('source', '')}")
    else:
        print("❌ No results found.")
