# nic_api.py

from flask import Flask, request, jsonify
from nic_search_pipeline import run_search
from flask_cors import CORS
import mysql.connector
from config import DB_CONFIG

app = Flask(__name__)
CORS(app)

# Connect to DB
conn = mysql.connector.connect(
    host=DB_CONFIG["host"],
    port=DB_CONFIG["port"],
    user=DB_CONFIG["user"],
    password=DB_CONFIG["password"],
    database=DB_CONFIG["database"],
    ssl_ca=DB_CONFIG["ssl_ca"]
)

@app.route("/api/nic-search", methods=["GET"])
def api_nic_search():
    query = request.args.get("query", "").strip()
    if not query:
        return jsonify({"error": "Query is required"}), 400

    print(f"📥 Received NIC search query: {query}")
    log = {"results": []}

    def capture_results(query):
        from nic_search_pipeline import preprocess_query, expand_query, boolean_search, semantic_search, synonym_dict, keyword_to_section
        tokens = preprocess_query(query)
        boolean_query = expand_query(tokens)

        boolean_results = boolean_search(boolean_query)
        if boolean_results:
            for r in boolean_results:
                conf_pct = r["confidence"] * 100
                color = "GREEN" if conf_pct >= 65 else ("YELLOW" if conf_pct >= 35 else "RED")
                log["results"].append({
                    "code": r["code"],
                    "description": r["description"],
                    "confidence": round(conf_pct, 2),
                    "color": color,
                    "source": "BOOLEAN"
                })
            return log

        section_hint = next((keyword_to_section[t] for t in tokens if t in keyword_to_section), None)
        expanded_query = " ".join(set(tokens).union(*[synonym_dict.get(t, []) for t in tokens]))
        semantic_results = semantic_search(expanded_query, section_hint)

        if not semantic_results and section_hint:
            semantic_results = semantic_search(expanded_query)

        for r in semantic_results:
            conf_pct = r["confidence"] * 100
            color = "GREEN" if conf_pct >= 65 else ("YELLOW" if conf_pct >= 35 else "RED")
            log["results"].append({
                "code": r["code"],
                "description": r["description"],
                "confidence": round(conf_pct, 2),
                "color": color,
                "source": "SBERT"
            })

        return log

    results = capture_results(query)
    return jsonify(results)


@app.route("/api/nic-dropdown/<level>", methods=["GET"])
def get_dropdown(level):
    parent = request.args.get("parent")
    cursor = conn.cursor(dictionary=True)

    if level == "section":
        cursor.execute("SELECT section_code AS code, section_name AS name FROM nic_section")
    elif level == "division":
        cursor.execute("SELECT division_code AS code, division_name AS name FROM nic_division WHERE section_code = %s", (parent,))
    elif level == "group":
        cursor.execute("SELECT group_code AS code, group_name AS name FROM nic_group WHERE division_code = %s", (parent,))
    elif level == "class":
        cursor.execute("SELECT class_code AS code, class_name AS name FROM nic_class WHERE group_code = %s", (parent,))
    elif level == "subclass":
        cursor.execute("SELECT subclass_code AS code, subclass_description AS name FROM nic_subclass WHERE class_code = %s", (parent,))
    else:
        return jsonify({"error": "Invalid level"}), 400

    results = cursor.fetchall()
    return jsonify(results)


@app.route("/api/nic-description", methods=["GET"])
def get_subclass_description():
    code = request.args.get("code")
    if not code:
        return jsonify({"error": "Code is required"}), 400

    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT subclass_description FROM nic_subclass WHERE subclass_code = %s", (code,))
    row = cursor.fetchone()

    if row:
        return jsonify({"code": code, "description": row["subclass_description"]})
    return jsonify({"error": "Subclass not found"}), 404


@app.route("/api/nic-lookup", methods=["GET"])
def nic_lookup():
    code = request.args.get("code")
    if not code or len(code) != 5:
        return jsonify({"error": "Invalid subclass code"}), 400
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT s.section_code, s.section_name, d.division_code, d.division_name,
               g.group_code, g.group_name, c.class_code, c.class_name,
               sc.subclass_code, sc.subclass_description
        FROM nic_subclass sc
        JOIN nic_class c ON sc.class_code = c.class_code
        JOIN nic_group g ON c.group_code = g.group_code
        JOIN nic_division d ON g.division_code = d.division_code
        JOIN nic_section s ON d.section_code = s.section_code
        WHERE sc.subclass_code = %s
    """, (code,))
    row = cursor.fetchone()
    return jsonify(row or {"error": "Code not found"})
