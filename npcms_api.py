# npcms_api.py

from flask import Flask, request, jsonify
from npcms_search_pipeline import run_npcms_search
from flask_cors import CORS
import mysql.connector
from config import DB_CONFIG

app = Flask(__name__)
CORS(app)

conn = mysql.connector.connect(
    host=DB_CONFIG["host"],
    port=DB_CONFIG["port"],
    user=DB_CONFIG["user"],
    password=DB_CONFIG["password"],
    database=DB_CONFIG["database"],
    ssl_ca=DB_CONFIG["ssl_ca"]
)

@app.route("/api/npcms-dropdown/<level>", methods=["GET"])
def npcms_dropdown(level):
    parent = request.args.get("parent")
    cursor = conn.cursor(dictionary=True)

    if level == "section":
        cursor.execute("SELECT section_code AS code, section_description AS name FROM npcms_section")
    elif level == "division":
        cursor.execute("SELECT division_code AS code, division_description AS name FROM npcms_division WHERE section_code = %s", (parent,))
    elif level == "group":
        cursor.execute("SELECT group_code AS code, group_description AS name FROM npcms_group WHERE division_code = %s", (parent,))
    elif level == "class":
        cursor.execute("SELECT class_code AS code, class_description AS name FROM npcms_class WHERE group_code = %s", (parent,))
    elif level == "subclass":
        cursor.execute("SELECT subclass_code AS code, subclass_description AS name FROM npcms_subclass WHERE class_code = %s", (parent,))
    elif level == "product":
        cursor.execute("SELECT product_code AS code, product_description AS name FROM npcms_product WHERE subclass_code = %s", (parent,))
    else:
        return jsonify({"error": "Invalid level"}), 400

    return jsonify(cursor.fetchall())

@app.route("/api/npcms-search", methods=["GET"])
def npcms_search():
    query = request.args.get("query", "").strip()
    category = request.args.get("category", "").strip()
    if not query or category not in {"chemical", "other"}:
        return jsonify({"error": "Both query and valid category (chemical/other) are required"}), 400

    results = run_npcms_search(query, "1" if category == "chemical" else "2")
    formatted = []
    for r in results.get("results", []):
        conf = r.get("confidence", 0)
        color = "GREEN" if conf > 65 else "YELLOW" if conf >= 35 else "RED"
        formatted.append({
            "code": r.get("product_code"),
            "description": r.get("product_description"),
            "unit": r.get("unit", ""),
            "confidence": conf,
            "color": color,
            "source": r.get("source")
        })
    return jsonify({"results": formatted})


@app.route("/api/npcms-lookup", methods=["GET"])
def npcms_lookup():
    code = request.args.get("code")
    if not code or len(code) != 7:
        return jsonify({"error": "Invalid product code"}), 400
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT s.section_code, s.section_description, d.division_code, d.division_description,
               g.group_code, g.group_description, c.class_code, c.class_description,
               sb.subclass_code, sb.subclass_description, p.product_code, p.product_description
        FROM npcms_product p
        JOIN npcms_subclass sb ON p.subclass_code = sb.subclass_code
        JOIN npcms_class c ON sb.class_code = c.class_code
        JOIN npcms_group g ON c.group_code = g.group_code
        JOIN npcms_division d ON g.division_code = d.division_code
        JOIN npcms_section s ON d.section_code = s.section_code
        WHERE p.product_code = %s
    """, (code,))
    row = cursor.fetchone()
    return jsonify(row or {"error": "Code not found"})
