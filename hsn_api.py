# hsn_api.py

from flask import Flask, request, jsonify
from hsn_search_pipeline import run_hsn_search, get_hsn_hierarchy
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

@app.route("/api/hsn-search", methods=["GET"])
def hsn_search():
    query = request.args.get("query", "").strip()
    if not query:
        return jsonify({"error": "Query is required"}), 400

    output = run_hsn_search(query)
    return jsonify(output)

@app.route("/api/hsn-hierarchy", methods=["GET"])
def hsn_code_lookup():
    code = request.args.get("code")
    if not code:
        return jsonify({"error": "HSN/ITCHS code required"}), 400
    return jsonify(get_hsn_hierarchy(code))

@app.route("/api/hsn-dropdown/<level>", methods=["GET"])
def hsn_dropdown(level):
    parent = request.args.get("parent")
    cursor = conn.cursor(dictionary=True)

    if level == "section":
        cursor.execute("SELECT section_code AS code, section_description AS name FROM hsn_section")
    elif level == "chapter":
        cursor.execute("SELECT chapter_code AS code, chapter_description AS name FROM hsn_chapter WHERE section_code = %s", (parent,))
    elif level == "heading":
        cursor.execute("SELECT heading_code AS code, heading_description AS name FROM hsn_heading WHERE chapter_code = %s", (parent,))
    elif level == "subheading":
        cursor.execute("SELECT subheading_code AS code, subheading_description AS name FROM hsn_subheading WHERE heading_code = %s", (parent,))
    elif level == "national":
        cursor.execute("SELECT national_code AS code, national_description AS name FROM hsn_national WHERE subheading_code = %s", (parent,))
    else:
        return jsonify({"error": "Invalid level"}), 400

    return jsonify(cursor.fetchall())


@app.route("/api/hsn-lookup", methods=["GET"])
def hsn_lookup():
    code = request.args.get("code")
    if not code or len(code) not in [6, 8]:
        return jsonify({"error": "Code must be 6 or 8 digits"}), 400
    cursor = conn.cursor(dictionary=True)

    if len(code) == 8:
        cursor.execute("SELECT national_description FROM hsn_national WHERE national_code = %s", (code,))
        row = cursor.fetchone()
        return jsonify({"national_code": code, "national_description": row["national_description"] if row else "Not found"})

    cursor.execute("SELECT national_code, national_description FROM hsn_national WHERE subheading_code = %s", (code,))
    rows = cursor.fetchall()
    return jsonify(rows if rows else {"error": "No national codes found under this 6-digit code"})
