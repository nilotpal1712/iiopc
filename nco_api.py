# nco_api.py

from flask import Flask, request, jsonify
from nco_search_pipeline import search
from flask_cors import CORS
import mysql.connector
from config import DB_CONFIG

app = Flask(__name__)
CORS(app)

# MySQL connection
conn = mysql.connector.connect(
    host=DB_CONFIG["host"],
    port=DB_CONFIG["port"],
    user=DB_CONFIG["user"],
    password=DB_CONFIG["password"],
    database=DB_CONFIG["database"],
    ssl_ca=DB_CONFIG["ssl_ca"]
)

@app.route("/api/nco-dropdown/<level>", methods=["GET"])
def nco_dropdown(level):
    parent = request.args.get("parent")
    cursor = conn.cursor(dictionary=True)

    if level == "division":
        cursor.execute("SELECT DISTINCT division_code AS code, division_name AS name FROM nco_division")
    elif level == "subdivision":
        cursor.execute("SELECT DISTINCT subdivision_code AS code, subdivision_name AS name FROM nco_subdivision WHERE division_code = %s", (parent,))
    elif level == "group":
        cursor.execute("SELECT DISTINCT group_code AS code, group_name AS name FROM nco_group WHERE subdivision_code = %s", (parent,))
    elif level == "family":
        cursor.execute("SELECT DISTINCT family_code AS code, family_name AS name FROM nco_family WHERE group_code = %s", (parent,))
    elif level == "nco":
        cursor.execute("SELECT nco_2015 AS code, nco_description AS name FROM nco_code WHERE family_code = %s", (parent,))
    else:
        return jsonify({"error": "Invalid level"}), 400

    return jsonify(cursor.fetchall())

@app.route("/api/nco-search", methods=["GET"])
def nco_search():
    query = request.args.get("query", "").strip()
    if not query:
        return jsonify({"error": "Query is required"}), 400

    results = search(query)
    formatted = []
    for r in results:
        conf = r.get("confidence", 0)
        color = "GREEN" if conf > 65 else "YELLOW" if conf >= 35 else "RED"
        formatted.append({
            "nco_2015": r["nco_2015"],
            "nco_description": r["nco_description"],
            "nco_2004": r.get("nco_2004", ""),
            "confidence": conf,
            "method": r["method"],
            "color": color
        })
    return jsonify({"results": formatted})


@app.route("/api/nco-lookup", methods=["GET"])
def nco_lookup():
    code = request.args.get("code")
    if not code or len(code) != 4:
        return jsonify({"error": "Invalid family code"}), 400
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT family_name FROM nco_family WHERE family_code = %s", (code,))
    fam = cursor.fetchone()
    cursor.execute("SELECT nco_2015, nco_description FROM nco_code WHERE family_code = %s", (code,))
    rows = cursor.fetchall()
    return jsonify({
        "family_code": code,
        "family_description": fam["family_name"] if fam else "Not found",
        "nco_2015_list": rows if rows else "No NCO 2015 codes found under this family."
    })
