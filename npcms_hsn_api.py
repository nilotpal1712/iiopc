@app.route("/api/npcms-to-hsn", methods=["GET"])
def npcms_to_hsn():
    code = request.args.get("code")
    if not code or len(code) != 7:
        return jsonify({"error": "Invalid NPCMS product code"}), 400

    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT h.national_code, n.national_description, h.confidence
        FROM npcms_hsn h
        JOIN hsn_national n ON h.national_code = n.national_code
        WHERE h.product_code = %s
        ORDER BY h.confidence DESC
    """, (code,))
    rows = cursor.fetchall()

    return jsonify({"product_code": code, "matches": rows})


@app.route("/api/hsn-to-npcms", methods=["GET"])
def hsn_to_npcms():
    code = request.args.get("code")
    if not code or len(code) != 8:
        return jsonify({"error": "Invalid HSN/ITCHS national code"}), 400

    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT h.product_code, p.product_description, h.confidence
        FROM npcms_hsn h
        JOIN npcms_product p ON h.product_code = p.product_code
        WHERE h.national_code = %s
        ORDER BY h.confidence DESC
    """, (code,))
    rows = cursor.fetchall()

    return jsonify({"national_code": code, "matches": rows})
