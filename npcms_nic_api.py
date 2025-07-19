@app.route("/api/npcms-to-nic", methods=["GET"])
def npcms_to_nic():
    code = request.args.get("code")
    if not code or len(code) != 7:
        return jsonify({"error": "Invalid NPCMS product code"}), 400

    cursor = conn.cursor(dictionary=True)

    # Get subclass from product_code
    cursor.execute("SELECT subclass_code FROM npcms_product WHERE product_code = %s", (code,))
    prod = cursor.fetchone()
    if not prod:
        return jsonify({"error": "Product not found"}), 404

    cursor.execute("""
        SELECT s.section_code, s.section_name, d.division_code, d.division_name,
               g.group_code, g.group_name, c.class_code, c.class_name,
               sc.subclass_code, sc.subclass_description
        FROM nic_npcms_asi map
        JOIN nic_subclass sc ON map.nic_class_code = sc.class_code
        JOIN nic_class c ON sc.class_code = c.class_code
        JOIN nic_group g ON c.group_code = g.group_code
        JOIN nic_division d ON g.division_code = d.division_code
        JOIN nic_section s ON d.section_code = s.section_code
        WHERE map.npcms_subclass_code = %s
    """, (prod["subclass_code"],))
    nic_rows = cursor.fetchall()

    return jsonify({"product_code": code, "subclass_code": prod["subclass_code"], "nic_mappings": nic_rows})
