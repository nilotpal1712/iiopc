from flask import Flask
from nic_api import nic_bp
from nco_api import nco_bp
from npcms_api import npcms_bp
from hsn_api import hsn_bp
from npcms_nic_api import npcms_nic_bp
from npcms_hsn_api import npcms_hsn_bp

app = Flask(__name__)

# Register all route blueprints
app.register_blueprint(nic_bp, url_prefix="/api/nic")
app.register_blueprint(nco_bp, url_prefix="/api/nco")
app.register_blueprint(npcms_bp, url_prefix="/api/npcms")
app.register_blueprint(hsn_bp, url_prefix="/api/hsn")
app.register_blueprint(npcms_nic_bp, url_prefix="/api/npcms-to-nic")
app.register_blueprint(npcms_hsn_bp, url_prefix="/api/npcms-hsn")

@app.route("/")
def home():
    return "🟢 IIOPC Flask Backend is Running Successfully!"

if __name__ == "__main__":
    app.run(debug=True)
