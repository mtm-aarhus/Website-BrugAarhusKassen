from flask import Flask, render_template, redirect, url_for
from sqlalchemy import create_engine
import os

app = Flask(__name__)

# --- Create and store DB engine globally ---
def get_engine():
    conn_str = os.getenv("BrugAarhusSQL")
    if not conn_str:
        raise RuntimeError("Environment variable BrugAarhusSQL is not set.")
    return create_engine(conn_str)

engine = get_engine()
app.config["ENGINE"] = engine  

# --- Register Blueprints ---
from udeservering.udeservering import udeservering_bp
app.register_blueprint(udeservering_bp, url_prefix="/udeservering")

@app.route("/")
def index():
    return redirect(url_for("udeservering.udeservering_applications_page"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5100, debug=True)
