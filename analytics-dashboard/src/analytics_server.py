"""
analytics_server.py
-------------------
Flask backend that:
  1. Serves the dashboard HTML
  2. Exposes /api/customers  — returns classified CSV as JSON
  3. Exposes /api/chat       — proxies to Ollama llama3 with customer context
"""

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import pandas as pd
import requests
import json
import os
import sys

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
REPORT_CSV    = os.path.join(BASE_DIR, "..", "output", "customer_loyalty_report.csv")
DASHBOARD_DIR = os.path.join(BASE_DIR, "..", "dashboard")
OLLAMA_URL    = "http://localhost:11434/api/generate"
OLLAMA_MODEL  = "llama3"

app = Flask(__name__)
CORS(app)   # allow the HTML page to call these APIs


# ── Helper: load report ───────────────────────────────────────────────────────
def load_report() -> list[dict]:
    if not os.path.exists(REPORT_CSV):
        return []
    df = pd.read_csv(REPORT_CSV)
    # Fill optional columns gracefully
    for col in ("churn_risk", "coupon_code"):
        if col not in df.columns:
            df[col] = "N/A"
    return df.to_dict(orient="records")


# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory(DASHBOARD_DIR, "index.html")


@app.route("/api/customers", methods=["GET"])
def get_customers():
    customers = load_report()
    return jsonify(customers)


@app.route("/api/chat", methods=["POST"])
def chat():
    body        = request.get_json(force=True)
    user_message = body.get("message", "").strip()
    if not user_message:
        return jsonify({"error": "Empty message"}), 400

    # ── Build rich customer context ──────────────────────────────────────────
    customers    = load_report()
    customer_ctx = ""

    # Check if the message mentions a customer name or ID
    mentioned = [
        c for c in customers
        if c["customer_name"].lower() in user_message.lower()
        or c["customer_id"].lower() in user_message.lower()
    ]

    if mentioned:
        c = mentioned[0]
        customer_ctx = f"""
=== CUSTOMER RECORD ===
ID                   : {c['customer_id']}
Name                 : {c['customer_name']}
Total Orders         : {c['total_orders']}
Total Spent          : ₹{c['total_spent']}
Average Order Value  : ₹{c['average_order_value']}
Loyalty Segment      : {c['loyalty_segment']}
Activity Status      : {c['customer_activity_status']}
Churn Risk           : {c.get('churn_risk', 'N/A')}
Coupon Code          : {c.get('coupon_code', 'N/A')}
=======================
"""
    else:
        # Give summary stats as context
        df = pd.DataFrame(customers)
        if not df.empty:
            customer_ctx = f"""
=== OVERALL ANALYTICS SUMMARY (May 2024) ===
Total Customers   : {len(df)}
Total Revenue     : ₹{df['total_spent'].sum():,.2f}
Active Customers  : {(df['customer_activity_status'] == 'ACTIVE_CUSTOMER').sum()}
Inactive Customers: {(df['customer_activity_status'] == 'INACTIVE_CUSTOMER').sum()}
Platinum          : {(df['loyalty_segment'] == 'PLATINUM').sum()}
Gold              : {(df['loyalty_segment'] == 'GOLD').sum()}
Silver            : {(df['loyalty_segment'] == 'SILVER').sum()}
Bronze            : {(df['loyalty_segment'] == 'BRONZE').sum()}
=============================================
"""

    system_prompt = f"""You are an expert Customer Analytics Assistant for a business intelligence dashboard.
You have access to real customer data from the analytics pipeline below.
When asked about a specific customer, give a concise professional report covering:
- Spending behaviour and loyalty tier justification
- Activity status analysis
- Churn risk assessment
- Personalised business recommendation (retention strategy, upsell opportunity, etc.)

Keep responses clear, structured, and professional. Use bullet points where helpful.
Always base your analysis on the actual numbers provided.

{customer_ctx}"""

    # ── Call Ollama ──────────────────────────────────────────────────────────
    try:
        ollama_payload = {
            "model":  OLLAMA_MODEL,
            "prompt": user_message,
            "system": system_prompt,
            "stream": False,
        }
        resp = requests.post(OLLAMA_URL, json=ollama_payload, timeout=120)
        resp.raise_for_status()
        ai_text = resp.json().get("response", "No response from model.")
        return jsonify({"response": ai_text})

    except requests.exceptions.ConnectionError:
        return jsonify({
            "response": "⚠️ Cannot connect to Ollama. Make sure Ollama is running: `ollama serve`"
        })
    except Exception as e:
        return jsonify({"response": f"⚠️ Error: {str(e)}"})


if __name__ == "__main__":
    os.makedirs(DASHBOARD_DIR, exist_ok=True)
    print("=" * 55)
    print("  Customer Analytics Dashboard")
    print("  http://localhost:5000")
    print("=" * 55)
    app.run(debug=True, port=5000)
