"""
analytics_server.py  —  Fixed version
- Smart context builder (not raw CSV dump)
- Handles: summary, churn, upsell, specific customer, top spenders
- Short focused prompts → Ollama responds reliably
"""

import os
import pandas as pd
import requests
from flask import Flask, jsonify, request, Response, send_from_directory
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORT_FILE = os.path.join(BASE_DIR, "output", "customer_loyalty_report.csv")
OLLAMA_URL  = "http://localhost:11434/api/generate"
MODEL_NAME  = "llama3:latest"


# ── helpers ───────────────────────────────────────────────────────────────────

def load_df():
    if not os.path.exists(REPORT_FILE):
        return None
    try:
        return pd.read_csv(REPORT_FILE)
    except Exception:
        return None


def build_context(user_input: str, df: pd.DataFrame) -> str:
    """
    Build a SHORT, focused context string instead of dumping the whole CSV.
    This is why Ollama was failing — token overload + no structure.
    """
    msg = user_input.lower()

    # 1. Specific customer mentioned?
    for _, row in df.iterrows():
        name = str(row.get("customer_name", "")).lower()
        cid  = str(row.get("customer_id",   "")).lower()
        if name in msg or cid in msg:
            return f"""CUSTOMER RECORD
---------------
ID                 : {row['customer_id']}
Name               : {row['customer_name']}
Total Orders       : {row['total_orders']}
Total Spent        : Rs {row['total_spent']}
Avg Order Value    : Rs {row['average_order_value']}
Loyalty Segment    : {row['loyalty_segment']}
Activity Status    : {row['customer_activity_status']}
Churn Risk         : {row.get('churn_risk', 'N/A')}
Coupon Code        : {row.get('coupon_code', 'N/A')}"""

    # 2. Churn-related question
    if any(w in msg for w in ["churn", "risk", "losing", "inactive"]):
        inactive = df[df["customer_activity_status"] == "INACTIVE_CUSTOMER"]
        lines = ["INACTIVE / HIGH CHURN RISK CUSTOMERS"]
        for _, r in inactive.iterrows():
            lines.append(f"  - {r['customer_name']} ({r['customer_id']}) | Segment: {r['loyalty_segment']} | Spent: Rs {r['total_spent']} | Orders: {r['total_orders']}")
        return "\n".join(lines)

    # 3. Top spenders / upsell
    if any(w in msg for w in ["top", "spend", "upsell", "platinum", "upgrade", "best"]):
        top = df.nlargest(5, "total_spent")[["customer_name","customer_id","total_spent","loyalty_segment","total_orders"]]
        lines = ["TOP 5 CUSTOMERS BY SPENDING"]
        for _, r in top.iterrows():
            lines.append(f"  {r['customer_name']} ({r['customer_id']}) | Rs {r['total_spent']} | Segment: {r['loyalty_segment']} | Orders: {r['total_orders']}")
        return "\n".join(lines)

    # 4. Segment breakdown
    if any(w in msg for w in ["segment", "gold", "silver", "bronze", "distribution"]):
        counts = df["loyalty_segment"].value_counts().to_dict()
        lines  = ["LOYALTY SEGMENT BREAKDOWN"]
        for seg, cnt in counts.items():
            spent = df[df["loyalty_segment"] == seg]["total_spent"].sum()
            lines.append(f"  {seg}: {cnt} customers | Total Revenue: Rs {spent:,.2f}")
        return "\n".join(lines)

    # 5. Default: clean summary stats
    total    = len(df)
    revenue  = df["total_spent"].sum()
    active   = (df["customer_activity_status"] == "ACTIVE_CUSTOMER").sum()
    inactive = total - active
    seg      = df["loyalty_segment"].value_counts().to_dict()
    avg_order = df["average_order_value"].mean()

    return f"""ANALYTICS SUMMARY — May 2024
-----------------------------
Total Customers    : {total}
Total Revenue      : Rs {revenue:,.2f}
Active Customers   : {active}
Inactive Customers : {inactive}
Avg Order Value    : Rs {avg_order:,.2f}

Loyalty Breakdown:
  Platinum : {seg.get('PLATINUM', 0)}
  Gold     : {seg.get('GOLD', 0)}
  Silver   : {seg.get('SILVER', 0)}
  Bronze   : {seg.get('BRONZE', 0)}"""


def ask_ollama(system_prompt: str, user_prompt: str) -> str:
    full_prompt = f"{system_prompt}\n\nQuestion: {user_prompt}"
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={"model": MODEL_NAME, "prompt": full_prompt, "stream": False},
            timeout=180,
        )
        resp.raise_for_status()
        return resp.json().get("response", "No response generated.")
    except requests.exceptions.ConnectionError:
        return "Ollama is not running. Please start it with: ollama serve"
    except requests.exceptions.HTTPError:
        if resp.status_code == 404:
            return f"Model '{MODEL_NAME}' not found. Run: ollama pull llama3"
        return f"Ollama HTTP error: {resp.status_code}"
    except Exception as e:
        return f"Unexpected error: {e}"


# ── routes ────────────────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
def index():
    for folder in (
        os.path.join(BASE_DIR, "dashboard"),
        os.path.abspath(os.path.join(BASE_DIR, "..", "analytics-dashboard", "dashboard")),
    ):
        if os.path.exists(os.path.join(folder, "index.html")):
            return send_from_directory(folder, "index.html")
    return jsonify({"message": "Analytics API running.", "endpoints": ["/api/customers", "/api/chat"]}), 200


@app.route("/api/customers", methods=["GET"])
def get_customers():
    df = load_df()
    if df is None:
        return jsonify({"error": "Report not found. Run main.py first."}), 404
    return Response(df.to_json(orient="records"), mimetype="application/json")


@app.route("/api/chat", methods=["POST"])
def chat():
    body       = request.get_json(force=True)
    user_input = body.get("message", "").strip()
    if not user_input:
        return jsonify({"response": "Please enter a message."}), 400

    df = load_df()
    if df is None:
        return jsonify({"response": "No data file found. Run main.py first."}), 404

    context = build_context(user_input, df)

    system_prompt = (
        "You are a professional customer analytics assistant. "
        "Answer concisely and clearly based only on the data below. "
        "Use bullet points. Keep responses under 200 words unless a detailed report is asked.\n\n"
        f"DATA:\n{context}"
    )

    answer = ask_ollama(system_prompt, user_input)
    return jsonify({"response": answer})


# ── main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("  Analytics Dashboard Server")
    print("  http://localhost:5000")
    print(f"  Report : {REPORT_FILE}")
    print("=" * 50)
    app.run(port=5000, debug=True, use_reloader=False)