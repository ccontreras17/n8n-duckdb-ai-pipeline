"""
Flask Marketing Metrics API
---------------------------

This microservice exposes ETL, KPI modeling, and natural-language query endpoints
for marketing performance data (ads spend, conversions, impressions, etc.).

Key Features:
    • Secure API with API key authentication.
    • /load      → Runs ETL to ingest and persist CSV files into DuckDB.
    • /metrics   → Computes CAC and ROAS.
                   - compare mode: last 30 days vs prior 30 days
                   - single mode: aggregates within custom date ranges
    • /ask       → Accepts natural language questions about CAC/ROAS and
                   optionally calls OpenAI GPT to generate a short summary.
    • /health    → Lightweight health check endpoint.

Tech Stack:
    - Flask (REST API)
    - DuckDB (analytics warehouse)
    - Pandas (data manipulation / formatting)
    - dotenv (config and secrets)
    - OpenAI API (optional summarization)

Usage Example:
    1. POST /load                 # Ingest latest CSVs
    2. GET  /metrics?mode=compare # Return CAC/ROAS, last30 vs prior30
    3. POST /ask {"question": "..."}
       → JSON with metrics table + AI-generated summary (if API key configured)


"""

import os
import re
import pandas as pd
from flask import Flask, request, jsonify
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY", "")

# OpenAI
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL     = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
LLM_TEMPERATURE  = float(os.getenv("LLM_TEMPERATURE", "0.5"))

# Import ETL function
from load_duckdb import main as run_loader

# Metrics core
from metrics_core import compute_kpi_compare, compute_kpi_single, max_data_date, anchor_date, df_to_markdown_table

app = Flask(__name__)

def authorized(req) -> bool:
    return API_KEY and req.headers.get("X-API-Key") == API_KEY

@app.get("/health")
def health():
    return {"status": "ok"}, 200

@app.post("/load")
def load():
    if not authorized(request):
        return jsonify({"error": "unauthorized"}), 401
    try:
        run_loader()  # run the ETL once
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 500

@app.get("/metrics")
def metrics():
    """
    GET /metrics?mode=compare|single&start=YYYY-MM-DD&end=YYYY-MM-DD&group_by=platform,country
    - compare (default): last 30 vs prior 30 (ignores start/end)
    - single: aggregates within [start,end] (defaults to 30 days ending at anchor if missing)
    """
    if not authorized(request):
        return jsonify({"error": "unauthorized"}), 401

    mode = (request.args.get("mode") or "compare").lower()
    start = request.args.get("start")
    end   = request.args.get("end")
    group_by = request.args.get("group_by")

    try:
        if mode == "single":
            payload = compute_kpi_single(start=start, end=end, group_by=group_by)
        else:
            payload = compute_kpi_compare()
        return jsonify(payload), 200
    except ValueError as ve:
        return jsonify({"error": "bad_request", "detail": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": "internal_error", "detail": str(e)}), 500

# Retriever
_KPI_PAT = re.compile(r"\b(cac|roas)\b", re.I)
_LAST30_PAT = re.compile(r"\b(last\s*30|past\s*30|últimos\s*30)\b", re.I)
_PRIOR30_PAT = re.compile(r"\b(prior\s*30|previous\s*30|prev\s*30|anteriores\s*30)\b", re.I)
_COMPARE_PAT = re.compile(r"\b(compare|vs|versus|comparar)\b", re.I)

@app.post("/ask")
def ask():
    """
    POST /ask  { "question": "Compare CAC and ROAS for last 30 days vs prior 30 days." }
    - If we detect the compare intent (CAC/ROAS + compare + last30 + prior30), we run KPI compare.
    - If OPENAI_API_KEY is present, we call OpenAI and return a short friendly summary.
    """
    if not authorized(request):
        return jsonify({"error": "unauthorized"}), 401

    body = request.get_json(silent=True) or {}
    question = (body.get("question") or body.get("q") or "").strip()
    if not question:
        return jsonify({"error": "missing 'question'"}), 400

    wants_kpi_compare = (
        bool(_KPI_PAT.search(question)) and
        bool(_COMPARE_PAT.search(question)) and
        bool(_LAST30_PAT.search(question)) and
        bool(_PRIOR30_PAT.search(question))
    )

    if not wants_kpi_compare:
        return jsonify({
            "meta": {"matched_intent": False},
            "message": "No KPI compare intent detected. Try: 'Compare CAC and ROAS for last 30 days vs prior 30 days.'"
        }), 200

    try:
        payload = compute_kpi_compare()
        df = pd.DataFrame(payload["data"])
        md_table = df_to_markdown_table(df)
        a = payload["meta"].get("anchor_date")

        system_prompt = (
            "You are a friendly but precise marketing analyst. "
            "Explain KPI changes factually using the table provided. "
            "Call out notable increases/decreases in CAC and ROAS, and mention any caveats."
        )
        user_prompt = (
            f"Question: {question}\n\n"
            f"Anchor date (end of last-30 window): {a}\n"
            f"Metrics table (last 30 vs prior 30):\n{md_table}\n\n"
            "Summarize CAC and ROAS changes in 1–2 short sentences."
        )

        answer = None
        if OPENAI_API_KEY:
            # OpenAI call (server-side)
            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY)
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                temperature=LLM_TEMPERATURE,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt}
                ],
            )
            answer = resp.choices[0].message.content if resp.choices else None

        return jsonify({
            "meta": {
                "matched_intent": True,
                "mode": "compare",
                "anchor_date": a,
                "temperature": LLM_TEMPERATURE,
                "openai_model": OPENAI_MODEL if OPENAI_API_KEY else None
            },
            "prompt_preview": {
                "system": system_prompt,
                "user": user_prompt
            },
            "answer": answer,
            "data": payload["data"]
        }), 200

    except Exception as e:
        return jsonify({"error": "internal_error", "detail": str(e)}), 500

if __name__ == "__main__":
    # expose to other containers via service name
    app.run(host="0.0.0.0", port=5000)

