from flask import Flask, request, jsonify
from flask_cors import CORS
from query_safety import is_safe_sql
from db import run_select
from llm_client import ask_gemini_for_sql
import os
import traceback

app = Flask(__name__)
CORS(app)

MAX_ROWS = 200

@app.route("/api/chat", methods=["POST"])
def chat():
    payload = request.json or {}
    user_message = payload.get("message", "")
    chat_history = payload.get("history", []) # <-- New: Get chat history

    if not user_message:
        return jsonify({"error": "No message provided"}), 400
    
    try:
        # Pass history to the LLM
        llm_out = ask_gemini_for_sql(user_message, chat_history)
        
        sql = llm_out.get("sql")
        params = llm_out.get("params", [])
        explain = llm_out.get("explain", "")
        chart = llm_out.get("chart", {"type": "none"}) # <-- New: Get chart info

        if not sql:
            # If LLM didn't return SQL (e.g., "hello"), just return its explanation
            if explain:
                return jsonify({
                    "ok": True,
                    "explain": explain,
                    "rows": [],
                    "sql": None,
                    "chart": {"type": "none"}
                })
            return jsonify({"error": "LLM did not return SQL.", "llm_raw": llm_out}), 500

        ok, reason = is_safe_sql(sql)
        if not ok:
            return jsonify({"error": "Rejected SQL for safety: " + reason, "candidate_sql": sql}), 400

        rows = run_select(sql, params, limit=MAX_ROWS)

        # New: Simplified response. Frontend will do all the formatting.
        return jsonify({
            "ok": True,
            "explain": explain,
            "rows": rows,
            "sql": sql,
            "chart": chart # <-- New: Pass chart info to frontend
        })
    
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": "Server error: " + str(e)}), 500

if __name__ == "__main__":
    app.run(host=os.environ.get("FLASK_HOST","0.0.0.0"), port=int(os.environ.get("FLASK_PORT",5000)))