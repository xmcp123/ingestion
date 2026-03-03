#!/usr/bin/env python3
"""Small Flask server that serves progress data and health warnings."""
import json
import os
import time
from flask import Flask, jsonify

app = Flask(__name__)
_BASE = "/var/app" if os.path.exists("/var/app") else "."
# progress.json is the Flask endpoint: cursor, last_page_load_timestamp, total_inserted, pages
PROGRESS_PATH = os.path.join(_BASE, "progress.json")
STALE_SECONDS = 120


def load_health_data() -> dict:
    """Read only progress.json (Flask endpoint). Deleted on producer startup so may be missing."""
    if not os.path.exists(PROGRESS_PATH):
        return {
            "cursor": None,
            "last_page_load_timestamp": None,
            "total_inserted": 0,
            "pages": 0,
        }
    try:
        with open(PROGRESS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {
            "cursor": data.get("cursor"),
            "last_page_load_timestamp": data.get("last_page_load_timestamp"),
            "total_inserted": data.get("total_inserted", 0),
            "pages": data.get("pages", 0),
        }
    except (json.JSONDecodeError, OSError):
        return {
            "cursor": None,
            "last_page_load_timestamp": None,
            "total_inserted": 0,
            "pages": 0,
        }


@app.route("/")
@app.route("/health")
def health():
    data = load_health_data()
    warnings = []
    last_ts = data.get("last_page_load_timestamp")
    if last_ts is not None and last_ts > 0:
        elapsed = time.time() - last_ts
        if elapsed > STALE_SECONDS:
            warnings.append("No new pages in > 2 minutes")
    response = {**data, "warnings": warnings}
    return jsonify(response)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
