#!/usr/bin/env python3
"""
Submit ingested event IDs to the DataSync submissions API after producer.py completes.
Builds event_ids.txt from the DB (one ID per line) and POSTs per README: X-API-Key,
Content-Type: text/plain, query param github_repo. Set GITHUB_REPO_URL in .env.
"""
import logging
import os
import tempfile

import psycopg2
import requests
from dotenv import load_dotenv

_env_db_before = os.environ.get("DATABASE_URL") if os.path.exists("/var/app") else None
if os.path.exists("/var/app/.env"):
    load_dotenv("/var/app/.env", override=False)
if os.path.exists(".env"):
    load_dotenv(".env", override=False)
load_dotenv(override=False)
if _env_db_before is not None:
    os.environ["DATABASE_URL"] = _env_db_before

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

API_BASE_URL_DEFAULT = "http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1"
EVENTS_TABLE = "ingested_events"


def run_submit() -> None:
    api_url = os.getenv("API_BASE_URL", API_BASE_URL_DEFAULT).rstrip("/")
    api_key = os.getenv("TARGET_API_KEY","")
    github_repo = (os.getenv("GITHUB_REPO_URL") or os.getenv("GITHUB_REPO") or "").strip()
    if not github_repo:
        logger.info("GITHUB_REPO_URL not set; skipping submission")
        return
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.warning("DATABASE_URL not set; cannot submit")
        return

    # Build file from DB: all event IDs, one per line (per README)
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor(name="submit_ids") as cur:
            cur.execute(f"SELECT id FROM {EVENTS_TABLE} ORDER BY id")
            count = 0
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".txt",
                delete=False,
                encoding="utf-8",
                newline="\n",
            ) as f:
                path = f.name
                while True:
                    rows = cur.fetchmany(50_000)
                    if not rows:
                        break
                    for (id_val,) in rows:
                        f.write(id_val + "\n")
                        count += 1
        if count == 0:
            logger.warning("No event IDs in database; not submitting")
            return
        logger.info("Built event IDs file from DB: %d IDs", count)

        # Submit per README: POST, X-API-Key, Content-Type: text/plain, github_repo query param
        url = f"{api_url}/submissions"
        params = {"github_repo": github_repo}
        headers = {
            "X-API-Key": api_key,
            "Content-Type": "text/plain",
        }
        with open(path, "rb") as f:
            r = requests.post(url, params=params, headers=headers, data=f, timeout=600)
        try:
            os.unlink(path)
        except OSError:
            pass
        r.raise_for_status()
        body = r.json() if r.content else {}
        logger.info("Submission response: %s", body)
        if isinstance(body, dict) and body.get("success"):
            logger.info("Submission succeeded.")
        else:
            logger.warning("Submission response: %s", r.text)
    finally:
        conn.close()


if __name__ == "__main__":
    run_submit()
