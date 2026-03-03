#!/usr/bin/env python3
"""
Producer: fetch pages, insert each page's records. Class-based API.
"""
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import psycopg2
import requests
from dotenv import load_dotenv

# In Docker, compose sets DATABASE_URL; do not let .env override it so container uses postgres:5432.
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


class API:
    def __init__(self) -> None:
        self._base = "/var/app" if os.path.exists("/var/app") else "."
        self.progress_path = os.path.join(self._base, "progress.json")
        self.last_page_load_path = os.path.join(self._base, "last_page_load.json")
        self.events_table = "ingested_events"
        self.api_url = os.getenv("API_BASE_URL", API_BASE_URL_DEFAULT).rstrip("/")
        self.api_key = (
            os.getenv("TARGET_API_KEY") or os.getenv("apiKey") or os.getenv("API_KEY") or ""
        )
        self.max_workers = int(os.getenv("WORKER_CONCURRENCY", "5"))
        self.batch_size = int(os.getenv("BATCH_SIZE", "10"))
        self._session = requests.Session()
        self._session.headers.setdefault("Accept", "application/json")
        if self.api_key:
            self._session.headers["X-API-Key"] = self.api_key

    def load_progress(self) -> dict:
        """Read cursor from last_page_load.json; nullify cursor if last_page_load_timestamp is too old."""
        if not os.path.exists(self.last_page_load_path):
            with open(self.last_page_load_path, "w", encoding="utf-8") as f:
                json.dump({"cursor": None, "last_page_load_timestamp": None}, f)
            return {"cursor": None}
        with open(self.last_page_load_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        cursor = data.get("cursor")
        last_ts = data.get("last_page_load_timestamp")
        if last_ts is not None:
            try:
                threshold = int(os.getenv("CURSOR_REFRESH_THRESHOLD", "60"))
                if int(time.time()) - int(last_ts) > threshold:
                    logger.warning("Cursor refresh threshold exceeded. Resetting cursor to None")
                    cursor = None
            except (TypeError, ValueError):
                pass
        return {"cursor": cursor}

    def save_progress(
        self,
        cursor: str | None,
        last_page_load_timestamp: float | None,
        pages: int,
        total_inserted: int,
    ) -> None:
        ts = last_page_load_timestamp if last_page_load_timestamp is not None else time.time()
        with open(self.last_page_load_path, "w", encoding="utf-8") as f:
            json.dump({"cursor": cursor, "last_page_load_timestamp": ts}, f)
        with open(self.progress_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "cursor": cursor,
                    "last_page_load_timestamp": ts,
                    "total_inserted": total_inserted,
                    "pages": pages,
                },
                f,
            )

    @staticmethod
    def _parse_rate_limit_headers(headers: Any) -> dict[str, int] | None:
        """Parse X-RateLimit-* headers. Returns dict with 'remaining' and 'reset' (seconds), or None."""
        try:
            remaining = headers.get("X-RateLimit-Remaining")
            reset = headers.get("X-RateLimit-Reset")
            if remaining is None and reset is None:
                return None
            out = {}
            if remaining is not None:
                out["remaining"] = int(remaining)
            if reset is not None:
                out["reset"] = int(reset)
            return out if out else None
        except (TypeError, ValueError):
            return None

    def get_events(
        self,
        cursor: str | None,
        retries: int = 0,
        max_retries: int = 3,
        retry_delay: float = 4.0,
    ) -> tuple[list[dict], str | None, bool, dict[str, int] | None]:
        url = f"{self.api_url}/events"
        params = {"limit": 5000}
        if cursor:
            params["cursor"] = cursor
        if self.api_key:
            params["apiKey"] = self.api_key
        try:
            r = self._session.get(url, params=params, timeout=30)
            rate_limit = self._parse_rate_limit_headers(r.headers)
            if rate_limit is not None:
                logger.info("Rate limit: %s", rate_limit)
            try:
                r.raise_for_status()
            except requests.RequestException as e:
                if retries < max_retries:
                    logger.warning("API request failed: %s, retrying...", e)
                    time.sleep(retry_delay)
                    return self.get_events(cursor, retries + 1, max_retries, retry_delay)
                else:
                    logger.exception("API request failed: %s", e)
                    return ([], None, False, None)
            body = r.json()
            data = body.get("data", []) if isinstance(body, dict) else []
            if not isinstance(data, list):
                data = []
            pag = body.get("pagination", {}) if isinstance(body, dict) else {}
            return (data, pag.get("nextCursor"), pag.get("hasMore", False), rate_limit)
        except requests.RequestException as e:
            logger.exception("API request failed: %s", e)
            return ([], None, False, None)

    @staticmethod
    def _normalize_timestamp(ts: Any) -> str:
        """Normalize timestamp to ISO format. API sends either ms (number) or ISO string (e.g. 2026-01-27T18:43:33.499Z)."""
        if ts is None:
            return ""
        if isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
            return dt.isoformat().replace("+00:00", "Z")
        if isinstance(ts, str):
            s = ts.strip()
            if not s:
                return ""
            try:
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat().replace("+00:00", "Z")
            except (ValueError, TypeError):
                return ts
        return str(ts)

    @staticmethod
    def _normalize_record(r: dict) -> dict:
        session = r.get("session") or {}
        ts = API._normalize_timestamp(r.get("timestamp"))
        # API: top-level sessionId or session.id (same value)
        session_id = r.get("sessionId") or r.get("session_id") or session.get("id") or ""
        user_id = r.get("userId") or r.get("user_id") or ""
        return {
            "id": r.get("id") or "",
            "session_id": session_id,
            "user_id": user_id,
            "type": r.get("type") or "",
            "name": r.get("name") or "",
            "device": session.get("deviceType") or "",
            "timestamp": ts,
            "properties": json.dumps(r.get("properties")) if r.get("properties") is not None else None,
        }

    @staticmethod
    def get_connection() -> Any:
        url = os.getenv("DATABASE_URL")
        if not url:
            return None
        return psycopg2.connect(url)

    def create_table(self, conn: Any) -> None:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.events_table} (
                    id VARCHAR(255) PRIMARY KEY,
                    session_id VARCHAR(255),
                    user_id VARCHAR(255),
                    type VARCHAR(100) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    device VARCHAR(50),
                    timestamp TEXT NOT NULL,
                    properties JSONB
                )
                """
            )
        conn.commit()

    def _insert_batch(self, records: list[dict]) -> int:
        """Insert a batch of records in one query. Returns the buffer size"""
        if not records:
            logger.error("_insert_batch: no records")
            return 0
        conn = self.get_connection()
        if not conn:
            logger.warning("_insert_batch: no connection (DATABASE_URL unset?)")
            return 0
        try:
            rows = [self._normalize_record(r) for r in records]
            one = "(%s, %s, %s, %s, %s, %s, %s, %s::jsonb)"
            placeholders = ", ".join([one] * len(rows))
            values = []
            for r in rows:
                values.extend([r["id"], r["session_id"], r["user_id"], r["type"], r["name"], r["device"], r["timestamp"], r["properties"]])
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.events_table} (id, session_id, user_id, type, name, device, timestamp, properties)
                    VALUES {placeholders}
                    ON CONFLICT (id) DO NOTHING
                    RETURNING id
                    """,
                    values,
                )
                inserted = len(cur.fetchall())
            conn.commit()
            return inserted
        except Exception as e:
            logger.exception("_insert_batch failed: %s", e)
            raise
        finally:
            conn.close()

    def insert_page_with_futures(self, records: list[dict]) -> int:
        """Insert records in batches via futures; batch_size from env BATCH_SIZE. Returns total rows actually inserted."""
        if not isinstance(records, list):
            records = []
        if not records:
            return 0
        batch_size = max(1, int(self.batch_size))
        chunks = [records[i : i + batch_size] for i in range(0, len(records), batch_size)]
        total = 0
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._insert_batch, chunk) for chunk in chunks]
            for f in as_completed(futures):
                total += f.result()
        return total

    def run_ingestion(self) -> None:
        if os.path.exists(self.progress_path):
            os.remove(self.progress_path)
        progress = self.load_progress()
        cursor = progress.get("cursor")

        conn = self.get_connection()
        if not conn:
            logger.warning("DATABASE_URL not set")
            return
        self.create_table(conn)
        conn.close()

        pages = 0
        total_inserted = 0
        while True:
            records, next_cursor, has_more, rate_limit = self.get_events(cursor)
            pages += 1
            if rate_limit is not None and rate_limit.get("remaining", 1) == 0 and has_more:
                reset_sec = rate_limit.get("reset", 60)
                logger.info("Rate limit exhausted, waiting %s seconds until reset", reset_sec)
                time.sleep(reset_sec)
            n = self.insert_page_with_futures(records)
            total_inserted += n
            logger.info("Page %d: %d events, inserted %d (total inserted %d)", pages, len(records), n, total_inserted)
            self.save_progress(next_cursor, time.time(), pages, total_inserted)
            cursor = next_cursor
            if not has_more or not next_cursor:
                break

        logger.info("Done. Pages: %d, events inserted: %d", pages, total_inserted)


if __name__ == "__main__":
    api = API()
    api.run_ingestion()
