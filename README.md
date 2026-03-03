# DataSync Event Ingestion

A production-ready ingestion pipeline that extracts event data from the DataSync Analytics API and stores it in PostgreSQL. Runs entirely in Docker; resumable, rate-limit aware, with health checks and optional submission of results.

## Where was AI used?

AI used was cursor, and a little chatgpt. This is not my preferred setup (would prefer cli based solutions), but this is what is available right now.

* Database table creation (after being show data structures)
* Initial structure
* Rate limits
* Docker debugging and setup
* README
* Data submission
* Debugging
* Refactoring to class based structure once we had a working setup.


## How to Run

**Prerequisites:** Docker and Docker Compose.


1. **Start ingestion**
   ```bash
   bash run-ingestion.sh
   ```
   This will:
   - Build and start the stack (`docker compose up -d --build`)
   - Wait for Postgres to be healthy and set the DB password
   - Run the ingestion container (producer → submit if configured)
   - Poll the DB and print event counts until completion or you press Ctrl+C

2. **Manual control (optional)**
   ```bash
   docker compose up -d --build
   docker logs -f assignment-ingestion
   ```
   Health endpoint (while ingestion container is running):  
   `curl http://localhost:5001/health`

4. **Clean slate (remove containers and DB data)**
   ```bash
   docker compose down -v
   ```

## Architecture Overview

- **`packages/`** – Application code.
  - **`producer.py`** – Main ingestion. Class `API` fetches event pages from the DataSync API (cursor-based pagination), normalizes records, and inserts into PostgreSQL in batches via a thread pool. Progress and cursor are persisted so runs can resume after failure.
  - **`health.py`** – Flask server on port 5000 (mapped to host 5001). Serves `/` and `/health` with data read from `progress.json` (cursor, last page load time, pages, total inserted) and a `warnings` list (e.g. “No new pages in > 2 minutes” if the last update is stale).
  - **`submit.py`** – Runs after `producer.py` exits successfully. Streams all event IDs from the `ingested_events` table to a temp file (one ID per line), then POSTs to the DataSync submissions API with `X-API-Key`, `Content-Type: text/plain`, and `github_repo` query param. Skipped if `GITHUB_REPO_URL` is not set in `.env`.
  - **`entrypoint.sh`** – Starts the health server in the background, waits a few seconds for DNS, runs `producer.py`, then `submit.py` on success.

- **Docker**
  - **Postgres** – `assignment-postgres`, port 5434→5432, DB `ingestion`, user `postgres` / password `postgres`. Healthcheck before ingestion starts.
  - **Ingestion** – Built from `Dockerfile` (Python 3.11, venv, deps from `requirements.txt`). Uses `DATABASE_URL` from Compose to talk to Postgres; reads `.env` for API key, page limit, worker concurrency, etc.

- **Progress and resumability**
  - **`last_page_load.json`** – Cursor and last page load timestamp. Not deleted on startup; used to resume and to decide when to nullify a stale cursor (`CURSOR_REFRESH_THRESHOLD`).
  - **`progress.json`** – Written each page: cursor, last page load timestamp, pages, total inserted. Deleted at start of each run. Read by the health server for the `/health` payload.

## API Discoveries

- **Rate limits** – Response headers: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset` (seconds until window reset). We only sleep when `Remaining == 0` and there are more pages; then we wait `Reset` seconds. No fixed delay between requests when quota is available.
- **Pagination** – Cursor-based. `GET /events?limit=5000` (configurable via `API_PAGE_LIMIT`). Response: `data` (array of events), `pagination.nextCursor`, `pagination.hasMore`. Cursors can go stale; we reset to start if the last request is older than `CURSOR_REFRESH_THRESHOLD` seconds.
- **Limits** - Limit is not as low as it appears, can run many records in one request
- **Auth** – `X-API-Key` header and/or `apiKey` query parameter. We send both when the key is set.
- **Timestamps** – Mixed: numeric (milliseconds since epoch) or ISO strings (e.g. `2026-01-27T18:43:33.499Z`). We normalize to a single ISO format before storing.
## Configuration (.env)

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | Postgres connection (local runs); overridden in Docker by Compose |
| `TARGET_API_KEY` | DataSync API key |
| `API_BASE_URL` | DataSync API base (default: assignment URL) |
| `WORKER_CONCURRENCY` | Thread pool size for batch inserts (default 5) |
| `BATCH_SIZE` | Records per insert batch (default 10) |
| `API_PAGE_LIMIT` | Page size for `GET /events` (default 200; try 500/1000 if API allows) |
| `CURSOR_REFRESH_THRESHOLD` | Seconds after which the saved cursor is discarded and we start from the beginning (default 60) |
