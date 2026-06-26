# CLAUDE.md

Guidance for working in this repository.

## What this is

An async Python 3 scraper that walks configurable [OrthodoxWiki](https://orthodoxwiki.org)
categories and stores page content + attribution metadata in **PostgreSQL or
SQLite** (selectable in config). It downloads per-category media, tracks
crawl/seen times, soft-removes deleted pages, and captures the data needed to
redistribute content with proper attribution.

It talks to the **MediaWiki API** (`api.php`), never scraped HTML.

## Searching this repo (qmd)

This repo is indexed in **qmd** as the `ortho_scraper` collection (path
`/Users/jason/code/ortho_scraper`, pattern `**/*.{md,py,sql,txt,conf}` — all the
Python, SQL, config, and docs). **Consult the qmd index first when searching or
reading files in this folder** — prefer it over blind `grep`/`Read` sweeps:

- Search: `mcp__qmd__query` with `collections: ["ortho_scraper"]`. Combine a
  `lex` sub-query (exact symbols like `store_page`, `redistribution_level`) with
  a `vec` sub-query (concepts like "how are deleted pages reconciled") for recall.
- Retrieve: `mcp__qmd__get` for a single file (supports `path:line` offsets) and
  `mcp__qmd__multi_get` for a glob (e.g. `storage*.py`).
- Check freshness with `mcp__qmd__status`; the index is rebuilt out-of-band, so
  after editing a file, read it directly to confirm current contents rather than
  trusting a possibly-stale snippet.

## Running it

```sh
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
./.venv/bin/python main.py --config scraper.conf        # one pass (good for cron)
./.venv/bin/python main.py --config scraper.conf --loop 6h
./.venv/bin/python main.py --config scraper.conf -v     # debug logging
```

There is no test suite. Verify changes by byte-compiling and running a small
live crawl against SQLite:

```sh
./.venv/bin/python -m py_compile *.py
```

For live checks, point a throwaway `.conf` at `backend = "sqlite"`, a `/tmp`
path, and a single small category, and monkeypatch `iter_category_members` to
cap members (see how prior verification was done). **Always send a real
`User-Agent` with a contact email** — MediaWiki etiquette, and the configured
default identifies this scraper.

## Architecture

Flat module layout; the crawl pipeline lives in `scraper.py`.

- `main.py` — CLI + run/loop orchestration; builds the client, limiter, storage.
- `config.py` — loads the HOCON `.conf` into typed dataclasses. Hand-rolled
  `parse_duration` / `parse_size` (pyhocon parses `7 days` into a `timedelta`
  natively but not sizes; both helpers accept either form).
- `mediawiki.py` — async API client + `PageContent` / `MediaFile` dataclasses
  and `classify_media`. All requests go through `_get` (rate-limited, retried,
  JSON). Handles **continuation** for list queries.
- `ratelimit.py` — token bucket (sustained rate + burst) and a concurrency
  semaphore, combined in `RateLimiter` (an async context manager).
- `licenses.py` — maps OrthodoxWiki license templates to normalized license info.
- `storage.py` — backend-agnostic `Storage` ABC + `create_storage()` factory +
  shared `parse_ts`. `storage_postgres.py` (asyncpg) and `storage_sqlite.py`
  (aiosqlite) implement it.
- `scraper.py` — `Scraper.run()`: discover → mark_seen → crawl stale → reconcile
  deletions.

### Icon & Saints data layer (second pipeline, `--mode icons` / `--mode notify`)

A licensing-first pipeline independent of the wiki scraper, in the same DB,
**disabled by default** (`icons.enabled`). Powers a saints/icons consumer feature
(search/save/follow + notifications). Modules:

- `license_gate.py` — per-record, **fail-closed** gate (`approved`/`quarantined`/
  `rejected`). Pure/sync classifier; `icon_pipeline` applies `license_overrides`
  (human decisions) around it.
- `icon_sources.py` — `SourceAdapter`s for `met_api` (PD per-object), `wikimedia`
  (per-file tag vs. allowlist), `iconsaint` (blanket CC BY, local dataset).
  Adapters surface only the license signal; the gate decides. All HTTP is
  rate-limited via the shared `RateLimiter`.
- `icon_pipeline.py` — `IconPipeline.run()`: seed sources (re-flag approvals on
  `base_license` change) → per record: override/gate → resolve saint →
  **materialize image only if approved** (content-addressed like wiki media,
  with sidecar) → `store_icon` → emit `new_icon_added` when a new icon is
  approved for an already-followed saint.
- `notifications.py` — daily job; recurring events match `MM-DD`, one-offs match
  today; once-per-day-per-user dedup.

Invariants here: **only `crawl_status='approved'` is servable** (the app must
filter on it); the gate fails closed; **images and text/bios are licensed
independently** (`saints.bio_text` withheld while `bio_license` IS NULL); dates
are **UTC** end-to-end (`new_icon_added` event_date, the notify job's "today",
and `notifications_sent` must agree, or same-day dedup breaks). The new tables
(`sources`, `saints`, `icons`, `favorites`, `follows`, `events`,
`license_overrides`, `notifications_sent`, `users`) are additive
`CREATE TABLE IF NOT EXISTS` in both schema files — no ALTER migration needed,
but still keep both backends in lockstep.

### The crawl pipeline (scraper.py)

1. **discover()** — BFS over configured categories, recursing into subcategories
   up to `max_subcategory_depth`. Each page record carries `roots` = the set of
   *configured* root categories it descends from (subcategory pages inherit
   their root). `roots` drives the media policy and the seen-placeholder
   categories.
2. **mark_seen** — upserts/bumps `last_seen` for everything discovered; inserts
   new pages; clears `removed_at` (resurrection).
3. **crawl changed** — change detection, not pure time: a cheap `prop=info`
   probe (`fetch_latest_revids`) gets each known page's current `lastrevid`, and
   content is fetched iff the page is new or its lastrevid differs from the
   stored `revid`. `recrawl_after` is only a fallback forced-refresh of unchanged
   pages (>0 to enable; catches re-uploaded media). Content is fetched in batches
   of `BATCH_SIZE` (50). Per crawled page: media download, contributor fetch,
   attribution string, then a single `store_page`.
4. **reconcile deletions** — pages in the DB not seen this run get a cheap
   existence probe; only those the API confirms gone are soft-removed
   (`removed_at` stamped). Pages that merely left categories are kept.

## Key invariants — don't break these

- **MediaWiki version is old (1.30, no CommonsMetadata).** So: `rvslots` is
  ignored (read content from `rev["content"]` with a `slots.main` fallback);
  `extmetadata`/site `rights` are empty — license info comes from `licenses.py`
  parsing File: page wikitext, not the API.
- **The backend 502s on heavy queries.** File: page wikitext (the
  content-bearing query) is fetched separately from imageinfo — `fetch_imageinfo`
  is metadata-only (batch 50); `fetch_file_descriptions` pulls content at
  `DESCRIPTION_BATCH_SIZE` (10) and only for files that pass the media policy.
  Don't merge them back into one `imageinfo|revisions` query. Transient 5xx is
  retried with jittered backoff and logged at DEBUG; only exhaustion warns.
- **HTTP error policy (apply this everywhere, don't ask):**
  - **5xx / network / timeout** (`aiohttp.ClientError`, `asyncio.TimeoutError`):
    *transient* — retry with jittered exponential backoff, log retries at DEBUG,
    warn only on exhaustion (`_HttpJson` and `MediaWikiClient._get` already do this).
  - **4xx** (404 deleted/missing, 403, ...): *permanent* — do **not** retry.
    Skip the one bad item, log at DEBUG/WARNING, and keep the batch/loop going.
    One bad object must never derail the whole run.
  - **MediaWiki "soft" errors**: `action=parse`/`action=query` on a missing page
    returns **HTTP 200 with an `error` object**, not a 4xx. Check for `"error"`
    in the JSON and treat it like a 404 (warn + skip), since `raise_for_status()`
    won't catch it.
- **`last_seen` vs `last_crawled` are distinct** — one tracks "still in a
  category," the other "content last fetched." Keep them separate.
- **Recrawl is change-detected via `revid`**, not time. `store_page` must keep
  `revid` accurate (it's compared against the live `lastrevid` probe to decide
  re-fetch). `recrawl_after` is only a fallback; don't make it the primary gate.
- **`pages.categories` is authoritative API membership**, written by
  `store_page`. `mark_seen` must NOT overwrite it (only seeds a placeholder on
  insert) — otherwise non-recrawled pages get downgraded to discovery roots.
- **Media bytes are never stored in the DB** — files are content-addressed on
  disk at `<download_dir>/<sha1[:2]>/<sha1><ext>` (free dedupe). The DB holds
  *references*: `pages.media_paths` (per-page path list) and a `media` table with
  one row per file (`media_id`=sha1, path, mime, source_url, license_name, and
  the `redistribution` enum). Full per-file attribution also goes in a
  `<file>.json` sidecar. The `redistribution` level comes from `licenses.py`
  (`redistribution_level`) — public/free/restricted/prohibited, most permissive
  tag wins, unknown → prohibited.
- **Schema changes need a migration** for existing DBs: Postgres uses
  `ADD COLUMN IF NOT EXISTS`; SQLite needs a guarded `PRAGMA table_info` + `ALTER`
  in `storage_sqlite.apply_schema` (run before the schema script, since indexes
  may reference the new column). Update *both* schema files and *both* backends.
- **Both backends must stay in lockstep** — any `Storage` method or `store_page`
  signature change must land in `storage_postgres.py` and `storage_sqlite.py`,
  with SQLite using JSON for array-typed columns (`categories`, `media_paths`,
  `contributors`) and ISO-8601 UTC strings for timestamps.
- **Be polite**: every outbound request (API, file download, existence probe)
  goes through the shared `RateLimiter`. Don't add un-throttled HTTP calls.

## Licensing (important for this project's purpose)

The scraped data is intended for **redistribution**. OrthodoxWiki text is
dual-licensed **GFDL + CC BY-SA 2.5** (it is *not* NoDerivs). Image licenses vary
per file. The scraper captures `pages.contributors` + `pages.attribution` and
per-file sidecar `licenses` so attribution is possible — preserve this when
changing the crawl. See the README's "Licensing & attribution" section.

## Conventions

- Async throughout; never block the event loop (file writes use
  `asyncio.to_thread`).
- Config is file-based HOCON; add new knobs as typed dataclass fields + a loader
  in `config.py` and document them in `scraper.conf` and the README.
- Logging via the `ortho_scraper.*` logger hierarchy, not `print`. The run is
  heavily instrumented: `scraper.RunStats` accumulates per-run counters and
  `MediaWikiClient.stats` (`ClientStats`) tracks HTTP totals; `run()` logs
  phase banners, per-page `[n/total pct% ETA]` progress, and an end-of-run
  summary (`_log_summary`). When adding work to the pipeline, update the
  relevant counters so the summary stays accurate.
