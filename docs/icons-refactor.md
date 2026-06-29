# Icons refactor — renditions, m2m links, WikiArt source

Status: **implemented.** Schema, storage (both backends), config, ratelimit,
main, sources, gate, and pipeline are done; verified by `py_compile`, the
existing `test_saint_claims.py`, and a live SQLite exercise of the new storage
paths + ratelimit + recrawl-expiry. The WikiArt gate is a fail-closed stub and
its API response shape is unverified (no key on hand) — see the open seam.
This refactor restructures the icon layer around a clean rendition
model, replaces the single `icons.saint_id` with proper many-to-many links,
adds tags (on both icons *and* saints), introduces a configurable mandatory
recrawl, and adds **WikiArt** as a fourth source. The icon layer is
`enabled = false` by default and holds no production data, so the schema is
**dropped and recreated** — no migration.

## Decisions (the settled design)

| Concern | Resolution |
|---|---|
| **Identity / dedup** | `(source, uri)`, last-write-wins. `sha1` + `etag` are stored *attributes*, never merge keys. No cross-source or perceptual dedup — pixels are out of scope, so every distinct rendition is kept. |
| **`icons` table** | 1:1 with its source (a rendition has exactly one source by construction). Licensing lives on the row, not an edge. `UNIQUE (source_id, uri)`. |
| **saint ↔ icon** | Many-to-many via a bare junction `icon_saints`. Drops `icons.saint_id`. |
| **tags** | Shared `tags` vocabulary + `icon_tags` + `saint_tags` — tags apply to both entities ("tags between the saints/icons"). |
| **recrawl** | No recrawl by default. `last_crawled` column only; eligibility computed at runtime vs config `recrawl_after` (0 = never). `--force-recrawl` for on-demand. ETag conditional GET avoids re-downloading unchanged bytes. |
| **WikiArt gate** | Stubbed **fail-closed** (`wikiart_license_unverified`) — no API access yet to learn its license/date fields. Crawl-all, serve-approved-only (existing invariant). |
| **rate limit** | Per-source `RateLimiter` (so WikiArt's budget doesn't throttle Met/Wikimedia), multi-bucket: `4/s` **and** `400/hr` both satisfied. Single reused session (the "10 sessions/hr" cap needs no work). |
| **API key** | `api_key = ${?WIKIART_API_KEY}` via HOCON env substitution — no secret in the repo. |
| **events** | Fan out `new_icon_added` over **all** linked saints, via `asyncio.create_task` after the icon commits; refs held and `gather`ed before `run()` returns. |

### Why no image dedup

Exact-hash (sha1) over-splits — every re-encode is a new hash, so it dedups
almost nothing across sources. Perceptual hashing is the only thing that could
say "different files, same image," and it's out of scope (and risks eating
genuine renditions). So there is no reliable cross-source image-identity signal,
and we stop treating dedup as a catalog problem. What remains:

- `(source, uri)` uniqueness → never re-ingest the same record (this *is* the
  "dedup").
- sha1 content-addressing on disk → identical bytes already share one file
  (free storage win, never merges catalog rows).

### Identity is two-phase

`sha1` only exists *after* download, so the `(source, uri)` key is checked in two
steps:

1. **Pre-fetch (cheap):** `(source, uri)` known and within `recrawl_after` →
   skip download + re-gate entirely. This is what saves WikiArt's request quota.
2. **Post-download:** compute `sha1`; if unchanged, bump `last_crawled`; if the
   same URI now serves different bytes, last-write-wins (overwrite in place — no
   per-URL byte history).

ETag conditional GET sits between: on a recrawl we *do* perform, `If-None-Match`
→ 304 skips the body. It saves *bandwidth*, not request quota (a 304 still costs
one request); quota is saved by the pre-fetch skip.

## Schema (`schema.postgres.sql` / `schema.sqlite.sql`)

Drop & recreate the icon layer. Both backends in lockstep.

```sql
icons        id, source_id → sources, uri, source_record_id, sha1, etag,
             title, description, license, attribution,
             crawl_status, quarantine_reason, local_path,
             last_crawled, created_at, updated_at
             UNIQUE (source_id, uri)
-- removed: saint_id, image_source_id, image_license, attribution_text,
--          image_url, description_source_id, veneration_date

tags         id, name UNIQUE

icon_saints  icon_id → icons, saint_id → saints   PK (icon_id, saint_id)
icon_tags    icon_id → icons, tag_id  → tags      PK (icon_id, tag_id)
saint_tags   saint_id → saints, tag_id → tags     PK (saint_id, tag_id)
```

Indexes: `icons(crawl_status)`, `icons(last_crawled)`, plus reverse-lookup
indexes on the junction tables' second column. Junctions are scalar FKs — no
JSON array columns, so the SQLite mirror is straightforward.

## Code changes

### `ratelimit.py`
`RateLimiter.__init__(buckets: list[TokenBucket], max_concurrency)` — acquire
from **all** buckets in `__aenter__` (satisfy-all-limits). Keep a single-bucket
convenience path so the wiki scraper's call sites are untouched.

### `config.py`
- `IconSourceConfig`: add `api_key: str = ""`, WikiArt crawl knobs, per-source
  `rate_limit: RateLimitConfig`, `hourly_cap: int = 0` (0 = off), `http`.
- `IconsConfig`: add `recrawl_after: timedelta = 0` (0 = never auto-recrawl).
- `_load_icon_source`: read `api_key` (HOCON resolves `${?WIKIART_API_KEY}`
  itself), per-source `rate_limit`, `hourly_cap`.

### `main.py`
- Build **one `RateLimiter` per source** (multi-bucket when `hourly_cap > 0`);
  pass a `{name: limiter}` map into the pipeline instead of a single
  `with_limiter`.
- Add `--force-recrawl` (optionally source-scoped) → pipeline.

### `icon_sources.py`
- `RawRecord`: add required `uri` (stable per adapter; iconsaint uses its
  relative dataset path — "even a path on disk is a URI").
- `WikiArtAdapter`: authenticate once (session reused), `api_key` from cfg, its
  own `_HttpJson` on the per-source limiter; empty key → warn + skip (mirrors
  iconsaint's empty `dataset_path`). Crawls all records.
- `WikipediaCategoryAdapter` (`wikipedia`): walks an enwiki article category
  (recursing subcats to `subcat_depth`), takes each article's **lead image**
  (`pageimages`) and that file's Commons license (`imageinfo` extmetadata). The
  article title is the saint hint. Images are Commons-hosted, so the gate routes
  `wikipedia` → the existing `_check_wikimedia` (same `allowed_licenses` tags);
  fair-use locals quarantine, fail-closed. Config: `category`, `subcat_depth`.
- `build_adapters`: register `wikipedia` + `wikiart`; thread per-source
  http/limiter through.

### `license_gate.py`
`_check_wikiart` → `QUARANTINED, "wikiart_license_unverified"`; add to
`evaluate` dispatch. Seam comment marking where the PD/date signal goes once the
API is known (shape mirrors `_check_met`'s `is_public_domain`).

### `icon_pipeline.py`
- **Recrawl gate:** before download, look up `(source, uri)`; skip if present and
  `now < last_crawled + recrawl_after`, unless `--force-recrawl`.
- **Conditional GET:** send `If-None-Match` from stored `etag`; 304 → bump
  `last_crawled`; 200 → download → sha1 → last-write-wins; no validator →
  download.
- **m2m:** store the auto-resolved saint (0 or 1) into `icon_saints`; gate
  verdict + license/attribution now live on the icon row.
- **Fan-out:** after `store_icon` commits and `newly_approved`,
  `asyncio.create_task(self._fan_out_events(icon_id))`; hold refs in a set,
  `gather(..., return_exceptions=True)` before `run()` returns. **Not an OS
  thread** — the DB driver is async; spawn strictly *after* the commit so the
  task reads durable `icon_saints` rows.
- Use the per-source limiter for downloads.

### `storage.py` + `storage_postgres.py` + `storage_sqlite.py` (lockstep)
- `IconRow` restructure: `source_id, uri, sha1, etag, license, attribution, …`;
  no `saint_id`.
- `store_icon`: upsert on `(source_id, uri)`, last-write-wins, returns
  `icon_id` + `newly_approved`.
- New methods: `get_icon_recrawl_state(source_id, uri) -> {last_crawled, etag}`;
  `link_icon_saint(icon_id, saint_id)`; `add_tags` / `link_icon_tags` /
  `link_saint_tags`; `linked_saints(icon_id)` for fan-out.
- `_fan_out_events` uses `linked_saints` + existing `count_followers` /
  `record_event` per saint (per-user/day dedup already prevents double pings).

### `scraper.conf` + `README.md`
WikiArt source block (`api_key = ${?WIKIART_API_KEY}`,
`rate_limit { requests_per_second = 4 }`, `hourly_cap = 400`),
`icons.recrawl_after`, the `--force-recrawl` flag, and a tags/m2m note in the
licensing section.

## Open seam

**WikiArt gate is a stub.** Once API access exists, check whether a work returns
artist death-year or a PD/license flag. If yes → add a positive auto-approve
path in `_check_wikiart` (the genuinely-free slice clears, the rest stays
quarantined). If no → WikiArt is a quarantine pile promoted only via
`license_overrides` / per-source policy. Either way the adapter already
crawls-all and the read side already serves only `approved`.

## Sequencing & verification

1. Schema + storage + config (foundation).
2. ratelimit + main (wiring).
3. sources + gate + pipeline (behavior).

~10 files. Verify per CLAUDE.md: `py_compile *.py` + a throwaway SQLite crawl
with a capped adapter and a real contact `User-Agent`.
