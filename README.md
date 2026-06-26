# OrthodoxWiki Scraper

An async Python 3 scraper that walks a configurable list of
[OrthodoxWiki](https://orthodoxwiki.org) categories and stores page content in
either **PostgreSQL** or **SQLite** (selectable in the config). It talks to the
MediaWiki API (not rendered HTML), rate-limits itself, and only re-crawls a page
once a configurable amount of time has passed since it was last fetched.

## How it works

1. **Discover** — for each configured category, enumerate its members via the
   MediaWiki `categorymembers` API, optionally descending into subcategories up
   to a configurable depth.
2. **Mark seen** — every discovered page's `last_seen` timestamp is bumped (and
   new pages are inserted) so you always know when a page last appeared in a
   category.
3. **Crawl only what changed** — a cheap metadata probe (`prop=info`, no content)
   gets each previously-seen page's current `lastrevid`. A page's full wikitext is
   re-fetched **only if it's new or its revision id changed** since we stored it;
   unchanged pages are skipped without downloading their content. `recrawl_after`
   is an optional fallback that also refreshes unchanged pages once they're that
   old (set it to `0` to rely purely on change detection). Content is fetched in
   batches of 50 and upserted. Each crawled page also gets its **author list and
   a ready-to-use attribution string** (see
   [Licensing & attribution](#licensing--attribution)).

4. **Download media** — when a page is crawled, its media (images, audio,
   video, documents) is fetched according to the per-category media policy. Only
   the media classes you opted into for that page's categories are downloaded.
   Files are saved to disk content-addressed by sha1 (so a file shared across
   pages is fetched once), and the page row records just their local paths. See
   [Media](#media-downloads) below.
5. **Reconcile deletions** — after crawling, any page we have on file but did
   *not* observe in this run's category listings is a "disappeared" candidate.
   We run a cheap existence probe against the API and **soft-remove** (stamp
   `removed_at`) only the ones the wiki confirms are gone. Pages that still
   exist but merely left your categories are kept. If a removed page later
   reappears in a category, it is automatically un-removed. Toggle with
   `scraper.reconcile_deletions` (default `true`).

Concurrency is bounded by an async token-bucket rate limiter
(`requests_per_second` + `burst`) plus a `max_concurrency` semaphore, so it
stays polite regardless of how many batches run in parallel.

## Setup

```sh
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
```

Edit [`scraper.conf`](scraper.conf) — pick your storage backend, set the
categories you want, the recrawl window, and rate limits.

### Choosing a backend

Set `database.backend` to `"postgres"` or `"sqlite"`.

- **SQLite** (zero setup): just set `database.path`. The file and schema are
  created automatically on first run — nothing else to install.
- **PostgreSQL**: set `host`/`port`/`name`/`user`/`password`. The schema is
  applied automatically on startup, but you can also load it manually:

  ```sh
  createdb orthodoxwiki
  psql orthodoxwiki -f schema.postgres.sql
  ```

## Usage

```sh
# Run a single pass and exit (ideal for cron):
./.venv/bin/python main.py --config scraper.conf

# Run continuously, sleeping between passes:
./.venv/bin/python main.py --config scraper.conf --loop 6h

# Debug logging:
./.venv/bin/python main.py --config scraper.conf -v
```

`--mode` selects what to run (default `wiki`):

```sh
# OrthodoxWiki category scraper (default):
./.venv/bin/python main.py --config scraper.conf --mode wiki

# Licensed Icon & Saints ingestion (Met / Wikimedia / ICONSAINT):
./.venv/bin/python main.py --config scraper.conf --mode icons

# Saint roster from Wikipedia list articles (names only, no images/licensing):
./.venv/bin/python main.py --config scraper.conf --mode saints

# Daily follower-notification job (feast/nameday/veneration/new-icon):
./.venv/bin/python main.py --config scraper.conf --mode notify
```

All modes share the config file and the database, and each works with
`--loop` (e.g. `--mode notify --loop 24h`). See
[Icon & Saints data layer](#icon--saints-data-layer).

## Configuration

All configuration lives in a HOCON `.conf` file. See
[`scraper.conf`](scraper.conf) for the annotated defaults. Key settings:

| Key | Meaning |
| --- | --- |
| `scraper.categories` | Category names to crawl (no `Category:` prefix). |
| `scraper.recurse_subcategories` / `max_subcategory_depth` | Whether and how deep to descend into subcategories. |
| `scraper.recrawl_after` | HOCON duration (`7 days`, `12 hours`, or `0` to disable). Fallback that forces a refresh of *unchanged* pages once this old; changed pages are always re-crawled via the lastrevid probe regardless. |
| `scraper.reconcile_deletions` | After each run, soft-remove pages the wiki confirms are deleted. |
| `scraper.media.*` | Per-category media downloading — see [Media](#media-downloads). |
| `scraper.attribution.*` | Licensing/credit metadata — see [Licensing & attribution](#licensing--attribution). |
| `scraper.rate_limit.*` | Sustained req/s, burst allowance, and max concurrent requests. |
| `scraper.http.*` | Per-request timeout, retry count, and backoff base. |
| `database.backend` | `"postgres"` or `"sqlite"`. |
| `database.path` | **DB storage path** — SQLite database file (SQLite backend). |
| `database.host`/`port`/`name`/`user`/`password`/`pool_*` | PostgreSQL connection and pool sizing. |
| `scraper.media.download_dir` | **File storage path** — directory for downloaded media. |

**Storage paths.** The DB file path (`database.path`, SQLite backend) and the
media file directory (`scraper.media.download_dir`) both support `~` and
`$ENV_VAR` expansion and may be relative or absolute (e.g. `~/ortho/data.db`,
`/srv/ortho/media`). Parent directories are created automatically on first run.

## Schema

See [`schema.postgres.sql`](schema.postgres.sql) and
[`schema.sqlite.sql`](schema.sqlite.sql). The `pages` table is keyed on the
MediaWiki `pageid` and tracks `first_seen`, `last_seen`, and `last_crawled`
separately so re-crawl decisions and "still in category" observations are
independent. (`categories` reflects a page's full API category membership,
written when its content is crawled.) `removed_at` is `NULL` for active pages
and set when the wiki confirms a page was deleted; active rows are simply those
`WHERE removed_at IS NULL`. `media_paths` lists the on-disk paths of media
downloaded for the page — the files themselves are never stored in the DB.
`contributors` and `attribution` hold the page's authors and a ready-to-use
credit line (see [Licensing & attribution](#licensing--attribution)).

The `media` table has one row per downloaded file (the bytes stay on disk),
including a `redistribution` enum column (`public`/`free`/`restricted`/`prohibited`)
so you can query files by how freely they may be shared.

## Licensing & attribution

OrthodoxWiki text is **dual-licensed GFDL + Creative Commons Attribution-ShareAlike
2.5** ([OrthodoxWiki:Copyrights](https://orthodoxwiki.org/OrthodoxWiki:Copyrights)).
BY-SA permits redistribution and derivatives **provided you attribute the authors
and share derivatives under the same license**. The scraper captures the data you
need to do that:

**For each page** (stored in the DB):

- `contributors` — the full list of page authors, from the MediaWiki
  `contributors` API (configurable via `scraper.attribution.fetch_contributors`).
- `attribution` — a ready-to-use credit line combining title, authors, source
  URL, the exact revision permalink, the history-page link, and the license,
  e.g.:

  > "Abbot" by Magda, Pistevo, Dcndavid, … . Source: https://orthodoxwiki.org/Abbot
  > (revision 121805, permalink: …?oldid=121805; full history: …&action=history).
  > OrthodoxWiki, licensed under CC BY-SA 2.5 <…> and GFDL.

**For each downloaded media file**, the `media` table records a `redistribution`
level (`public`/`free`/`restricted`/`prohibited`) plus `license_name`, and a
`<file>.json` **sidecar** is written next to the file with the full detail.
⚠️ **Image licenses frequently differ from the
article text** — so each sidecar records the data to determine the file's own
license: `title`, `source_url`, `description_page` (the File: page, which states
the license), `uploader`, `sha1`, `mime`, `description_wikitext` (the File: page
wikitext), and a parsed **`licenses`** list.

The `licenses` list is produced by [`licenses.py`](licenses.py), which maps
OrthodoxWiki's license templates (`{{cc by-sa}}`, `{{gfdl}}`, `{{pd}}`,
`{{fairuse}}`, `{{cc by-nc-nd}}`, `{{oca}}`, `{{damickcopy}}`, …) to a normalized
`{name, url, free, note}`. The **`free`** flag means "freely redistributable,
including derivatives"; non-commercial, no-derivatives, fair-use, permission-only,
and unverified tags are `free: false` — **review those before redistributing**.
If `licenses` is empty, no known tag was recognized: fall back to
`description_wikitext` / `description_page`.

License strings, site name, and the image-license note are set under
`scraper.attribution` in the config, so you can adjust them if you point the
scraper at a different wiki.

## Media downloads

You choose **what media to download for which categories**. The `scraper.media`
block has a global `default` policy plus per-category overrides:

```hocon
media {
  enabled = true                 # master switch
  download_dir = "media"         # files stored content-addressed by sha1
  max_file_size = 25 MB          # skip larger files (metadata still recorded); 0 = no limit

  default { download = false, types = [] }

  categories_policy {
    Saints      { download = true, types = [image] }
    Feasts      { download = true, types = [image, audio] }
    Monasteries { download = true, types = [image] }
  }
}
```

- **`types`** is any of `image`, `audio`, `video`, `document`, `other`, or
  `all`. Each file is classified by its MIME type (with an extension fallback).
- A page reached by **recursing into subcategories** inherits the policy of the
  configured root category it descends from. A page found under **several**
  categories gets the **union** of their allowed media types.
- Media is (re)fetched when a page's *content* is crawled — i.e. when the page
  is new or its revision changed (or on a `recrawl_after` forced refresh). So a
  changed media policy takes effect for a page on its next content crawl; to pull
  in re-uploaded images on otherwise-unchanged pages, rely on `recrawl_after`.
- Files are saved at `<download_dir>/<sha1[:2]>/<sha1><ext>` and **deduplicated**
  by sha1: a file shared across many pages is downloaded only once. Downloads go
  through the same rate limiter as everything else.
- **The database never stores the files themselves**, only references. Each
  page's `pages.media_paths` column lists the paths to its downloaded media, and
  a `media` table holds one row per file (`media_id` = sha1, `local_path`,
  `mime`, `source_url`, `license_name`, and the **`redistribution`** enum). The
  bytes live on disk under `download_dir`. A file skipped for exceeding
  `max_file_size` (or that fails to download) simply doesn't appear.
- **`redistribution`** classifies how freely each file may be shared, derived
  from its license tags: `public` (PD) → `free` (GFDL, CC BY-SA) → `restricted`
  (NC/ND, permission-only) → `prohibited` (fair-use, unverified, or unrecognized).
  Most permissive tag wins; unknown defaults to `prohibited`. It's a real
  queryable column, e.g. `SELECT * FROM media WHERE redistribution = 'prohibited'`.

## Icon & Saints data layer

A second, licensing-first pipeline (independent of the wiki scraper) that builds
a consumer-facing data layer of **saints and icons** from sources that grant
reuse — so the product can let users search, save, and *follow* a saint or a
specific icon and be notified of feast days, veneration days, and newly added
icons. It lives alongside the wiki scraper in the same database and is **disabled
by default**.

**Hard rule: nothing is served unless its license is verified.** Every record
passes a **license gate** ([`license_gate.py`](license_gate.py)) at ingestion
time that marks it `approved`, `quarantined`, or `rejected`. Only `approved`
icons get their image downloaded, and an app must query
`WHERE crawl_status = 'approved'`. The gate **fails closed** — anything it can't
positively classify is quarantined (kept for audit, never surfaced).

### Sources

| Source | License handling |
| --- | --- |
| **Met Open Access** (`met_api`) | Public-domain **per object** — `isPublicDomain` is checked on every record; non-PD → quarantined. |
| **Wikimedia Commons** (`wikimedia`) | Per-file license tag (from `extmetadata`) checked against a configured `allowed_licenses` allowlist. |
| **ICONSAINT** (`iconsaint`) | Blanket **CC BY** grant verified via the companion paper (DOI 10.3390/info17040340). Images-only; attribution is **required** per image. Read from a *local* checkout — point `icons.sources.iconsaint.dataset_path` at it **after** the 5-minute manual check that the repo's own LICENSE doesn't conflict with the paper's CC BY. |

Commercial monastery/store sites and (by default) archdiocese sites are excluded
as unlicensed; a per-record human override is available (see below).

### How it works

1. **Source rows** are seeded from config into the `sources` table. Editing a
   source's `base_license` invalidates its cached approvals — affected icons are
   re-flagged `pending_license_check` on the next `--mode icons` run.
2. **Adapters** ([`icon_sources.py`](icon_sources.py)) pull raw records and
   surface only the license signal the gate needs; all HTTP is rate-limited.
3. **The gate** classifies each record. A human row in `license_overrides`
   (auditable, kept separate from automated decisions) wins over the gate.
4. **Approved** icons get their image fetched into content-addressed storage
   (`icons.download_dir`, deduped by sha1, with a `.json` attribution sidecar) —
   never hotlinked. The local path is stored in `icons.image_url`.
5. When a new icon is approved for a saint that **already has followers**, the
   pipeline writes a one-off `new_icon_added` event.

### Notifications

`--mode notify` runs a daily job ([`notifications.py`](notifications.py)),
decoupled from crawl cadence. It matches recurring events (feast/nameday/
veneration) on `MM-DD` and one-off `new_icon_added` events on today's date, then
notifies each follower exactly once per day (re-running the job is safe).
Delivery is pluggable (a `dispatch` callable; the default logs) — push/email
infra is out of scope. Feast/veneration **dates are mostly NULL at launch** (no
verified-licensed text source yet); as they get populated, `sync_recurring_events`
materializes the events automatically.

> **Text vs. images are licensed independently.** An approved image does **not**
> approve a bio: `saints.bio_text` is withheld while `saints.bio_license` is NULL.

### Config

See the `icons { … }` block in [`scraper.conf`](scraper.conf) for fully
annotated defaults. Enable the layer with `icons.enabled = true` and enable at
least one source under `icons.sources`. Each source carries its
`base_license`, `requires_per_item_check`, `allowed_licenses` (Wikimedia),
`attribution_template`, and per-adapter crawl scope (`queries` / `max_objects`,
`search_terms` / `max_files`, or `dataset_path` / `manifest`). The layer has its
own `rate_limit` and `http` blocks (separate budget — different hosts).

## Saint roster (`--mode saints`)

A separate, much simpler job from the icon pipeline: it parses one or more
Wikipedia **list articles** and upserts the linked saint names into the `saints`
table. No images, no licensing gate — English Wikipedia text is CC BY-SA/GFDL as
a blanket fact about the source, so there is nothing per-record to verify. A
missing or renamed article is logged and skipped, never fatal.

Configure it in the `saints { … }` block of [`scraper.conf`](scraper.conf):
`enabled`, `wikipedia_articles` (article **titles**, not URLs — e.g.
`"List of Eastern Orthodox saints"` for
<https://en.wikipedia.org/wiki/List_of_Eastern_Orthodox_saints>), and
`max_records`. Run with `--mode saints` (combinable with `--loop`).

## Files

| File | Purpose |
| --- | --- |
| [`main.py`](main.py) | CLI entry point + run/loop orchestration; `--mode wiki\|icons\|saints\|notify`. |
| [`config.py`](config.py) | HOCON loading + duration parsing into typed dataclasses. |
| [`mediawiki.py`](mediawiki.py) | Async MediaWiki API client (category enumeration, content fetch). |
| [`ratelimit.py`](ratelimit.py) | Token-bucket + concurrency limiter. |
| [`licenses.py`](licenses.py) | Maps OrthodoxWiki license templates to normalized license info. |
| [`storage.py`](storage.py) | Backend-agnostic storage interface + factory. |
| [`storage_postgres.py`](storage_postgres.py) | PostgreSQL backend (asyncpg). |
| [`storage_sqlite.py`](storage_sqlite.py) | SQLite backend (aiosqlite). |
| [`scraper.py`](scraper.py) | Discovery + crawl orchestration (wiki). |
| [`license_gate.py`](license_gate.py) | Per-record, fail-closed license verification gate. |
| [`icon_sources.py`](icon_sources.py) | Met / Wikimedia / ICONSAINT source adapters. |
| [`icon_pipeline.py`](icon_pipeline.py) | Icon ingestion: gate → normalize → store → emit events. |
| [`saint_sources.py`](saint_sources.py) | Saint roster: parse Wikipedia list articles into saint names. |
| [`notifications.py`](notifications.py) | Daily follower-notification job. |
