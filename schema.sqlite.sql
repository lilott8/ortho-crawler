-- SQLite schema for the OrthodoxWiki scraper.
-- Applied automatically on startup, but provided here for reference.
--
-- Notes vs. the PostgreSQL schema:
--   * SQLite has no array type, so `categories` is a JSON array stored as TEXT.
--   * Timestamps are ISO-8601 UTC strings (e.g. "2024-01-02T03:04:05+00:00").

CREATE TABLE IF NOT EXISTS pages (
    pageid          INTEGER  PRIMARY KEY,          -- MediaWiki page id
    title           TEXT     NOT NULL,
    url             TEXT     NOT NULL,
    namespace       INTEGER  NOT NULL DEFAULT 0,
    revid           INTEGER,                        -- latest revision id we stored
    content         TEXT,                           -- wikitext of the page
    content_length  INTEGER,
    categories      TEXT     NOT NULL DEFAULT '[]', -- JSON array of categories
    page_touched    TEXT,                           -- MediaWiki "touched" timestamp
    first_seen      TEXT     NOT NULL,
    last_seen       TEXT     NOT NULL,              -- last time we observed it in a category
    last_crawled    TEXT,                           -- last time we fetched its content
    removed_at      TEXT,                           -- set when the wiki confirms the page is gone (NULL = active)
    media_paths     TEXT     NOT NULL DEFAULT '[]', -- JSON array of local paths of media downloaded for this page
    contributors    TEXT     NOT NULL DEFAULT '[]', -- JSON array of page authors, for attribution
    attribution     TEXT,                           -- ready-to-use credit line (CC BY-SA / GFDL)
    UNIQUE (title)
);

CREATE INDEX IF NOT EXISTS pages_last_crawled_idx ON pages (last_crawled);
-- Speeds up the per-run "what did we not see this time" reconciliation query.
CREATE INDEX IF NOT EXISTS pages_active_idx ON pages (last_seen) WHERE removed_at IS NULL;

-- Downloaded media files live on disk (content-addressed by sha1 under the
-- configured download_dir); the DB never stores the bytes. pages.media_paths
-- lists each page's files, and this table records one row per file with its
-- redistribution level (derived from the file's license tags).
CREATE TABLE IF NOT EXISTS media (
    media_id        TEXT     PRIMARY KEY,          -- sha1 (or url-hash fallback)
    title           TEXT,                           -- e.g. "File:Saint_Nicholas.jpg"
    local_path      TEXT,
    mime            TEXT,
    source_url      TEXT,
    license_name    TEXT,                           -- most permissive recognized license
    redistribution  TEXT     NOT NULL DEFAULT 'prohibited'
        CHECK (redistribution IN ('public', 'free', 'restricted', 'prohibited')),
    first_seen      TEXT     NOT NULL,
    last_seen       TEXT     NOT NULL
);
CREATE INDEX IF NOT EXISTS media_redistribution_idx ON media (redistribution);


-- ============================================================================
-- Icon & Saints data layer (PRD: "Orthodox Icon & Saints Data Layer").
-- Mirror of the PostgreSQL tables; SQLite uses INTEGER autoincrement keys,
-- TEXT for JSON arrays, and ISO-8601 / CURRENT_TIMESTAMP strings for times.
--
-- HARD CONSTRAINT: nothing is served unless crawl_status = 'approved' (set
-- only by the license gate). Quarantined/rejected icons are retained for audit.
-- ============================================================================

CREATE TABLE IF NOT EXISTS sources (
    id                      INTEGER  PRIMARY KEY AUTOINCREMENT,
    name                    TEXT     NOT NULL UNIQUE,
    base_license            TEXT     NOT NULL,
    attribution_template    TEXT,
    requires_per_item_check INTEGER  NOT NULL DEFAULT 1,   -- boolean (0/1)
    last_verified_at        TEXT,
    notes                   TEXT,
    created_at              TEXT     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TEXT     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS saints (
    id              INTEGER  PRIMARY KEY AUTOINCREMENT,
    canonical_name  TEXT     NOT NULL UNIQUE,
    alt_names       TEXT,                                  -- JSON array, for search
    feast_day       TEXT,                                  -- MM-DD, nullable
    bio_text        TEXT,
    bio_source_id   INTEGER  REFERENCES sources(id),
    bio_license     TEXT,                                  -- NULL = do not serve bio_text
    created_at      TEXT     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS icons (
    id                    INTEGER  PRIMARY KEY AUTOINCREMENT,
    saint_id              INTEGER  REFERENCES saints(id),
    title                 TEXT     NOT NULL,
    image_url             TEXT,                            -- local/CDN copy, never hotlinked
    image_source_id       INTEGER  NOT NULL REFERENCES sources(id),
    image_license         TEXT     NOT NULL,
    attribution_text      TEXT     NOT NULL,
    description           TEXT,
    description_source_id INTEGER  REFERENCES sources(id),
    veneration_date       TEXT,                            -- MM-DD, nullable
    source_record_id      TEXT,
    crawl_status          TEXT     NOT NULL DEFAULT 'pending_license_check'
        CHECK (crawl_status IN ('pending_license_check','approved','quarantined','rejected')),
    quarantine_reason     TEXT,
    local_path            TEXT,
    created_at            TEXT     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            TEXT     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (image_source_id, source_record_id)
);
CREATE INDEX IF NOT EXISTS icons_status_idx ON icons (crawl_status);
CREATE INDEX IF NOT EXISTS icons_saint_idx  ON icons (saint_id);

CREATE TABLE IF NOT EXISTS users (
    id          INTEGER  PRIMARY KEY AUTOINCREMENT,
    created_at  TEXT     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS favorites (
    user_id     INTEGER  NOT NULL REFERENCES users(id),
    icon_id     INTEGER  NOT NULL REFERENCES icons(id),
    created_at  TEXT     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, icon_id)
);

CREATE TABLE IF NOT EXISTS follows (
    user_id     INTEGER  NOT NULL REFERENCES users(id),
    target_type TEXT     NOT NULL CHECK (target_type IN ('saint','icon')),
    target_id   INTEGER  NOT NULL,
    created_at  TEXT     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, target_type, target_id)
);
CREATE INDEX IF NOT EXISTS follows_target_idx ON follows (target_type, target_id);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER  PRIMARY KEY AUTOINCREMENT,
    target_type TEXT     NOT NULL CHECK (target_type IN ('saint','icon')),
    target_id   INTEGER  NOT NULL,
    event_type  TEXT     NOT NULL
        CHECK (event_type IN ('feast_day','nameday','veneration_day','new_icon_added')),
    event_date  TEXT,                                      -- MM-DD recurring, full date one-off
    created_at  TEXT     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (target_type, target_id, event_type, event_date)
);
CREATE INDEX IF NOT EXISTS events_date_idx ON events (event_type, event_date);

CREATE TABLE IF NOT EXISTS license_overrides (
    id               INTEGER  PRIMARY KEY AUTOINCREMENT,
    source_name      TEXT     NOT NULL,
    source_record_id TEXT     NOT NULL,
    decision         TEXT     NOT NULL CHECK (decision IN ('approved','rejected')),
    license          TEXT,
    attribution      TEXT,
    reviewer         TEXT,
    reason           TEXT,
    created_at       TEXT     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_name, source_record_id)
);

CREATE TABLE IF NOT EXISTS notifications_sent (
    id          INTEGER  PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER  NOT NULL REFERENCES users(id),
    event_id    INTEGER  NOT NULL REFERENCES events(id),
    sent_at     TEXT     NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS notifications_user_event_idx ON notifications_sent (user_id, event_id);
