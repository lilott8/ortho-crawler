"""SQLite storage backend (aiosqlite).

SQLite has no array type, so ``categories`` is stored as a JSON array, and
timestamps are kept as ISO-8601 UTC strings.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Iterable, List

import aiosqlite

from config import DatabaseConfig
from mediawiki import PageContent
from storage import (Storage, IconRow, IconStoreResult, SourceState,
                     parse_ts, _schema_path, reduce_claims,
                     materialized_saint_columns, managed_saint_columns)

log = logging.getLogger("ortho_scraper.storage.sqlite")


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _row_to_dict(row) -> dict:
    return {k: row[k] for k in row.keys()}


class SqliteStorage(Storage):
    def __init__(self, conn: aiosqlite.Connection):
        self._conn = conn
        # SQLite serializes writers; a lock keeps concurrent upserts orderly.
        self._lock = asyncio.Lock()

    @classmethod
    async def connect(cls, cfg: DatabaseConfig) -> "SqliteStorage":
        # Create the parent directory so a configured path like "~/ortho/data.db"
        # works even on first run.
        parent = os.path.dirname(cfg.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        conn = await aiosqlite.connect(cfg.path)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA foreign_keys=ON")
        log.info("Connected to SQLite database %r.", cfg.path)
        return cls(conn)

    async def close(self) -> None:
        await self._conn.close()

    async def apply_schema(self) -> None:
        with open(_schema_path("schema.sqlite.sql"), "r", encoding="utf-8") as fh:
            ddl = fh.read()
        async with self._lock:
            # Migrate pre-removed_at databases first: the schema's partial index
            # references removed_at, so the column must exist before it runs.
            async with self._conn.execute("PRAGMA table_info(pages)") as cur:
                cols = {row[1] for row in await cur.fetchall()}
            if cols and "removed_at" not in cols:
                await self._conn.execute("ALTER TABLE pages ADD COLUMN removed_at TEXT")
            if cols and "media_paths" not in cols:
                await self._conn.execute(
                    "ALTER TABLE pages ADD COLUMN media_paths TEXT NOT NULL DEFAULT '[]'")
            if cols and "contributors" not in cols:
                await self._conn.execute(
                    "ALTER TABLE pages ADD COLUMN contributors TEXT NOT NULL DEFAULT '[]'")
            if cols and "attribution" not in cols:
                await self._conn.execute("ALTER TABLE pages ADD COLUMN attribution TEXT")
            # saints.qid must exist before the schema's saints_qid_idx runs.
            async with self._conn.execute("PRAGMA table_info(saints)") as cur:
                saint_cols = {row[1] for row in await cur.fetchall()}
            if saint_cols and "qid" not in saint_cols:
                await self._conn.execute("ALTER TABLE saints ADD COLUMN qid TEXT")
            if saint_cols and "description" not in saint_cols:
                await self._conn.execute("ALTER TABLE saints ADD COLUMN description TEXT")
            # saint_claims gained `value` in its UNIQUE key. A pre-existing ledger
            # with the old 3-column key must be rebuilt — it's a derived ledger,
            # so re-ingest repopulates it.
            async with self._conn.execute("PRAGMA table_info(saint_claims)") as cur:
                sc_exists = bool(await cur.fetchall())
            if sc_exists and not await self._unique_covers_value("saint_claims"):
                log.warning("Rebuilding saint_claims for multi-valued UNIQUE key "
                            "(derived ledger; re-ingest repopulates).")
                await self._conn.execute("DROP TABLE saint_claims")
            await self._conn.executescript(ddl)
            await self._conn.commit()
        log.info("Schema ensured.")

    async def get_crawl_state(self, pageids: Iterable[int]) -> Dict[int, dict]:
        ids = list(pageids)
        if not ids:
            return {}
        placeholders = ",".join("?" for _ in ids)
        async with self._conn.execute(
            f"SELECT pageid, revid, last_crawled FROM pages WHERE pageid IN ({placeholders})",
            ids,
        ) as cur:
            rows = await cur.fetchall()
        return {r["pageid"]: {"revid": r["revid"], "last_crawled": parse_ts(r["last_crawled"])}
                for r in rows}

    async def mark_seen(self, members: List[dict]) -> None:
        if not members:
            return
        now = _iso(datetime.now(timezone.utc))
        rows = [
            (m["pageid"], m["title"], m["url"], m["namespace"],
             json.dumps(m["categories"]), now, now)
            for m in members
        ]
        async with self._lock:
            await self._conn.executemany(
                """
                INSERT INTO pages (pageid, title, url, namespace, categories,
                                   first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (pageid) DO UPDATE SET
                    title      = excluded.title,
                    url        = excluded.url,
                    namespace  = excluded.namespace,
                    last_seen  = excluded.last_seen,
                    -- Seeing a page again resurrects a previously-removed row.
                    removed_at = NULL
                    -- categories is left to store_page (authoritative API
                    -- membership); here it's only a placeholder on insert.
                """,
                rows,
            )
            await self._conn.commit()

    async def store_page(self, page: PageContent, categories: List[str],
                         media_paths: List[str]) -> None:
        now = _iso(datetime.now(timezone.utc))
        touched = parse_ts(page.touched)
        touched_iso = _iso(touched) if touched else None
        content_length = len(page.content) if page.content is not None else None
        async with self._lock:
            await self._conn.execute(
                """
                INSERT INTO pages (pageid, title, url, namespace, revid, content,
                                   content_length, categories, page_touched,
                                   first_seen, last_seen, last_crawled, media_paths,
                                   contributors, attribution)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (pageid) DO UPDATE SET
                    title          = excluded.title,
                    url            = excluded.url,
                    namespace      = excluded.namespace,
                    revid          = excluded.revid,
                    content        = excluded.content,
                    content_length = excluded.content_length,
                    categories     = excluded.categories,
                    page_touched   = excluded.page_touched,
                    last_seen      = excluded.last_seen,
                    last_crawled   = excluded.last_crawled,
                    media_paths    = excluded.media_paths,
                    contributors   = excluded.contributors,
                    attribution    = excluded.attribution
                """,
                (page.pageid, page.title, page.url, page.ns, page.revid, page.content,
                 content_length, json.dumps(categories), touched_iso, now, now, now,
                 json.dumps(media_paths), json.dumps(page.contributors), page.attribution),
            )
            await self._conn.commit()

    async def get_unseen_active(self, run_started: datetime) -> List[int]:
        async with self._conn.execute(
            "SELECT pageid FROM pages WHERE removed_at IS NULL AND last_seen < ?",
            (_iso(run_started),),
        ) as cur:
            rows = await cur.fetchall()
        return [r["pageid"] for r in rows]

    async def mark_removed(self, pageids: List[int], when: datetime) -> None:
        if not pageids:
            return
        placeholders = ",".join("?" for _ in pageids)
        async with self._lock:
            await self._conn.execute(
                f"UPDATE pages SET removed_at = ? WHERE pageid IN ({placeholders})",
                [_iso(when), *pageids],
            )
            await self._conn.commit()

    async def store_media(self, record) -> None:
        now = _iso(datetime.now(timezone.utc))
        async with self._lock:
            await self._conn.execute(
                """
                INSERT INTO media (media_id, title, local_path, mime, source_url,
                                   license_name, redistribution, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (media_id) DO UPDATE SET
                    title          = excluded.title,
                    local_path     = excluded.local_path,
                    mime           = excluded.mime,
                    source_url     = excluded.source_url,
                    license_name   = excluded.license_name,
                    redistribution = excluded.redistribution,
                    last_seen      = excluded.last_seen
                """,
                (record.media_id, record.title, record.local_path, record.mime,
                 record.source_url, record.license_name, record.redistribution, now, now),
            )
            await self._conn.commit()

    # --- Icon & Saints data layer -------------------------------------------

    async def upsert_source(self, name, base_license, attribution_template,
                            requires_per_item_check, notes=None) -> SourceState:
        now = _iso(datetime.now(timezone.utc))
        async with self._lock:
            async with self._conn.execute(
                "SELECT id, base_license FROM sources WHERE name = ?", (name,),
            ) as cur:
                row = await cur.fetchone()
            changed = row is not None and row["base_license"] != base_license
            await self._conn.execute(
                """
                INSERT INTO sources (name, base_license, attribution_template,
                                     requires_per_item_check, notes, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (name) DO UPDATE SET
                    base_license            = excluded.base_license,
                    attribution_template    = excluded.attribution_template,
                    requires_per_item_check = excluded.requires_per_item_check,
                    notes                   = excluded.notes,
                    updated_at              = excluded.updated_at
                """,
                (name, base_license, attribution_template,
                 1 if requires_per_item_check else 0, notes, now, now),
            )
            async with self._conn.execute(
                "SELECT id FROM sources WHERE name = ?", (name,)) as cur:
                sid = (await cur.fetchone())["id"]
            await self._conn.commit()
        return SourceState(source_id=sid, base_license_changed=changed)

    async def reflag_icons_for_source(self, source_id: int) -> int:
        async with self._lock:
            cur = await self._conn.execute(
                """UPDATE icons SET crawl_status = 'pending_license_check',
                                    updated_at = ?
                   WHERE image_source_id = ? AND crawl_status = 'approved'""",
                (_iso(datetime.now(timezone.utc)), source_id),
            )
            n = cur.rowcount
            await self._conn.commit()
        return n

    async def upsert_saint(self, canonical_name, alt_names=None) -> int:
        alt = json.dumps(alt_names) if alt_names else None
        async with self._lock:
            await self._conn.execute(
                """INSERT INTO saints (canonical_name, alt_names) VALUES (?, ?)
                   ON CONFLICT (canonical_name) DO UPDATE SET
                       alt_names = COALESCE(excluded.alt_names, saints.alt_names)""",
                (canonical_name, alt),
            )
            async with self._conn.execute(
                "SELECT id FROM saints WHERE canonical_name = ?", (canonical_name,)) as cur:
                sid = (await cur.fetchone())["id"]
            await self._conn.commit()
        return sid

    async def get_license_override(self, source_name, source_record_id):
        async with self._conn.execute(
            """SELECT decision, license, attribution, reviewer, reason
               FROM license_overrides WHERE source_name = ? AND source_record_id = ?""",
            (source_name, source_record_id),
        ) as cur:
            row = await cur.fetchone()
        return _row_to_dict(row) if row else None

    async def store_icon(self, row: IconRow) -> IconStoreResult:
        now = _iso(datetime.now(timezone.utc))
        async with self._lock:
            async with self._conn.execute(
                """SELECT id, crawl_status FROM icons
                   WHERE image_source_id = ? AND source_record_id = ?""",
                (row.image_source_id, row.source_record_id),
            ) as cur:
                existing = await cur.fetchone()
            if existing is None:
                cur = await self._conn.execute(
                    """INSERT INTO icons (saint_id, title, image_url, image_source_id,
                                          image_license, attribution_text, description,
                                          veneration_date, source_record_id, crawl_status,
                                          quarantine_reason, local_path, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (row.saint_id, row.title, row.image_url, row.image_source_id,
                     row.image_license, row.attribution_text, row.description,
                     row.veneration_date, row.source_record_id, row.crawl_status,
                     row.quarantine_reason, row.local_path, now, now),
                )
                icon_id = cur.lastrowid
                newly_approved = row.crawl_status == "approved"
            else:
                icon_id = existing["id"]
                newly_approved = (row.crawl_status == "approved"
                                  and existing["crawl_status"] != "approved")
                await self._conn.execute(
                    """UPDATE icons SET saint_id = ?, title = ?, image_url = ?,
                            image_license = ?, attribution_text = ?, description = ?,
                            veneration_date = ?, crawl_status = ?, quarantine_reason = ?,
                            local_path = ?, updated_at = ?
                       WHERE id = ?""",
                    (row.saint_id, row.title, row.image_url, row.image_license,
                     row.attribution_text, row.description, row.veneration_date,
                     row.crawl_status, row.quarantine_reason, row.local_path, now, icon_id),
                )
            await self._conn.commit()
        return IconStoreResult(icon_id=icon_id, newly_approved=newly_approved)

    async def count_followers(self, target_type, target_id) -> int:
        async with self._conn.execute(
            "SELECT COUNT(*) AS n FROM follows WHERE target_type = ? AND target_id = ?",
            (target_type, target_id),
        ) as cur:
            return (await cur.fetchone())["n"]

    async def record_event(self, target_type, target_id, event_type, event_date):
        async with self._lock:
            cur = await self._conn.execute(
                """INSERT OR IGNORE INTO events (target_type, target_id, event_type, event_date)
                   VALUES (?, ?, ?, ?)""",
                (target_type, target_id, event_type, event_date),
            )
            inserted = cur.rowcount
            await self._conn.commit()
            if not inserted:
                return None
            return cur.lastrowid

    async def sync_recurring_events(self) -> int:
        async with self._lock:
            # feast_day is a JSON array of MM-DD: expand to one event per date.
            # json_valid guards any legacy scalar value (skipped, not fatal).
            c1 = await self._conn.execute(
                """INSERT OR IGNORE INTO events (target_type, target_id, event_type, event_date)
                   SELECT 'saint', s.id, 'feast_day', je.value
                   FROM saints s, json_each(s.feast_day) je
                   WHERE s.feast_day IS NOT NULL AND json_valid(s.feast_day)""")
            c2 = await self._conn.execute(
                """INSERT OR IGNORE INTO events (target_type, target_id, event_type, event_date)
                   SELECT 'icon', id, 'veneration_day', veneration_date FROM icons
                   WHERE veneration_date IS NOT NULL AND crawl_status = 'approved'""")
            total = (c1.rowcount or 0) + (c2.rowcount or 0)
            await self._conn.commit()
        return total

    async def due_recurring_events(self, today_md: str):
        async with self._conn.execute(
            """SELECT * FROM events
               WHERE event_type IN ('feast_day','nameday','veneration_day')
                 AND event_date = ?""",
            (today_md,),
        ) as cur:
            return [_row_to_dict(r) for r in await cur.fetchall()]

    async def due_new_icon_events(self, today_date: str):
        async with self._conn.execute(
            "SELECT * FROM events WHERE event_type = 'new_icon_added' AND event_date = ?",
            (today_date,),
        ) as cur:
            return [_row_to_dict(r) for r in await cur.fetchall()]

    async def get_followers(self, target_type, target_id):
        async with self._conn.execute(
            "SELECT user_id FROM follows WHERE target_type = ? AND target_id = ?",
            (target_type, target_id),
        ) as cur:
            return [r["user_id"] for r in await cur.fetchall()]

    async def already_notified(self, user_id, event_id, day: str) -> bool:
        async with self._conn.execute(
            """SELECT 1 FROM notifications_sent
               WHERE user_id = ? AND event_id = ? AND substr(sent_at, 1, 10) = ?
               LIMIT 1""",
            (user_id, event_id, day),
        ) as cur:
            return await cur.fetchone() is not None

    async def record_notification(self, user_id, event_id) -> None:
        async with self._lock:
            await self._conn.execute(
                "INSERT INTO notifications_sent (user_id, event_id, sent_at) VALUES (?, ?, ?)",
                (user_id, event_id, _iso(datetime.now(timezone.utc))),
            )
            await self._conn.commit()

    # --- Saint claims ledger -------------------------------------------------

    async def upsert_saint_by_qid(self, qid, display_name) -> int:
        async with self._lock:
            async with self._conn.execute(
                "SELECT id FROM saints WHERE qid = ?", (qid,)) as cur:
                row = await cur.fetchone()
            if row:
                return row["id"]
            # Adopt a name-only saint by backfilling its qid (migration/merge).
            # ponytail: a same display_name already bound to a *different* qid is
            # treated as the same saint — display name is a weak secondary key,
            # and saint name collisions across QIDs are vanishingly rare.
            async with self._conn.execute(
                "SELECT id, qid FROM saints WHERE canonical_name = ?", (display_name,)) as cur:
                row = await cur.fetchone()
            if row:
                if row["qid"] is None:
                    await self._conn.execute(
                        "UPDATE saints SET qid = ? WHERE id = ?", (qid, row["id"]))
                    await self._conn.commit()
                return row["id"]
            cur = await self._conn.execute(
                "INSERT INTO saints (canonical_name, qid) VALUES (?, ?)", (display_name, qid))
            sid = cur.lastrowid
            await self._conn.commit()
            return sid

    async def get_saint_id_by_qid(self, qid):
        async with self._conn.execute(
            "SELECT id FROM saints WHERE qid = ?", (qid,)) as cur:
            row = await cur.fetchone()
        return row["id"] if row else None

    async def _unique_covers_value(self, table: str) -> bool:
        """True if some UNIQUE index on ``table`` includes the ``value`` column."""
        async with self._conn.execute(f"PRAGMA index_list({table})") as cur:
            indexes = await cur.fetchall()
        for idx in indexes:
            if not idx["unique"]:
                continue
            async with self._conn.execute(f"PRAGMA index_info({idx['name']})") as c2:
                cols = {r["name"] for r in await c2.fetchall()}
            if "value" in cols:
                return True
        return False

    async def set_claims(self, saint_id, field, values, source_id, weight,
                         license, attribution) -> None:
        now = _iso(datetime.now(timezone.utc))
        async with self._lock:
            await self._conn.execute(
                "DELETE FROM saint_claims WHERE saint_id = ? AND field = ? AND source_id = ?",
                (saint_id, field, source_id))
            if values:
                await self._conn.executemany(
                    """INSERT INTO saint_claims (saint_id, field, value, source_id,
                                                 weight, license, attribution, observed_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    [(saint_id, field, v, source_id, weight, license, attribution, now)
                     for v in values])
            await self._conn.commit()

    async def recompute_saint(self, saint_id) -> None:
        async with self._lock:
            async with self._conn.execute(
                """SELECT field, value, source_id, weight, license, attribution
                   FROM saint_claims WHERE saint_id = ?""", (saint_id,)) as cur:
                claims = [_row_to_dict(r) for r in await cur.fetchall()]
            winners = reduce_claims(claims)
            # Reset every managed column, then apply winners: a claim that lost
            # its license (or vanished) must clear its slot — fail closed.
            cols = {c: None for c in managed_saint_columns()}
            cols.update(materialized_saint_columns(winners))
            assignments = ", ".join(f"{k} = ?" for k in cols)
            await self._conn.execute(
                f"UPDATE saints SET {assignments} WHERE id = ?",
                (*cols.values(), saint_id))
            await self._conn.commit()

    async def saint_bio_coverage(self):
        async with self._conn.execute(
            """SELECT COUNT(*) AS total,
                      COUNT(CASE WHEN bio_text IS NOT NULL AND bio_license IS NOT NULL
                                 THEN 1 END) AS with_bio
               FROM saints""") as cur:
            row = await cur.fetchone()
        return (row["total"], row["with_bio"])

    async def coverage(self):
        async with self._conn.execute(
            """SELECT
                 (SELECT COUNT(*) FROM saints) AS saints_total,
                 (SELECT COUNT(*) FROM saints
                    WHERE bio_text IS NOT NULL AND bio_license IS NOT NULL) AS saints_with_bio,
                 (SELECT COUNT(*) FROM saints WHERE qid IS NULL) AS saints_needs_review,
                 (SELECT COUNT(*) FROM saints WHERE feast_day IS NOT NULL) AS saints_with_feast,
                 (SELECT COUNT(*) FROM icons) AS icons_total,
                 (SELECT COUNT(*) FROM icons WHERE crawl_status='approved') AS icons_approved,
                 (SELECT COUNT(*) FROM icons WHERE saint_id IS NOT NULL) AS icons_linked,
                 (SELECT COUNT(*) FROM icons
                    WHERE saint_id IS NULL AND crawl_status='approved') AS icons_orphan,
                 (SELECT COUNT(*) FROM saint_claims) AS claims_total""") as cur:
            return _row_to_dict(await cur.fetchone())

    async def all_saint_names(self):
        async with self._conn.execute(
            "SELECT id, canonical_name, alt_names, qid FROM saints") as cur:
            return [_row_to_dict(r) for r in await cur.fetchall()]

    async def fetch_saint_candidate_pages(self):
        async with self._conn.execute(
            """SELECT title, content, attribution FROM pages
               WHERE namespace = 0 AND removed_at IS NULL AND content IS NOT NULL""") as cur:
            return [_row_to_dict(r) for r in await cur.fetchall()]

    async def needs_review_saints(self, limit):
        async with self._conn.execute(
            "SELECT canonical_name FROM saints WHERE qid IS NULL ORDER BY id LIMIT ?",
            (limit,)) as cur:
            return [r["canonical_name"] for r in await cur.fetchall()]
