"""PostgreSQL storage backend (asyncpg)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List

import asyncpg

import json

from config import DatabaseConfig
from mediawiki import PageContent
from storage import (Storage, IconRow, IconStoreResult, SourceState,
                     parse_ts, _schema_path)

log = logging.getLogger("ortho_scraper.storage.postgres")


def _affected(status: str) -> int:
    """Parse asyncpg's command status ('INSERT 0 5') into the row count."""
    try:
        return int(status.split()[-1])
    except (ValueError, IndexError):
        return 0


class PostgresStorage(Storage):
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    @classmethod
    async def connect(cls, cfg: DatabaseConfig) -> "PostgresStorage":
        pool = await asyncpg.create_pool(
            min_size=cfg.pool_min_size,
            max_size=cfg.pool_max_size,
            **cfg.dsn_kwargs(),
        )
        log.info("Connected to PostgreSQL database %r.", cfg.name)
        return cls(pool)

    async def close(self) -> None:
        await self._pool.close()

    async def apply_schema(self) -> None:
        with open(_schema_path("schema.postgres.sql"), "r", encoding="utf-8") as fh:
            ddl = fh.read()
        async with self._pool.acquire() as conn:
            await conn.execute(ddl)
        log.info("Schema ensured.")

    async def get_crawl_state(self, pageids: Iterable[int]) -> Dict[int, dict]:
        ids = list(pageids)
        if not ids:
            return {}
        rows = await self._pool.fetch(
            "SELECT pageid, revid, last_crawled FROM pages WHERE pageid = ANY($1::bigint[])",
            ids,
        )
        return {r["pageid"]: {"revid": r["revid"], "last_crawled": r["last_crawled"]}
                for r in rows}

    async def mark_seen(self, members: List[dict]) -> None:
        if not members:
            return
        now = datetime.now(timezone.utc)
        rows = [
            (m["pageid"], m["title"], m["url"], m["namespace"], m["categories"], now)
            for m in members
        ]
        async with self._pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO pages (pageid, title, url, namespace, categories,
                                   first_seen, last_seen)
                VALUES ($1, $2, $3, $4, $5, $6, $6)
                ON CONFLICT (pageid) DO UPDATE SET
                    title      = EXCLUDED.title,
                    url        = EXCLUDED.url,
                    namespace  = EXCLUDED.namespace,
                    last_seen  = EXCLUDED.last_seen,
                    -- Seeing a page again resurrects a previously-removed row.
                    removed_at = NULL
                    -- categories is left to store_page (authoritative API
                    -- membership); here it's only a placeholder on insert.
                """,
                rows,
            )

    async def store_page(self, page: PageContent, categories: List[str],
                         media_paths: List[str]) -> None:
        now = datetime.now(timezone.utc)
        touched = parse_ts(page.touched)
        content_length = len(page.content) if page.content is not None else None
        await self._pool.execute(
            """
            INSERT INTO pages (pageid, title, url, namespace, revid, content,
                               content_length, categories, page_touched,
                               first_seen, last_seen, last_crawled, media_paths,
                               contributors, attribution)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10, $10, $11, $12, $13)
            ON CONFLICT (pageid) DO UPDATE SET
                title          = EXCLUDED.title,
                url            = EXCLUDED.url,
                namespace      = EXCLUDED.namespace,
                revid          = EXCLUDED.revid,
                content        = EXCLUDED.content,
                content_length = EXCLUDED.content_length,
                categories     = EXCLUDED.categories,
                page_touched   = EXCLUDED.page_touched,
                last_seen      = EXCLUDED.last_seen,
                last_crawled   = EXCLUDED.last_crawled,
                media_paths    = EXCLUDED.media_paths,
                contributors   = EXCLUDED.contributors,
                attribution    = EXCLUDED.attribution
            """,
            page.pageid, page.title, page.url, page.ns, page.revid, page.content,
            content_length, categories, touched, now, media_paths,
            page.contributors, page.attribution,
        )

    async def get_unseen_active(self, run_started: datetime) -> List[int]:
        rows = await self._pool.fetch(
            "SELECT pageid FROM pages WHERE removed_at IS NULL AND last_seen < $1",
            run_started,
        )
        return [r["pageid"] for r in rows]

    async def mark_removed(self, pageids: List[int], when: datetime) -> None:
        if not pageids:
            return
        await self._pool.execute(
            "UPDATE pages SET removed_at = $1 WHERE pageid = ANY($2::bigint[])",
            when, pageids,
        )

    async def store_media(self, record) -> None:
        now = datetime.now(timezone.utc)
        await self._pool.execute(
            """
            INSERT INTO media (media_id, title, local_path, mime, source_url,
                               license_name, redistribution, first_seen, last_seen)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
            ON CONFLICT (media_id) DO UPDATE SET
                title          = EXCLUDED.title,
                local_path     = EXCLUDED.local_path,
                mime           = EXCLUDED.mime,
                source_url     = EXCLUDED.source_url,
                license_name   = EXCLUDED.license_name,
                redistribution = EXCLUDED.redistribution,
                last_seen      = EXCLUDED.last_seen
            """,
            record.media_id, record.title, record.local_path, record.mime,
            record.source_url, record.license_name, record.redistribution, now,
        )

    # --- Icon & Saints data layer -------------------------------------------

    async def upsert_source(self, name, base_license, attribution_template,
                            requires_per_item_check, notes=None) -> SourceState:
        now = datetime.now(timezone.utc)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                prior = await conn.fetchval(
                    "SELECT base_license FROM sources WHERE name = $1", name)
                changed = prior is not None and prior != base_license
                sid = await conn.fetchval(
                    """
                    INSERT INTO sources (name, base_license, attribution_template,
                                         requires_per_item_check, notes, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $6)
                    ON CONFLICT (name) DO UPDATE SET
                        base_license            = EXCLUDED.base_license,
                        attribution_template    = EXCLUDED.attribution_template,
                        requires_per_item_check = EXCLUDED.requires_per_item_check,
                        notes                   = EXCLUDED.notes,
                        updated_at              = EXCLUDED.updated_at
                    RETURNING id
                    """,
                    name, base_license, attribution_template,
                    requires_per_item_check, notes, now,
                )
        return SourceState(source_id=sid, base_license_changed=changed)

    async def reflag_icons_for_source(self, source_id: int) -> int:
        status = await self._pool.execute(
            """UPDATE icons SET crawl_status = 'pending_license_check', updated_at = now()
               WHERE image_source_id = $1 AND crawl_status = 'approved'""",
            source_id,
        )
        return _affected(status)

    async def upsert_saint(self, canonical_name, alt_names=None) -> int:
        alt = json.dumps(alt_names) if alt_names else None
        return await self._pool.fetchval(
            """INSERT INTO saints (canonical_name, alt_names) VALUES ($1, $2)
               ON CONFLICT (canonical_name) DO UPDATE SET
                   alt_names = COALESCE(EXCLUDED.alt_names, saints.alt_names)
               RETURNING id""",
            canonical_name, alt,
        )

    async def get_license_override(self, source_name, source_record_id):
        row = await self._pool.fetchrow(
            """SELECT decision, license, attribution, reviewer, reason
               FROM license_overrides WHERE source_name = $1 AND source_record_id = $2""",
            source_name, source_record_id,
        )
        return dict(row) if row else None

    async def store_icon(self, row: IconRow) -> IconStoreResult:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                prior = await conn.fetchval(
                    """SELECT crawl_status FROM icons
                       WHERE image_source_id = $1 AND source_record_id = $2""",
                    row.image_source_id, row.source_record_id,
                )
                icon_id = await conn.fetchval(
                    """
                    INSERT INTO icons (saint_id, title, image_url, image_source_id,
                                       image_license, attribution_text, description,
                                       veneration_date, source_record_id, crawl_status,
                                       quarantine_reason, local_path)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (image_source_id, source_record_id) DO UPDATE SET
                        saint_id          = EXCLUDED.saint_id,
                        title             = EXCLUDED.title,
                        image_url         = EXCLUDED.image_url,
                        image_license     = EXCLUDED.image_license,
                        attribution_text  = EXCLUDED.attribution_text,
                        description       = EXCLUDED.description,
                        veneration_date   = EXCLUDED.veneration_date,
                        crawl_status      = EXCLUDED.crawl_status,
                        quarantine_reason = EXCLUDED.quarantine_reason,
                        local_path        = EXCLUDED.local_path,
                        updated_at        = now()
                    RETURNING id
                    """,
                    row.saint_id, row.title, row.image_url, row.image_source_id,
                    row.image_license, row.attribution_text, row.description,
                    row.veneration_date, row.source_record_id, row.crawl_status,
                    row.quarantine_reason, row.local_path,
                )
        newly_approved = row.crawl_status == "approved" and prior != "approved"
        return IconStoreResult(icon_id=icon_id, newly_approved=newly_approved)

    async def count_followers(self, target_type, target_id) -> int:
        return await self._pool.fetchval(
            "SELECT COUNT(*) FROM follows WHERE target_type = $1 AND target_id = $2",
            target_type, target_id,
        )

    async def record_event(self, target_type, target_id, event_type, event_date):
        return await self._pool.fetchval(
            """INSERT INTO events (target_type, target_id, event_type, event_date)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (target_type, target_id, event_type, event_date) DO NOTHING
               RETURNING id""",
            target_type, target_id, event_type, event_date,
        )

    async def sync_recurring_events(self) -> int:
        total = 0
        s1 = await self._pool.execute(
            """INSERT INTO events (target_type, target_id, event_type, event_date)
               SELECT 'saint', id, 'feast_day', feast_day FROM saints
               WHERE feast_day IS NOT NULL
               ON CONFLICT DO NOTHING""")
        s2 = await self._pool.execute(
            """INSERT INTO events (target_type, target_id, event_type, event_date)
               SELECT 'icon', id, 'veneration_day', veneration_date FROM icons
               WHERE veneration_date IS NOT NULL AND crawl_status = 'approved'
               ON CONFLICT DO NOTHING""")
        total = _affected(s1) + _affected(s2)
        return total

    async def due_recurring_events(self, today_md: str):
        rows = await self._pool.fetch(
            """SELECT * FROM events
               WHERE event_type IN ('feast_day','nameday','veneration_day')
                 AND event_date = $1""",
            today_md,
        )
        return [dict(r) for r in rows]

    async def due_new_icon_events(self, today_date: str):
        rows = await self._pool.fetch(
            "SELECT * FROM events WHERE event_type = 'new_icon_added' AND event_date = $1",
            today_date,
        )
        return [dict(r) for r in rows]

    async def get_followers(self, target_type, target_id):
        rows = await self._pool.fetch(
            "SELECT user_id FROM follows WHERE target_type = $1 AND target_id = $2",
            target_type, target_id,
        )
        return [r["user_id"] for r in rows]

    async def already_notified(self, user_id, event_id, day: str) -> bool:
        return await self._pool.fetchval(
            """SELECT EXISTS (SELECT 1 FROM notifications_sent
               WHERE user_id = $1 AND event_id = $2 AND sent_at::date = $3::date)""",
            user_id, event_id, day,
        )

    async def record_notification(self, user_id, event_id) -> None:
        await self._pool.execute(
            "INSERT INTO notifications_sent (user_id, event_id, sent_at) VALUES ($1, $2, now())",
            user_id, event_id,
        )
