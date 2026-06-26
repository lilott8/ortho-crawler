"""Storage abstraction with pluggable backends.

The scraper talks to a backend-agnostic :class:`Storage` interface. Which
concrete backend is used (PostgreSQL or SQLite) is decided by
``database.backend`` in the config file.
"""

from __future__ import annotations

import abc
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional

from config import DatabaseConfig
from mediawiki import PageContent

_SCHEMA_DIR = os.path.dirname(__file__)


@dataclass
class MediaRecord:
    """A downloaded media file's DB row (the bytes stay on disk)."""
    media_id: str                    # sha1, or a url-hash fallback
    title: str
    local_path: str
    mime: Optional[str]
    source_url: Optional[str]
    license_name: Optional[str]
    redistribution: str              # one of licenses.REDISTRIBUTION_LEVELS


# --- Icon & Saints data layer rows ------------------------------------------

@dataclass
class SourceState:
    """Result of upserting a source row."""
    source_id: int
    base_license_changed: bool       # true if base_license differs from stored


@dataclass
class IconRow:
    """An icon record ready to persist (post-gate)."""
    title: str
    image_source_id: int
    image_license: str               # '' allowed only for non-approved rows
    attribution_text: str            # '' allowed only for non-approved rows
    source_record_id: str
    crawl_status: str                # license_gate.CRAWL_STATUSES
    saint_id: Optional[int] = None
    image_url: Optional[str] = None  # local/CDN path, never a hotlink
    description: Optional[str] = None
    veneration_date: Optional[str] = None
    quarantine_reason: Optional[str] = None
    local_path: Optional[str] = None


@dataclass
class IconStoreResult:
    icon_id: int
    newly_approved: bool             # reached 'approved' for the first time


class Storage(abc.ABC):
    """Interface every storage backend implements."""

    @abc.abstractmethod
    async def close(self) -> None: ...

    @abc.abstractmethod
    async def apply_schema(self) -> None: ...

    @abc.abstractmethod
    async def get_crawl_state(self, pageids: Iterable[int]) -> Dict[int, dict]:
        """Return {pageid: {"revid", "last_crawled"}} for pages already on file.

        Drives the change-detection crawl decision: a page is re-crawled only if
        never crawled, or its current lastrevid differs from the stored revid.
        """

    @abc.abstractmethod
    async def mark_seen(self, members: List[dict]) -> None:
        """Upsert minimal rows and bump last_seen / categories for seen pages."""

    @abc.abstractmethod
    async def store_page(self, page: PageContent, categories: List[str],
                         media_paths: List[str]) -> None:
        """Persist freshly crawled content + downloaded media paths; stamp last_crawled."""

    @abc.abstractmethod
    async def get_unseen_active(self, run_started: datetime) -> List[int]:
        """Return pageids of active rows not observed since ``run_started``.

        These are candidates that disappeared from the configured categories
        this run; the caller decides which are truly deleted.
        """

    @abc.abstractmethod
    async def mark_removed(self, pageids: List[int], when: datetime) -> None:
        """Soft-delete: stamp removed_at so the pages are excluded from crawls."""

    @abc.abstractmethod
    async def store_media(self, record: MediaRecord) -> None:
        """Upsert a media file's metadata + redistribution level (no bytes)."""

    # --- Icon & Saints data layer -------------------------------------------

    @abc.abstractmethod
    async def upsert_source(self, name: str, base_license: str,
                            attribution_template: Optional[str],
                            requires_per_item_check: bool,
                            notes: Optional[str] = None) -> SourceState:
        """Insert or update a source row; report whether base_license changed.

        A changed base_license invalidates cached approvals (gate rule 3); the
        caller re-flags that source's icons via :meth:`reflag_icons_for_source`.
        """

    @abc.abstractmethod
    async def reflag_icons_for_source(self, source_id: int) -> int:
        """Reset approved icons of a source to pending_license_check; return count."""

    @abc.abstractmethod
    async def upsert_saint(self, canonical_name: str,
                           alt_names: Optional[List[str]] = None) -> int:
        """Resolve a saint by canonical_name, creating it if needed; return its id."""

    @abc.abstractmethod
    async def get_license_override(self, source_name: str,
                                   source_record_id: str) -> Optional[dict]:
        """Return a human override row for this record, or None.

        Shape: {decision, license, attribution, reviewer, reason}. Applied by the
        pipeline around the automated gate so manual decisions stay separate.
        """

    @abc.abstractmethod
    async def store_icon(self, row: IconRow) -> IconStoreResult:
        """Upsert an icon (idempotent by source + source_record_id).

        Returns the row id and whether it reached 'approved' for the first time
        (drives new_icon_added events).
        """

    @abc.abstractmethod
    async def count_followers(self, target_type: str, target_id: int) -> int:
        """Number of users following a saint/icon (for new_icon_added gating)."""

    @abc.abstractmethod
    async def record_event(self, target_type: str, target_id: int,
                           event_type: str, event_date: Optional[str]) -> Optional[int]:
        """Insert an event; return its id, or None if it already existed."""

    @abc.abstractmethod
    async def sync_recurring_events(self) -> int:
        """Materialize feast_day/veneration_day events from saints/icons.

        Idempotent; returns the number of newly created events. Lets the
        notification job fire as soon as feast/veneration dates get populated.
        """

    @abc.abstractmethod
    async def due_recurring_events(self, today_md: str) -> List[dict]:
        """Recurring events (feast/nameday/veneration) whose MM-DD is today."""

    @abc.abstractmethod
    async def due_new_icon_events(self, today_date: str) -> List[dict]:
        """one-off new_icon_added events created today (YYYY-MM-DD)."""

    @abc.abstractmethod
    async def get_followers(self, target_type: str, target_id: int) -> List[int]:
        """User ids following a target."""

    @abc.abstractmethod
    async def already_notified(self, user_id: int, event_id: int, day: str) -> bool:
        """True if this user was already notified of this event on ``day`` (YYYY-MM-DD)."""

    @abc.abstractmethod
    async def record_notification(self, user_id: int, event_id: int) -> None:
        """Persist that a notification was dispatched."""


def _schema_path(name: str) -> str:
    return os.path.join(_SCHEMA_DIR, name)


def parse_ts(value) -> Optional[datetime]:
    """Parse a MediaWiki / ISO-8601 timestamp into an aware datetime."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


async def create_storage(cfg: DatabaseConfig) -> Storage:
    """Construct and connect the storage backend named in the config."""
    backend = cfg.backend.lower()
    if backend == "postgres":
        from storage_postgres import PostgresStorage
        return await PostgresStorage.connect(cfg)
    if backend == "sqlite":
        from storage_sqlite import SqliteStorage
        return await SqliteStorage.connect(cfg)
    raise ValueError(
        f"Unknown database.backend {cfg.backend!r}; expected 'postgres' or 'sqlite'."
    )
