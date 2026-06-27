"""Storage abstraction with pluggable backends.

The scraper talks to a backend-agnostic :class:`Storage` interface. Which
concrete backend is used (PostgreSQL or SQLite) is decided by
``database.backend`` in the config file.
"""

from __future__ import annotations

import abc
import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

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
    async def get_saint_id_by_qid(self, qid: str) -> Optional[int]:
        """Existing saint id for this QID, or None — **never creates**. Image
        producers link to already-seeded saints only (enrichment never seeds)."""

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

    # --- Saint claims ledger (additive multi-source merge) ------------------

    @abc.abstractmethod
    async def upsert_saint_by_qid(self, qid: str, display_name: str) -> int:
        """Resolve a saint by Wikidata QID, creating it if needed; return its id.

        If a name-only saint (qid IS NULL) already exists under ``display_name``,
        it is adopted by backfilling the qid (the migration/merge path) rather
        than creating a duplicate.
        """

    @abc.abstractmethod
    async def set_claims(self, saint_id: int, field: str, values: List[str],
                         source_id: int, weight: int,
                         license: Optional[str], attribution: Optional[str]) -> None:
        """Replace this source's entire contribution to (saint, field).

        Deletes the source's prior rows for the field, then inserts one row per
        value — so a re-ingest correctly handles updated *and* removed values, for
        scalar (one value) and multi-valued (many) fields alike. ``license=None``
        stores rows for audit but leaves them un-servable for non-fact fields.
        """

    async def add_claim(self, saint_id: int, field: str, value: str,
                        source_id: int, weight: int,
                        license: Optional[str], attribution: Optional[str]) -> None:
        """Convenience for a single-valued claim — a 1-element :meth:`set_claims`."""
        await self.set_claims(saint_id, field, [value], source_id, weight,
                              license, attribution)

    @abc.abstractmethod
    async def recompute_saint(self, saint_id: int) -> None:
        """Fold this saint's claims into the materialized saints.* columns.

        Idempotent; re-runnable any time a claim/weight/license changes. Only
        license-cleared claims (or fact fields) win a servable slot.
        """

    @abc.abstractmethod
    async def saint_bio_coverage(self) -> Tuple[int, int]:
        """(total saints, saints with a servable bio) — coverage at a glance."""

    @abc.abstractmethod
    async def coverage(self) -> Dict[str, int]:
        """Cross-pipeline visibility counts for the `--mode stats` report.

        Keys: saints_total, saints_with_bio, saints_with_feast,
        saints_needs_review, icons_total, icons_approved, icons_linked,
        icons_orphan, claims_total.
        """

    @abc.abstractmethod
    async def all_saint_names(self) -> List[dict]:
        """Rows of {id, canonical_name, alt_names, qid} for building name and QID
        indexes (OrthodoxWiki enrichment matches crawled pages by name, then QID)."""

    @abc.abstractmethod
    async def fetch_saint_candidate_pages(self) -> List[dict]:
        """Active main-namespace crawled pages with content, as
        {title, content, attribution} — bio-claim candidates for enrichment."""

    @abc.abstractmethod
    async def needs_review_saints(self, limit: int) -> List[str]:
        """Up to ``limit`` canonical names of needs-review saints (qid IS NULL),
        for the `--mode review` worklist."""


# Fact fields are core data with no licensing contract: a feast date, a short
# name, or a CC0 Wikidata description can't be license-encumbered, so they are
# servable without a cleared license. (Prose like `bio` is the opposite contract.)
FACT_FIELDS = frozenset({"feast_day", "alt_names", "description"})

# Fields whose reducer keeps the whole weight-ordered set (union) instead of one
# winner. Their value column holds a JSON array.
MULTI_VALUED_FIELDS = frozenset({"alt_names", "feast_day"})

# How a field maps onto saints.* columns: field -> (value_col,
# source_col_or_None, license_col_or_None). Multi-valued fields use value_col
# only (their array carries no single source/license).
_SAINT_FIELD_COLUMNS = {
    "bio": ("bio_text", "bio_source_id", "bio_license"),
    "feast_day": ("feast_day", None, None),
    "alt_names": ("alt_names", None, None),
    "description": ("description", None, None),
}

# Weight for human corrections (the `curated` source). Far above any producer so
# an explicit operator decision always wins the reducer.
CURATED_WEIGHT = 1_000_000


def reduce_claims(claims: Iterable[dict]) -> Dict[str, List[dict]]:
    """Per-field reducer: the servable claims for each field, weight-desc.

    Drops uncleared non-fact claims (fail closed). Returns a list per field so a
    scalar field's winner is ``[0]`` and a multi-valued field keeps the whole
    ordered set. Sort is stable, so equal-weight claims keep ingest order.
    """
    by_field: Dict[str, List[dict]] = {}
    for c in claims:
        field = c["field"]
        servable = field in FACT_FIELDS or bool(c.get("license"))
        if not servable:
            continue
        by_field.setdefault(field, []).append(c)
    for field, cs in by_field.items():
        cs.sort(key=lambda c: c["weight"], reverse=True)
    return by_field


def managed_saint_columns() -> List[str]:
    """Every saints.* column the reducer owns — reset these before writing winners
    so a withdrawn/withheld claim clears its slot (fail-closed)."""
    cols: List[str] = []
    for value_col, source_col, license_col in _SAINT_FIELD_COLUMNS.values():
        cols += [c for c in (value_col, source_col, license_col) if c]
    return cols


def materialized_saint_columns(reduced: Dict[str, List[dict]]) -> Dict[str, object]:
    """Translate reducer output into a {saints_column: value} update map.

    Scalar fields take the top claim; multi-valued fields union their values in
    weight order (dedup, order-preserving) into a JSON array.
    """
    cols: Dict[str, object] = {}
    for field, claim_list in reduced.items():
        mapping = _SAINT_FIELD_COLUMNS.get(field)
        if not mapping or not claim_list:
            continue  # unknown field has no materialized home yet
        value_col, source_col, license_col = mapping
        if field in MULTI_VALUED_FIELDS:
            seen, ordered = set(), []
            for c in claim_list:                      # already weight-desc
                if c["value"] not in seen:
                    seen.add(c["value"])
                    ordered.append(c["value"])
            cols[value_col] = json.dumps(ordered)
        else:
            top = claim_list[0]
            cols[value_col] = top["value"]
            if source_col:
                cols[source_col] = top["source_id"]
            if license_col:
                cols[license_col] = top.get("license")
    return cols


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
