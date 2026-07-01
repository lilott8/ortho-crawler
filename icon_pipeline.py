"""Icon & Saints ingestion pipeline.

Mirrors the wiki ``Scraper`` in spirit: pull records from each enabled source,
run them through the license gate, normalize, and persist. The crucial
difference is the hard licensing constraint — an icon only reaches
``crawl_status = 'approved'`` (and only then is its image materialized to disk)
if the gate, or a human override, positively clears it. Quarantined/rejected
records are still stored for audit but carry no servable image.

Flow per source:
    upsert source row → (re-flag approvals if base_license changed) →
    for each record:
        manual override?  →  else license gate
        resolve/attach saint
        if approved: materialize image (content-addressed) + sidecar
        store icon (idempotent by source + source_record_id)
        if newly approved AND the saint already has followers:
            write a one-off new_icon_added event (picked up by the notify job)

All HTTP is rate-limited through the shared :class:`~ratelimit.RateLimiter`.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional
from urllib.parse import urlparse

import aiohttp

from config import Config, select_policy
from content_store import content_path, read_bytes, write_bytes, write_sidecar
from icon_sources import RawRecord, _HttpJson, build_adapters
from license_gate import APPROVED, GateResult, LicenseGate, QUARANTINED, REJECTED, Source
from saint_sources import resolve_qid
from storage import IconRow, Storage

log = logging.getLogger("ortho_scraper.icon_pipeline")


@dataclass
class IconRunStats:
    started: float = field(default_factory=time.monotonic)
    seen: int = 0
    recrawl_skipped: int = 0     # within recrawl_after window — not re-fetched
    by_status: Counter = field(default_factory=Counter)
    approved_new: int = 0
    unresolved: int = 0          # image kept, saint-link left for review (no clean QID)
    image_downloaded: int = 0
    image_deduped: int = 0       # byte-identical on disk, or 304 Not Modified
    image_skipped: int = 0
    image_failed: int = 0
    events: int = 0
    saints: int = 0

    def elapsed(self) -> float:
        return time.monotonic() - self.started


class IconPipeline:
    def __init__(self, config: Config, session: aiohttp.ClientSession, db: Storage):
        self._cfg = config.icons
        self._policies = config.license_policies
        self._session = session
        self._db = db
        self._gate = LicenseGate()
        # Limiters are injected via with_limiters(): one per source (so a tight
        # budget doesn't throttle the others) plus a default for shared calls
        # (resolve_qid → Wikidata). _limiter is the *active* source limiter, set
        # at the start of each source's _ingest and used for image downloads.
        self._limiter = None
        self._default_limiter = None
        self._source_limiters: Dict[str, object] = {}
        self._http: Optional[_HttpJson] = None          # default (resolve_qid)
        self._source_http: Dict[str, _HttpJson] = {}
        self._saint_cache: Dict[str, int] = {}
        self._pending: set = set()                       # in-flight fan-out tasks
        self._stats = IconRunStats()

    def with_limiters(self, source_limiters: Dict[str, object],
                      default_limiter) -> "IconPipeline":
        h = self._cfg.http
        self._source_limiters = source_limiters
        self._default_limiter = default_limiter
        self._http = _HttpJson(self._session, default_limiter, h.max_retries, h.retry_backoff)
        self._source_http = {
            name: _HttpJson(self._session, lim, h.max_retries, h.retry_backoff)
            for name, lim in source_limiters.items()
        }
        return self

    async def run(self) -> None:
        icfg = self._cfg
        if not icfg.enabled:
            log.warning("icons.enabled is false; nothing to ingest.")
            return
        if self._http is None:
            raise RuntimeError("IconPipeline.with_limiters() must be called before run().")

        self._stats = IconRunStats()
        log.info("=" * 64)
        log.info("Icon ingest starting | %d source(s) enabled", len(icfg.enabled_sources()))
        for s in icfg.enabled_sources():
            log.info("  %-10s | %s", s.name, _describe_query(s))
        if not icfg.enabled_sources():
            log.info("  (no sources enabled)")
        log.info("=" * 64)

        # Seed source rows; a changed base_license invalidates cached approvals.
        source_objs: Dict[str, Source] = {}
        source_ids: Dict[str, int] = {}
        for sc in icfg.sources.values():
            state = await self._db.upsert_source(
                sc.name, sc.base_license, sc.attribution_template,
                sc.requires_per_item_check, sc.notes)
            source_ids[sc.name] = state.source_id
            source_objs[sc.name] = Source(
                name=sc.name, base_license=sc.base_license,
                attribution_template=sc.attribution_template,
                requires_per_item_check=sc.requires_per_item_check,
                allowed_licenses=set(sc.allowed_licenses), notes=sc.notes)
            if state.base_license_changed:
                n = await self._db.reflag_icons_for_source(state.source_id)
                log.warning("Source %r base_license changed -> re-flagged %d approved "
                            "icon(s) to pending_license_check.", sc.name, n)

        adapters = build_adapters(icfg.sources, self._source_http)
        if not adapters:
            log.warning("No enabled icon sources; nothing to ingest.")
            return

        for adapter in adapters:
            await self._ingest(adapter, source_objs[adapter.name], source_ids[adapter.name])

        # Drain in-flight new_icon_added fan-out tasks before we summarize/return,
        # so none are cancelled at shutdown and their exceptions surface.
        if self._pending:
            await asyncio.gather(*self._pending, return_exceptions=True)

        self._log_summary()

    async def _ingest(self, adapter, source: Source, source_id: int) -> None:
        log.info("Source %r: ingesting...", adapter.name)
        # Use this source's own limiter for image downloads (its API calls already
        # do, via the per-source _HttpJson inside the adapter).
        self._limiter = self._source_limiters.get(adapter.name) or self._default_limiter
        n = 0
        async for record in adapter.records():
            n += 1
            self._stats.seen += 1

            # Recrawl skip: a stored rendition still inside its recrawl_after
            # window is left untouched (no download, no re-gate) — saves quota.
            state = await self._db.get_icon_recrawl_state(source_id, record.uri)
            if state and not self._cfg.force_recrawl and not self._is_expired(state):
                self._stats.recrawl_skipped += 1
                continue

            result = await self._decide(source, record)
            saint_id = await self._resolve_saint(record)

            local_path = sha1 = etag = None
            if result.status == APPROVED:
                mat = await self._materialize(record, result, state)
                if mat is None or mat[0] is None:
                    # An approved record with no servable image is useless and
                    # could leak a hotlink — fail closed to quarantine instead.
                    result = GateResult(status=QUARANTINED, reason="image_unavailable")
                else:
                    local_path, sha1, etag = mat

            res = await self._db.store_icon(IconRow(
                source_id=source_id,
                uri=record.uri,
                title=record.title,
                crawl_status=result.status,
                license=result.license or "",
                attribution=result.attribution or "",
                source_record_id=record.source_record_id,
                sha1=sha1,
                etag=etag,
                description=record.description,
                quarantine_reason=result.reason,
                local_path=local_path,
            ))
            # Link the auto-resolved saint (0 or 1) BEFORE fan-out so the task
            # reads a committed icon_saints row (the "committed first" constraint).
            if saint_id is not None:
                await self._db.link_icon_saint(res.icon_id, saint_id)
            self._stats.by_status[result.status] += 1
            if res.newly_approved:
                self._stats.approved_new += 1
                self._spawn_fanout(res.icon_id)
            log.info("[%s %d] %-12s %s", adapter.name, n, result.status, record.title)
        log.info("Source %r: processed %d record(s).", adapter.name, n)

    def _is_expired(self, state: dict) -> bool:
        """True if a stored icon is due for mandatory recrawl.

        recrawl_after == 0 → never auto-recrawl (recrawl on demand only).
        """
        after = self._cfg.recrawl_after
        if not after:
            return False
        last = state.get("last_crawled")
        if last is None:
            return True
        if last.tzinfo is None:                 # some backends hand back naive UTC
            last = last.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) >= last + after

    async def _decide(self, source: Source, record: RawRecord) -> GateResult:
        """Precedence: per-record override > per-type policy > automated gate."""
        override = await self._db.get_license_override(source.name, record.source_record_id)
        if override:
            if override.get("decision") == "approved":
                # Attribution is mandatory even for manual approvals.
                attribution = (override.get("attribution")
                               or f"Reviewed/approved by {override.get('reviewer') or 'staff'}")
                return GateResult(status=APPROVED,
                                  license=override.get("license") or source.base_license,
                                  attribution=attribution, reason="manual_override")
            return GateResult(status=REJECTED,
                              reason=f"manual_override:{override.get('reason') or 'rejected'}")
        policy = select_policy(self._policies, "icon", source.name, None)
        if policy:
            if policy.decision == "approved":
                lic = policy.license or source.base_license
                # Attribution stays mandatory: synthesize one if the policy omits it.
                attribution = policy.attribution or f"{source.name} ({lic})"
                return GateResult(status=APPROVED, license=lic,
                                  attribution=attribution, reason="policy_override")
            return GateResult(status=REJECTED, reason="policy_override")
        return self._gate.evaluate(source, record)

    async def _resolve_saint(self, record: RawRecord) -> Optional[int]:
        """Resolve a record's (low-trust) saint label to a QID and link on a clean
        hit only; otherwise keep the image but leave the link for review (Q13)."""
        name = (record.saint_name or "").strip()
        if not name:
            return None
        if name in self._saint_cache:            # None is a valid cached result
            return self._saint_cache[name]
        qid = None
        try:
            qid = await resolve_qid(name, self._http)
        except Exception as exc:  # noqa: BLE001 - a resolver hiccup must not kill ingest
            log.debug("QID resolve failed for %r: %s", name, exc)
        # Link to an *existing* (Wikipedia-seeded) saint only — image producers
        # never seed saints. No QID, or a QID we haven't seeded -> needs-review
        # (the image is still stored and servable; only the link waits).
        saint_id = await self._db.get_saint_id_by_qid(qid) if qid else None
        if saint_id is not None:
            self._stats.saints += 1
        else:
            self._stats.unresolved += 1
        self._saint_cache[name] = saint_id
        return saint_id

    def _spawn_fanout(self, icon_id: int) -> None:
        """Offload new_icon_added fan-out to a background task (not an OS thread —
        the DB driver is async). Hold a ref so it isn't GC'd; run() gathers them."""
        task = asyncio.create_task(self._fan_out_events(icon_id))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    async def _fan_out_events(self, icon_id: int) -> None:
        """Write a new_icon_added event for every linked saint that has followers.

        An icon may depict several saints (m2m); each followed saint gets an
        event. The notify job's once-per-user-per-day dedup keeps a user who
        follows two of them from being pinged twice for the same day.
        """
        try:
            saint_ids = await self._db.linked_saints(icon_id)
            # UTC to match notifications_sent timestamps, so the notify job's
            # same-day dedup compares like with like.
            today = datetime.now(timezone.utc).date().isoformat()
            for saint_id in saint_ids:
                if await self._db.count_followers("saint", saint_id) <= 0:
                    continue
                event_id = await self._db.record_event(
                    "saint", saint_id, "new_icon_added", today)
                if event_id is not None:
                    self._stats.events += 1
                    log.info("new_icon_added event for saint %d (icon %d).",
                             saint_id, icon_id)
        except Exception as exc:  # noqa: BLE001 - a fan-out hiccup must not kill the run
            log.warning("new_icon_added fan-out failed for icon %d: %s", icon_id, exc)

    async def _materialize(self, record: RawRecord, result: GateResult,
                           prior: Optional[dict]):
        """Fetch the approved image into content-addressed storage; write a sidecar.

        Returns ``(local_path, sha1, etag)``, or None if the image can't be
        obtained / is too large. Files are content-addressed by sha1 (identical
        bytes share one file on disk). On a recrawl, a conditional GET against the
        stored ``etag`` lets the server answer 304 Not Modified — then we reuse
        the prior bytes/path instead of re-downloading.
        """
        prior = prior or {}
        try:
            if record.local_source_path:
                data = await asyncio.to_thread(read_bytes, record.local_source_path)
                ext = os.path.splitext(record.local_source_path)[1].lower()
                etag = None
            elif record.image_url:
                data, etag = await self._download(record.image_url, prior.get("etag"))
                if data is None:                      # 304 Not Modified — unchanged
                    self._stats.image_deduped += 1
                    return prior.get("local_path"), prior.get("sha1"), prior.get("etag")
                ext = os.path.splitext(urlparse(record.image_url).path)[1].lower() or ".img"
            else:
                return None
        except Exception as exc:  # noqa: BLE001 - one bad image shouldn't kill the run
            log.warning("Failed to fetch image for %r: %s", record.title, exc)
            self._stats.image_failed += 1
            return None

        max_size = self._cfg.max_file_size
        if max_size and len(data) > max_size:
            log.debug("Skipping oversize icon image %r (%d bytes).", record.title, len(data))
            self._stats.image_skipped += 1
            return None

        sha1 = hashlib.sha1(data).hexdigest()
        dest = content_path(self._cfg.download_dir, sha1, ext)
        if os.path.exists(dest):
            self._stats.image_deduped += 1
        else:
            await asyncio.to_thread(write_bytes, dest, data)
            self._stats.image_downloaded += 1
        await self._write_sidecar(dest, record, result)
        return dest, sha1, etag

    async def _download(self, url: str, prior_etag: Optional[str] = None):
        """Download bytes, returning ``(data, etag)``. With a prior etag, sends a
        conditional GET; a 304 returns ``(None, prior_etag)`` (caller reuses)."""
        headers = {"If-None-Match": prior_etag} if prior_etag else {}
        async with self._limiter:
            async with self._session.get(url, headers=headers) as resp:
                if resp.status == 304:
                    return None, prior_etag
                resp.raise_for_status()
                data = await resp.read()
                return data, resp.headers.get("ETag")

    async def _write_sidecar(self, dest: str, record: RawRecord, result: GateResult) -> None:
        payload = {
            "title": record.title,
            "source": record.source,
            "source_record_id": record.source_record_id,
            "saint": record.saint_name,
            "license": result.license,
            "attribution": result.attribution,
            "crawl_status": result.status,
            "image_origin": record.image_url or record.local_source_path,
            "captured_at": datetime.now(timezone.utc).isoformat(),
        }
        await asyncio.to_thread(write_sidecar, dest + ".json", payload)

    def _log_summary(self) -> None:
        s = self._stats
        st = s.by_status
        log.info("=" * 64)
        log.info("Icon ingest complete in %.1fs", s.elapsed())
        log.info("  Records   : %d seen | %d recrawl-skipped (within window)", s.seen,
                 s.recrawl_skipped)
        log.info("  Verdicts  : %d approved, %d quarantined, %d rejected, %d pending",
                 st.get(APPROVED, 0), st.get(QUARANTINED, 0), st.get(REJECTED, 0),
                 st.get("pending_license_check", 0))
        log.info("  Approvals : %d newly approved | %d saints linked | %d links unresolved",
                 s.approved_new, s.saints, s.unresolved)
        log.info("  Images    : %d downloaded, %d already-on-disk, %d skipped, %d failed",
                 s.image_downloaded, s.image_deduped, s.image_skipped, s.image_failed)
        log.info("  Events    : %d new_icon_added written", s.events)
        log.info("=" * 64)


def _describe_query(s) -> str:
    """One-line summary of what a source will query, for the startup banner."""
    if s.name == "met_api":
        return f"queries={s.queries or ['icon']} (max {s.max_objects} objects)"
    if s.name == "wikimedia":
        return f"search_terms={s.search_terms or ['Orthodox icon']} (max {s.max_files} files)"
    if s.name == "wikipedia":
        cat = s.category or 'Category:Eastern Orthodox icons'
        return f"category={cat!r} depth={s.subcat_depth} (max {s.max_files})"
    if s.name == "wikiart":
        key = "set" if s.api_key else "MISSING — will skip"
        return f"queries={s.queries or ['icon']} (max {s.max_objects}); api_key {key}"
    if s.name == "iconsaint":
        return f"dataset_path={s.dataset_path or '(unset — will skip)'}"
    if s.name == "openverse":
        return f"queries={s.queries or ['Byzantine icon']} (max {s.max_objects} objects)"
    return ""
