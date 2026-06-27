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
import json
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
from icon_sources import RawRecord, _HttpJson, build_adapters
from license_gate import APPROVED, GateResult, LicenseGate, QUARANTINED, REJECTED, Source
from saint_sources import resolve_qid
from storage import IconRow, Storage

log = logging.getLogger("ortho_scraper.icon_pipeline")


@dataclass
class IconRunStats:
    started: float = field(default_factory=time.monotonic)
    seen: int = 0
    by_status: Counter = field(default_factory=Counter)
    approved_new: int = 0
    unresolved: int = 0          # image kept, saint-link left for review (no clean QID)
    image_downloaded: int = 0
    image_deduped: int = 0
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
        # The rate limiter is injected via with_limiter() so image downloads
        # share the same politeness budget as the adapters' API calls.
        self._limiter = None
        self._http: Optional[_HttpJson] = None
        self._saint_cache: Dict[str, int] = {}
        self._stats = IconRunStats()

    # The limiter is injected explicitly so downloads share the same budget as
    # the adapters' API calls.
    def with_limiter(self, limiter) -> "IconPipeline":
        self._limiter = limiter
        self._http = _HttpJson(self._session, limiter,
                               self._cfg.http.max_retries, self._cfg.http.retry_backoff)
        return self

    async def run(self) -> None:
        icfg = self._cfg
        if not icfg.enabled:
            log.warning("icons.enabled is false; nothing to ingest.")
            return
        if self._http is None:
            raise RuntimeError("IconPipeline.with_limiter() must be called before run().")

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

        adapters = build_adapters(icfg.sources, self._http)
        if not adapters:
            log.warning("No enabled icon sources; nothing to ingest.")
            return

        for adapter in adapters:
            await self._ingest(adapter, source_objs[adapter.name], source_ids[adapter.name])

        self._log_summary()

    async def _ingest(self, adapter, source: Source, source_id: int) -> None:
        log.info("Source %r: ingesting...", adapter.name)
        n = 0
        async for record in adapter.records():
            n += 1
            self._stats.seen += 1
            result = await self._decide(source, record)
            saint_id = await self._resolve_saint(record)

            local_path = None
            image_ref = None
            if result.status == APPROVED:
                local_path = await self._materialize(record, result)
                if local_path is None:
                    # An approved record with no servable image is useless and
                    # could leak a hotlink — fail closed to quarantine instead.
                    result = GateResult(status=QUARANTINED, reason="image_unavailable")
                else:
                    image_ref = local_path

            res = await self._db.store_icon(IconRow(
                title=record.title,
                image_source_id=source_id,
                image_license=result.license or "",
                attribution_text=result.attribution or "",
                source_record_id=record.source_record_id,
                crawl_status=result.status,
                saint_id=saint_id,
                image_url=image_ref,
                description=record.description,
                veneration_date=None,        # no verified-licensed source yet (PRD §7)
                quarantine_reason=result.reason,
                local_path=local_path,
            ))
            self._stats.by_status[result.status] += 1
            if res.newly_approved:
                self._stats.approved_new += 1
                await self._maybe_new_icon_event(saint_id, res.icon_id)
            log.info("[%s %d] %-12s %s", adapter.name, n, result.status, record.title)
        log.info("Source %r: processed %d record(s).", adapter.name, n)

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
        if qid:
            saint_id = await self._db.upsert_saint_by_qid(qid, name)
            self._stats.saints += 1
        else:
            saint_id = None                       # image still stored; link -> review
            self._stats.unresolved += 1
        self._saint_cache[name] = saint_id
        return saint_id

    async def _maybe_new_icon_event(self, saint_id: Optional[int], icon_id: int) -> None:
        if saint_id is None:
            return
        if await self._db.count_followers("saint", saint_id) <= 0:
            return
        # UTC to match notifications_sent timestamps (which are UTC), so the
        # notify job's same-day dedup compares like with like.
        today = datetime.now(timezone.utc).date().isoformat()
        event_id = await self._db.record_event("saint", saint_id, "new_icon_added", today)
        if event_id is not None:
            self._stats.events += 1
            log.info("new_icon_added event for saint %d (icon %d).", saint_id, icon_id)

    async def _materialize(self, record: RawRecord, result: GateResult) -> Optional[str]:
        """Fetch the approved image into content-addressed storage; write a sidecar.

        Returns the local path, or None if the image can't be obtained / is too
        large. Files are deduplicated by sha1 of their bytes.
        """
        try:
            if record.local_source_path:
                data = await asyncio.to_thread(_read_bytes, record.local_source_path)
                ext = os.path.splitext(record.local_source_path)[1].lower()
            elif record.image_url:
                data = await self._download(record.image_url)
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
        dest = os.path.join(self._cfg.download_dir, sha1[:2], sha1 + ext)
        if os.path.exists(dest):
            self._stats.image_deduped += 1
        else:
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            await asyncio.to_thread(_write_bytes, dest, data)
            self._stats.image_downloaded += 1
        await self._write_sidecar(dest, record, result)
        return dest

    async def _download(self, url: str) -> bytes:
        async with self._limiter:
            async with self._session.get(url) as resp:
                resp.raise_for_status()
                return await resp.read()

    async def _write_sidecar(self, dest: str, record: RawRecord, result: GateResult) -> None:
        sidecar = dest + ".json"
        if os.path.exists(sidecar):
            return
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
        await asyncio.to_thread(
            _write_bytes, sidecar,
            json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8"))

    def _log_summary(self) -> None:
        s = self._stats
        st = s.by_status
        log.info("=" * 64)
        log.info("Icon ingest complete in %.1fs", s.elapsed())
        log.info("  Records   : %d seen | %d approved, %d quarantined, %d rejected, %d pending",
                 s.seen, st.get(APPROVED, 0), st.get(QUARANTINED, 0), st.get(REJECTED, 0),
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
    if s.name == "iconsaint":
        return f"dataset_path={s.dataset_path or '(unset — will skip)'}"
    return ""


def _read_bytes(path: str) -> bytes:
    with open(path, "rb") as fh:
        return fh.read()


def _write_bytes(path: str, data: bytes) -> None:
    with open(path, "wb") as fh:
        fh.write(data)
