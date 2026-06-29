"""Source adapters for the Icon & Saints data layer.

Each adapter knows how to pull raw icon records from one licensed source and
normalize them into a :class:`RawRecord`. Adapters do *not* decide licensing —
they surface the signal the :class:`~license_gate.LicenseGate` needs and leave
the verdict to the gate (fail-closed). They also never assume an image is
servable; the pipeline only materializes images the gate approves.

All outbound HTTP goes through the shared :class:`~ratelimit.RateLimiter` (same
politeness contract as the wiki scraper) via :class:`_HttpJson`.

Sources (see PRD §2):
  * ``met_api``   — Met Open Access REST API (public domain per-object).
  * ``wikimedia`` — Wikimedia Commons API (per-file license tag, via
    ``extmetadata`` — Commons is modern enough to expose it, unlike the old
    OrthodoxWiki backend).
  * ``iconsaint`` — local copy of the ICONSAINT GitHub dataset (blanket CC BY,
    images-only). Read from a configured ``dataset_path`` so the required
    human license check happens before bytes are pointed at the pipeline.
"""

from __future__ import annotations

import abc
import asyncio
import csv
import logging
import os
import re
from dataclasses import dataclass, field
from typing import AsyncIterator, Dict, List, Optional

import aiohttp

from config import IconSourceConfig
from ratelimit import RateLimiter

log = logging.getLogger("ortho_scraper.icon_sources")

MET_SEARCH_URL = "https://collectionapi.metmuseum.org/public/collection/v1/search"
MET_OBJECT_URL = "https://collectionapi.metmuseum.org/public/collection/v1/objects/{id}"
COMMONS_API_URL = "https://commons.wikimedia.org/w/api.php"

_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".tif", ".tiff", ".webp", ".bmp"}

# Conservative saint-name guess from a Met/Commons title: only fires on an
# explicit "Saint/St./Holy <Name…>" phrase. It's a low-trust hint — the pipeline
# resolves it to a QID and links only to an already-seeded saint, so a wrong
# guess simply fails to link (the image is still stored).
_SAINT_HINT_RE = re.compile(
    r"\b(?:Saint|St\.?|Holy)\s+([A-Z][a-zA-Z]+(?:\s+(?:of\s+|the\s+)?[A-Z][a-zA-Z]+){0,3})")


def guess_saint_name(title: Optional[str]) -> Optional[str]:
    """Best-effort saint label from an image title, or None. Conservative on
    purpose; downstream QID resolution + existing-saint linkage make it safe."""
    if not title:
        return None
    text = re.sub(r"^File:", "", title)
    text = re.sub(r"\.\w{3,4}$", "", text)       # drop a file extension
    m = _SAINT_HINT_RE.search(text)
    return m.group(0).strip() if m else None


@dataclass
class RawRecord:
    """A normalized, pre-gate icon record emitted by an adapter.

    ``license_signal`` carries only what the gate inspects (e.g.
    ``{"is_public_domain": True}`` for Met, ``{"license_short": "CC BY-SA 4.0",
    "author": ...}`` for Wikimedia); the gate's verdict is authoritative.

    Exactly one image origin is set: ``image_url`` (download) or
    ``local_source_path`` (copy an already-local file, e.g. ICONSAINT).
    """
    source: str
    source_record_id: str
    title: str
    saint_name: Optional[str] = None
    description: Optional[str] = None
    image_url: Optional[str] = None
    local_source_path: Optional[str] = None
    license_signal: dict = field(default_factory=dict)
    raw: dict = field(default_factory=dict)


class _HttpJson:
    """Minimal rate-limited, retried JSON GET helper shared by adapters."""

    def __init__(self, session: aiohttp.ClientSession, limiter: RateLimiter,
                 max_retries: int = 3, retry_backoff: float = 1.0):
        self._session = session
        self._limiter = limiter
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff

    async def get(self, url: str, params: Optional[dict] = None) -> dict:
        import random
        attempt = 0
        while True:
            attempt += 1
            try:
                async with self._limiter:
                    async with self._session.get(url, params=params) as resp:
                        if resp.status >= 500:
                            raise aiohttp.ClientResponseError(
                                resp.request_info, resp.history, status=resp.status,
                                message=f"server error {resp.status}")
                        resp.raise_for_status()
                        # Met sometimes returns text/json; don't enforce content-type.
                        return await resp.json(content_type=None)
            except aiohttp.ClientResponseError as exc:
                # 4xx is permanent (404 deleted object, 403, ...) — retrying is
                # pointless; let the caller skip it. Only 5xx falls through to retry.
                if exc.status < 500:
                    raise
                if attempt > self._max_retries:
                    log.warning("GET %s failed after %d attempts: %s", url, attempt, exc)
                    raise
                delay = self._retry_backoff * (2 ** (attempt - 1)) + random.uniform(0, 1)
                log.debug("Transient error on %s (%s); retry %d/%d in %.1fs",
                          url, exc, attempt, self._max_retries, delay)
                await asyncio.sleep(delay)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt > self._max_retries:
                    log.warning("GET %s failed after %d attempts: %s", url, attempt, exc)
                    raise
                delay = self._retry_backoff * (2 ** (attempt - 1)) + random.uniform(0, 1)
                log.debug("Transient error on %s (%s); retry %d/%d in %.1fs",
                          url, exc, attempt, self._max_retries, delay)
                await asyncio.sleep(delay)


class SourceAdapter(abc.ABC):
    """Yields normalized records for one source."""

    name: str = ""

    @abc.abstractmethod
    def records(self) -> AsyncIterator[RawRecord]:
        ...


class MetAdapter(SourceAdapter):
    """Met Open Access: search by query terms, then fetch each object.

    Per-object: ``isPublicDomain`` must be checked individually (the gate does
    that); we never bulk-assume. Bounded by ``max_objects`` to stay polite.
    """

    name = "met_api"

    def __init__(self, cfg: IconSourceConfig, http: _HttpJson):
        self._cfg = cfg
        self._http = http

    async def records(self) -> AsyncIterator[RawRecord]:
        queries = self._cfg.queries or ["icon"]
        seen: set = set()
        emitted = 0
        for query in queries:
            if emitted >= self._cfg.max_objects:
                break
            data = await self._http.get(MET_SEARCH_URL,
                                        {"q": query, "hasImages": "true"})
            object_ids = data.get("objectIDs") or []
            log.info("[met_api] query %r -> %d object(s).", query, len(object_ids))
            for oid in object_ids:
                if emitted >= self._cfg.max_objects:
                    break
                if oid in seen:
                    continue
                seen.add(oid)
                try:
                    obj = await self._http.get(MET_OBJECT_URL.format(id=oid))
                except aiohttp.ClientError as exc:
                    # One bad/deleted object (e.g. 404) shouldn't kill the crawl.
                    log.debug("[met_api] skipping object %s: %s", oid, exc)
                    continue
                image = obj.get("primaryImage") or obj.get("primaryImageSmall")
                if not image:
                    continue  # nothing servable to ingest
                emitted += 1
                title = obj.get("title") or f"Met object {oid}"
                yield RawRecord(
                    source=self.name,
                    source_record_id=str(obj.get("objectID", oid)),
                    title=title,
                    saint_name=guess_saint_name(title),   # low-trust hint, link-only
                    description=obj.get("objectName") or obj.get("medium"),
                    image_url=image,
                    license_signal={
                        "is_public_domain": obj.get("isPublicDomain") is True,
                        "author": obj.get("artistDisplayName") or None,
                    },
                    raw=obj,
                )
        log.info("[met_api] emitted %d record(s).", emitted)


class WikimediaAdapter(SourceAdapter):
    """Wikimedia Commons: search the File namespace, read per-file license tags.

    Commons exposes ``extmetadata`` (license short name, artist), so the gate can
    check each file. Bounded by ``max_files``.
    """

    name = "wikimedia"

    def __init__(self, cfg: IconSourceConfig, http: _HttpJson):
        self._cfg = cfg
        self._http = http

    async def records(self) -> AsyncIterator[RawRecord]:
        terms = self._cfg.search_terms or ["Orthodox icon"]
        seen: set = set()
        emitted = 0
        for term in terms:
            if emitted >= self._cfg.max_files:
                break
            cont: Dict[str, str] = {}
            while emitted < self._cfg.max_files:
                params = {
                    "action": "query",
                    "format": "json",
                    "formatversion": "2",
                    "generator": "search",
                    "gsrsearch": term,
                    "gsrnamespace": "6",        # File:
                    "gsrlimit": "50",
                    "prop": "imageinfo",
                    "iiprop": "url|extmetadata|mime",
                    **cont,
                }
                data = await self._http.get(COMMONS_API_URL, params)
                pages = data.get("query", {}).get("pages", [])
                for page in pages:
                    if emitted >= self._cfg.max_files:
                        break
                    title = page.get("title")
                    if not title or title in seen:
                        continue
                    seen.add(title)
                    infos = page.get("imageinfo") or []
                    if not infos:
                        continue
                    ii = infos[0]
                    ext = ii.get("extmetadata", {}) or {}
                    license_short = (ext.get("LicenseShortName", {}) or {}).get("value")
                    artist = (ext.get("Artist", {}) or {}).get("value")
                    emitted += 1
                    yield RawRecord(
                        source=self.name,
                        source_record_id=str(page.get("pageid") or title),
                        title=title,
                        saint_name=guess_saint_name(title),   # low-trust hint, link-only
                        description=(ext.get("ImageDescription", {}) or {}).get("value"),
                        image_url=ii.get("url"),
                        license_signal={
                            "license_short": license_short,
                            "author": _strip_html(artist),
                        },
                        raw=page,
                    )
                cont = data.get("continue", {})
                if not cont:
                    break
        log.info("[wikimedia] emitted %d record(s).", emitted)


class IconsaintAdapter(SourceAdapter):
    """ICONSAINT dataset, read from a local copy of the GitHub repo.

    Images-only (saint class labels, no bios/feast days). License is a blanket
    CC BY grant verified at the source level, so there is no per-record license
    signal — the gate clears records via ``source.base_license``.

    Two layouts are supported:
      * ``manifest`` CSV with columns ``image_path,saint_name`` (extra columns
        like ``icon_id`` are preserved in ``raw``); or
      * directory-per-class: ``<dataset_path>/<Saint Name>/<image files>``.

    If ``dataset_path`` is empty the adapter yields nothing (so the required
    human license check gates real ingestion, not the code path).
    """

    name = "iconsaint"

    def __init__(self, cfg: IconSourceConfig):
        self._cfg = cfg

    async def records(self) -> AsyncIterator[RawRecord]:
        path = self._cfg.dataset_path
        if not path:
            log.warning("[iconsaint] dataset_path not set; skipping (point it at a "
                        "local checkout after the manual CC BY repo-license check).")
            return
        if not os.path.isdir(path):
            log.warning("[iconsaint] dataset_path %r is not a directory; skipping.", path)
            return

        # Reading a local tree is blocking; do it off the event loop.
        rows = await asyncio.to_thread(self._scan, path)
        for rec in rows:
            yield rec
        log.info("[iconsaint] emitted %d record(s) from %s.", len(rows), path)

    def _scan(self, path: str) -> List[RawRecord]:
        if self._cfg.manifest:
            manifest = self._cfg.manifest
            if not os.path.isabs(manifest):
                manifest = os.path.join(path, manifest)
            return self._from_manifest(path, manifest)
        return self._from_dirs(path)

    def _from_manifest(self, base: str, manifest: str) -> List[RawRecord]:
        records: List[RawRecord] = []
        with open(manifest, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                rel = row.get("image_path") or row.get("path") or row.get("file")
                if not rel:
                    continue
                img = rel if os.path.isabs(rel) else os.path.join(base, rel)
                saint = row.get("saint_name") or row.get("saint") or row.get("class")
                records.append(self._make(img, saint, row))
        return records

    def _from_dirs(self, base: str) -> List[RawRecord]:
        records: List[RawRecord] = []
        for saint in sorted(os.listdir(base)):
            class_dir = os.path.join(base, saint)
            if not os.path.isdir(class_dir):
                continue
            for fname in sorted(os.listdir(class_dir)):
                if os.path.splitext(fname)[1].lower() not in _IMAGE_EXTS:
                    continue
                records.append(self._make(os.path.join(class_dir, fname), saint, {}))
        return records

    def _make(self, img_path: str, saint: Optional[str], raw: dict) -> RawRecord:
        rel = os.path.relpath(img_path, self._cfg.dataset_path)
        return RawRecord(
            source=self.name,
            source_record_id=rel,                       # stable id within the dataset
            title=os.path.splitext(os.path.basename(img_path))[0].replace("_", " "),
            saint_name=saint,
            description=None,
            local_source_path=img_path,
            license_signal={},                          # source-level grant
            raw=raw,
        )


def _strip_html(value: Optional[str]) -> Optional[str]:
    """Commons ``extmetadata`` often wraps artist/desc in HTML; flatten it."""
    if not value:
        return None
    import re
    text = re.sub(r"<[^>]+>", " ", value)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def build_adapters(sources: Dict[str, IconSourceConfig], http: _HttpJson) -> List[SourceAdapter]:
    """Instantiate the enabled adapters in a stable order."""
    adapters: List[SourceAdapter] = []
    for name in ("met_api", "wikimedia", "iconsaint"):
        cfg = sources.get(name)
        if not cfg or not cfg.enabled:
            continue
        if name == "met_api":
            adapters.append(MetAdapter(cfg, http))
        elif name == "wikimedia":
            adapters.append(WikimediaAdapter(cfg, http))
        elif name == "iconsaint":
            adapters.append(IconsaintAdapter(cfg))
    return adapters
