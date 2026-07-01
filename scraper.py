"""Scraper orchestration: discover category members, then crawl the stale ones."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set
from urllib.parse import quote

from config import Config
from content_store import content_path, write_bytes, write_sidecar
from storage import Storage, MediaRecord
from licenses import detect_licenses, redistribution_level, best_license_name
from mediawiki import BATCH_SIZE, MediaWikiClient

log = logging.getLogger("ortho_scraper.scraper")


def _article_url(api_url: str, title: str) -> str:
    """Best-effort article URL from the API endpoint + page title.

    Used for pages we only mark as seen; crawled pages get the authoritative
    fullurl from the API.
    """
    base = api_url.rsplit("/", 1)[0]  # strip "/api.php"
    return f"{base}/{quote(title.replace(' ', '_'))}"


def _fmt_bytes(n: int) -> str:
    f = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if f < 1024 or unit == "GB":
            return f"{f:.0f} {unit}" if unit == "B" else f"{f:.1f} {unit}"
        f /= 1024
    return f"{f:.1f} GB"


def _fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


@dataclass
class RunStats:
    """Cumulative counters + timing for a single crawl run."""
    started: float = field(default_factory=time.monotonic)
    categories: int = 0
    discovered: int = 0
    new: int = 0
    changed: int = 0
    forced: int = 0
    unchanged: int = 0
    to_crawl: int = 0
    crawled: int = 0
    content_bytes: int = 0
    contributors: int = 0
    media_downloaded: int = 0
    media_deduped: int = 0
    media_skipped: int = 0
    media_failed: int = 0
    deleted: int = 0
    kept: int = 0

    def elapsed(self) -> float:
        return time.monotonic() - self.started


class Scraper:
    def __init__(self, config: Config, client: MediaWikiClient, db: Storage):
        self._cfg = config
        self._client = client
        self._db = db
        self._stats = RunStats()
        self._progress = {"done": 0, "total": 0}

    async def discover(self) -> Dict[int, dict]:
        """Walk the configured categories (and optionally subcategories).

        Returns {pageid: {title, namespace, url, roots}} where ``roots`` is the
        set of *configured* root categories the page descends from (pages reached
        via subcategory recursion inherit the root they came from). ``roots``
        drives both the seen-placeholder categories and the media policy.
        """
        cfg = self._cfg.scraper
        pages: Dict[int, dict] = {}
        visited_cats: Set[str] = set()

        # Queue of (category_title, depth, root_category).
        queue: List[tuple] = [(c, 0, c) for c in cfg.categories]

        processed_cats = 0
        while queue:
            category, depth, root = queue.pop(0)
            if category in visited_cats:
                continue
            visited_cats.add(category)
            processed_cats += 1

            members = subcats = new_here = 0
            async for member in self._client.iter_category_members(category):
                if member.is_subcat:
                    subcats += 1
                    if cfg.recurse_subcategories and depth < cfg.max_subcategory_depth:
                        queue.append((member.title, depth + 1, root))
                    continue
                members += 1
                rec = pages.get(member.pageid)
                if rec is None:
                    new_here += 1
                    rec = {
                        "pageid": member.pageid,
                        "title": member.title,
                        "namespace": member.ns,
                        "url": _article_url(cfg.api_url, member.title),
                        "roots": set(),
                    }
                    pages[member.pageid] = rec
                rec["roots"].add(root)

            log.info("  [cat %d] %r (depth %d, root %r): %d pages (%d new), %d subcats"
                     " | unique so far: %d | %d categories queued",
                     processed_cats, category, depth, root, members, new_here, subcats,
                     len(pages), len(queue))

        log.info("Discovery done: %d unique pages across %d categories (%s).",
                 len(pages), len(visited_cats), _fmt_duration(self._stats.elapsed()))
        self._stats.categories = len(visited_cats)
        self._stats.discovered = len(pages)
        return pages

    async def _select_to_crawl(self, pages: Dict[int, dict], now: datetime) -> List[int]:
        """Decide which discovered pages need a (re)crawl.

        A page is crawled if it was never crawled, or its current lastrevid
        differs from the revision we stored (the content actually changed). As a
        fallback, ``recrawl_after`` (when > 0) forces a refresh of unchanged
        pages — useful for catching re-uploaded media, which doesn't bump the
        page's revid.
        """
        state = await self._db.get_crawl_state(pages.keys())

        never_crawled = [pid for pid in pages
                         if not state.get(pid) or state[pid]["last_crawled"] is None]
        # Cheap metadata probe only for pages we might otherwise skip.
        known = [pid for pid in pages if pid not in set(never_crawled)]
        log.info("Change check: %d never crawled; probing lastrevid for %d known page(s)...",
                 len(never_crawled), len(known))
        current_revids = await self._client.fetch_latest_revids(known)

        recrawl_after = self._cfg.scraper.recrawl_after
        forced_refresh = recrawl_after > timedelta(0)

        to_crawl = list(never_crawled)
        changed = stale = 0
        for pid in known:
            st = state[pid]
            cur = current_revids.get(pid)
            if cur is not None and cur != st["revid"]:
                to_crawl.append(pid)
                changed += 1
            elif forced_refresh and (now - st["last_crawled"]) >= recrawl_after:
                to_crawl.append(pid)
                stale += 1
        unchanged = len(pages) - len(to_crawl)
        self._stats.new = len(never_crawled)
        self._stats.changed = changed
        self._stats.forced = stale
        self._stats.unchanged = unchanged
        self._stats.to_crawl = len(to_crawl)
        log.info("Crawl selection: %d to crawl (%d new, %d changed, %d forced-refresh) "
                 "| %d unchanged, skipped.",
                 len(to_crawl), len(never_crawled), changed, stale, unchanged)
        return to_crawl

    async def run(self) -> None:
        # Captured before mark_seen so we can tell which existing rows were NOT
        # observed this run (their last_seen will predate this timestamp).
        run_started = datetime.now(timezone.utc)
        self._stats = RunStats()
        self._progress = {"done": 0, "total": 0}

        cfg = self._cfg.scraper
        log.info("=" * 64)
        log.info("Crawl run starting | site=%s | categories=%s | media=%s",
                 cfg.api_url, ", ".join(cfg.categories) or "(none)",
                 "on" if cfg.media.enabled else "off")
        log.info("=" * 64)

        log.info("Phase 1/4: discovering category members...")
        pages = await self.discover()
        if not pages:
            log.warning("No pages discovered; nothing to do.")
            return

        log.info("Phase 2/4: marking %d page(s) as seen...", len(pages))
        seen_payload = [
            {"pageid": rec["pageid"], "title": rec["title"], "url": rec["url"],
             "namespace": rec["namespace"], "categories": sorted(rec["roots"])}
            for rec in pages.values()
        ]
        await self._db.mark_seen(seen_payload)

        now = datetime.now(timezone.utc)
        log.info("Phase 3/4: selecting pages to crawl...")
        to_crawl = await self._select_to_crawl(pages, now)
        self._progress["total"] = len(to_crawl)

        if to_crawl:
            batches = [to_crawl[i:i + BATCH_SIZE] for i in range(0, len(to_crawl), BATCH_SIZE)]
            log.info("Phase 4/4: crawling %d page(s) in %d batch(es) of up to %d...",
                     len(to_crawl), len(batches), BATCH_SIZE)
            # Concurrent batches; the RateLimiter bounds actual request rate.
            await asyncio.gather(*(self._crawl_batch(batch, pages) for batch in batches))
        else:
            log.info("Phase 4/4: nothing to crawl — everything is up to date.")

        if self._cfg.scraper.reconcile_deletions:
            await self._reconcile_deletions(run_started)

        self._log_summary()

    def _log_summary(self) -> None:
        s = self._stats
        cs = self._client.stats
        elapsed = s.elapsed()
        rate = s.crawled / elapsed if elapsed > 0 else 0.0
        log.info("=" * 64)
        log.info("Run complete in %s", _fmt_duration(elapsed))
        log.info("  Discovery : %d pages across %d categories", s.discovered, s.categories)
        log.info("  Selection : %d crawled (%d new, %d changed, %d forced) | %d unchanged",
                 s.crawled, s.new, s.changed, s.forced, s.unchanged)
        log.info("  Content   : %s of wikitext | %d contributors recorded | %.1f pages/s",
                 _fmt_bytes(s.content_bytes), s.contributors, rate)
        log.info("  Media     : %d downloaded (%s), %d already-on-disk, %d skipped, %d failed",
                 s.media_downloaded, _fmt_bytes(cs.download_bytes),
                 s.media_deduped, s.media_skipped, s.media_failed)
        log.info("  Deletions : %d soft-removed, %d still exist", s.deleted, s.kept)
        log.info("  HTTP      : %d API requests, %d downloads, %d total attempts, %d retries",
                 cs.api_requests, cs.downloads, cs.http_attempts, cs.retries)
        log.info("=" * 64)

    async def _reconcile_deletions(self, run_started: datetime) -> None:
        """Soft-remove tracked pages the wiki confirms no longer exist.

        Pages we have on file but did not observe this run may have been deleted
        or merely recategorized. We verify with a cheap existence probe and only
        stamp removed_at on the ones the API reports as gone.
        """
        candidates = await self._db.get_unseen_active(run_started)
        if not candidates:
            log.info("Reconcile: no previously-tracked pages went missing this run.")
            return
        log.info("Reconcile: probing %d page(s) not seen this run for deletion...",
                 len(candidates))

        existing = set()
        batches = [candidates[i:i + BATCH_SIZE] for i in range(0, len(candidates), BATCH_SIZE)]
        results = await asyncio.gather(*(self._client.check_existing(b) for b in batches))
        for r in results:
            existing |= r

        gone = [pid for pid in candidates if pid not in existing]
        if gone:
            await self._db.mark_removed(gone, datetime.now(timezone.utc))
        self._stats.deleted = len(gone)
        self._stats.kept = len(candidates) - len(gone)
        log.info("Reconcile: %d deleted (soft-removed), %d still exist (kept).",
                 len(gone), len(candidates) - len(gone))

    async def _crawl_batch(self, pageids: List[int], discovered: Dict[int, dict]) -> None:
        contents = await self._client.fetch_pages(pageids)

        media_by_page: Dict[int, List[str]] = {}
        if self._cfg.scraper.media.enabled:
            media_by_page = await self._collect_media(contents, discovered)

        contributors_by_page: Dict[int, List[str]] = {}
        if self._cfg.scraper.attribution.fetch_contributors:
            contributors_by_page = await self._client.fetch_contributors(
                [p.pageid for p in contents])

        for page in contents:
            page.contributors = contributors_by_page.get(page.pageid, [])
            page.attribution = self._build_attribution(page)
            media_paths = media_by_page.get(page.pageid, [])
            await self._db.store_page(page, page.categories, media_paths)

            chars = len(page.content) if page.content else 0
            self._stats.crawled += 1
            self._stats.content_bytes += chars
            self._stats.contributors += len(page.contributors)
            self._log_page_progress(page, chars, len(media_paths), len(page.contributors))

    def _log_page_progress(self, page, chars: int, n_media: int, n_authors: int) -> None:
        p = self._progress
        p["done"] += 1
        done, total = p["done"], p["total"]
        pct = (100.0 * done / total) if total else 100.0
        elapsed = self._stats.elapsed()
        rate = done / elapsed if elapsed > 0 else 0.0
        eta = (total - done) / rate if rate > 0 else 0.0
        log.info("[%d/%d %4.1f%% ETA %s] %s (rev %s, %s, %d media, %d authors)",
                 done, total, pct, _fmt_duration(eta), page.title, page.revid,
                 _fmt_bytes(chars), n_media, n_authors)

    def _build_attribution(self, page) -> str:
        """Compose a ready-to-use BY-SA / GFDL credit line for a page."""
        a = self._cfg.scraper.attribution
        base = self._cfg.scraper.api_url.rsplit("/", 1)[0]  # strip "/api.php"
        title_us = quote(page.title.replace(" ", "_"))
        permalink = f"{base}/index.php?oldid={page.revid}" if page.revid else page.url
        history = f"{base}/index.php?title={title_us}&action=history"
        authors = ", ".join(page.contributors) if page.contributors \
            else f"{a.site_name} contributors"
        return (
            f'"{page.title}" by {authors}. '
            f'Source: {page.url} (revision {page.revid}, permalink: {permalink}; '
            f'full history: {history}). '
            f'{a.site_name}, licensed under {a.license_name} <{a.license_url}> '
            f'and {a.additional_license}.'
        )

    async def _collect_media(self, contents: List, discovered: Dict[int, dict]) -> Dict[int, List[str]]:
        """Download the policy-allowed media for crawled pages.

        Returns {pageid: [local file paths]}. Each page's allowed media classes
        come from the media policy of the configured categories it was found
        under. Files are downloaded once (content-addressed by sha1); a file
        shared across pages resolves to the same path for each.
        """
        media_cfg = self._cfg.scraper.media

        # Effective allowed media classes per page id.
        wanted: Dict[int, set] = {}
        for page in contents:
            rec = discovered.get(page.pageid)
            roots = rec["roots"] if rec else set()
            classes = media_cfg.effective_types(roots)
            if classes:
                wanted[page.pageid] = classes
        if not wanted:
            return {}

        # One images query for the whole batch, then resolve lightweight metadata.
        images_map = await self._client.fetch_page_images(list(wanted.keys()))
        all_titles = {t for titles in images_map.values() for t in titles}
        if not all_titles:
            return {}
        info = await self._client.fetch_imageinfo(sorted(all_titles))

        # Only the files that pass the per-page class filter are worth the heavy
        # description-wikitext fetch (for license detection / sidecars).
        needed: Set[str] = set()
        for pageid, classes in wanted.items():
            for title in images_map.get(pageid, []):
                meta = info.get(title)
                if meta and ("all" in classes or meta.media_class in classes):
                    needed.add(title)
        if needed:
            descriptions = await self._client.fetch_file_descriptions(sorted(needed))
            for title, wikitext in descriptions.items():
                if title in info:
                    info[title].description_wikitext = wikitext

        result: Dict[int, List[str]] = {}
        for pageid, classes in wanted.items():
            paths: List[str] = []
            for title in images_map.get(pageid, []):
                meta = info.get(title)
                if meta is None:
                    continue
                if "all" not in classes and meta.media_class not in classes:
                    continue
                local_path = await self._download_media(meta)
                if local_path:
                    paths.append(local_path)
            if paths:
                result[pageid] = paths
        return result

    async def _download_media(self, meta) -> Optional[str]:
        """Download a file to content-addressed storage; return its local path.

        Returns None if the file exceeds max_file_size, lacks a URL, or the
        download fails. Files are deduplicated by sha1, so a file shared across
        pages is fetched at most once.
        """
        media_cfg = self._cfg.scraper.media
        max_size = media_cfg.max_file_size
        if not meta.url:
            self._stats.media_skipped += 1
            return None
        if max_size and meta.size and meta.size > max_size:
            log.debug("Skipping oversize media %r (%s > %s)", meta.title,
                      _fmt_bytes(meta.size), _fmt_bytes(max_size))
            self._stats.media_skipped += 1
            return None

        dest = self._media_path(meta)
        if os.path.exists(dest):
            # Already have these exact bytes; just refresh the record/sidecar.
            self._stats.media_deduped += 1
            await self._record_media(dest, meta)
            return dest

        try:
            data = await self._client.download_file(meta.url)
        except Exception as exc:  # noqa: BLE001 - one bad file shouldn't kill the run
            log.warning("Failed to download %r: %s", meta.title, exc)
            self._stats.media_failed += 1
            return None

        await asyncio.to_thread(write_bytes, dest, data)
        await self._record_media(dest, meta)
        self._stats.media_downloaded += 1
        log.debug("Downloaded %r -> %s (%s)", meta.title, dest, _fmt_bytes(len(data)))
        return dest

    async def _record_media(self, dest: str, meta) -> None:
        """Persist a media file's DB row (with redistribution level) + JSON sidecar."""
        licenses = detect_licenses(meta.description_wikitext or "")
        level = redistribution_level(licenses)
        license_name = best_license_name(licenses)

        await self._db.store_media(MediaRecord(
            media_id=self._media_id(meta),
            title=meta.title,
            local_path=dest,
            mime=meta.mime,
            source_url=meta.url,
            license_name=license_name,
            redistribution=level,
        ))

        a = self._cfg.scraper.attribution
        record = {
            "title": meta.title,
            "source_url": meta.url,
            "description_page": meta.descriptionurl,
            "uploader": meta.uploader,
            "sha1": meta.sha1,
            "mime": meta.mime,
            "media_type": meta.media_class,
            "site": a.site_name,
            # How freely this file may be shared (public/free/restricted/prohibited).
            "redistribution": level,
            # Normalized license(s) parsed from the File: page's license templates.
            # Empty if none recognized — check description_wikitext / description_page.
            "licenses": licenses,
            "license_note": a.image_license_note,
            "description_wikitext": meta.description_wikitext,
        }
        await asyncio.to_thread(write_sidecar, dest + ".json", record)

    def _media_id(self, meta) -> str:
        """Content identity: the file's sha1, or a url hash when sha1 is absent."""
        return meta.sha1 or hashlib.sha1(meta.url.encode()).hexdigest()

    def _media_path(self, meta) -> str:
        """Content-addressed path: <download_dir>/<media_id[:2]>/<media_id><ext>."""
        mid = self._media_id(meta)
        ext = os.path.splitext(meta.title)[1].lower()
        return content_path(self._cfg.scraper.media.download_dir, mid, ext)
