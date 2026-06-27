#!/usr/bin/env python3
"""Entry point for the OrthodoxWiki scraper.

Usage:
    python3 main.py --config scraper.conf
    python3 main.py --config scraper.conf --loop 6h   # run forever, sleeping between passes
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

import aiohttp

from config import load_config, parse_duration, select_policy
from storage import create_storage
from mediawiki import MediaWikiClient
from ratelimit import RateLimiter
from scraper import Scraper
from icon_pipeline import IconPipeline
from notifications import run_daily_notifications
from icon_sources import _HttpJson
from saint_sources import (ORTHODOXWIKI_LICENSE, WIKIPEDIA_LICENSE,
                           build_name_index, clean_wikitext_lead, fetch_saint_records,
                           normalize_name, resolve_qid)

log = logging.getLogger("ortho_scraper")


async def run_wiki(config, db) -> None:
    """One pass of the OrthodoxWiki category scraper."""
    s = config.scraper
    timeout = aiohttp.ClientTimeout(total=s.http.timeout)
    headers = {"User-Agent": s.user_agent}
    limiter = RateLimiter(
        requests_per_second=s.rate_limit.requests_per_second,
        burst=s.rate_limit.burst,
        max_concurrency=s.rate_limit.max_concurrency,
    )
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        client = MediaWikiClient(
            session=session,
            api_url=s.api_url,
            limiter=limiter,
            max_retries=s.http.max_retries,
            retry_backoff=s.http.retry_backoff,
        )
        scraper = Scraper(config, client, db)
        await scraper.run()


async def run_icons(config, db) -> None:
    """One pass of the licensed Icon & Saints ingestion pipeline."""
    ic = config.icons
    if not ic.enabled:
        log.warning("icons.enabled is false in the config; nothing to ingest.")
        return
    timeout = aiohttp.ClientTimeout(total=ic.http.timeout)
    # Reuse the wiki scraper's User-Agent for a polite, identifying contact.
    headers = {"User-Agent": config.scraper.user_agent}
    limiter = RateLimiter(
        requests_per_second=ic.rate_limit.requests_per_second,
        burst=ic.rate_limit.burst,
        max_concurrency=ic.rate_limit.max_concurrency,
    )
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        pipeline = IconPipeline(config, session, db).with_limiter(limiter)
        await pipeline.run()


async def run_notify(config, db) -> None:
    """One pass of the daily notification job (no HTTP needed)."""
    await run_daily_notifications(db)


async def run_saints(config, db) -> None:
    """One pass of saint ingestion from Wikipedia: identity (QID) + licensed bio.

    Each saint resolves to a Wikidata QID and lands a CC BY-SA bio claim in the
    ledger; ``recompute_saint`` then materializes the servable saints.* columns.
    """
    sc = config.saints
    if not sc.enabled:
        log.warning("saints.enabled is false in the config; nothing to ingest.")
        return
    headers = {"User-Agent": config.scraper.user_agent}
    limiter = RateLimiter(requests_per_second=1.0, burst=2, max_concurrency=1)

    # Wikipedia is a blanket CC BY-SA source (no per-item licensing); register it
    # so bio claims carry a source_id.
    src = await db.upsert_source(
        name="wikipedia", base_license=WIKIPEDIA_LICENSE,
        attribution_template=None, requires_per_item_check=False,
        notes="English Wikipedia saint articles (text spine).")

    # A per-type policy can widen/withhold the bio license (e.g. reject during an
    # enwiki license dispute) without touching code.
    policy = select_policy(config.license_policies, "saint", "wikipedia", "bio")

    seen = bio = aliases = feasts = needs_review = 0
    async with aiohttp.ClientSession(headers=headers) as session:
        async for rec in fetch_saint_records(sc, session, limiter):
            seen += 1
            if rec.qid:
                saint_id = await db.upsert_saint_by_qid(rec.qid, rec.display_name)
            else:
                needs_review += 1
                saint_id = await db.upsert_saint(rec.display_name)  # qid stays NULL
            if rec.bio_text:
                lic, attr = rec.license, rec.attribution
                if policy and policy.decision == "rejected":
                    lic = None                       # stored for audit, not servable
                elif policy and policy.decision == "approved":
                    lic = policy.license or rec.license
                    attr = policy.attribution or rec.attribution
                await db.add_claim(saint_id, "bio", rec.bio_text, src.source_id,
                                   sc.wikipedia_weight, lic, attr)
                bio += 1
            if rec.qid:
                # Replace this source's alt-name set (empty list clears stale ones).
                await db.set_claims(saint_id, "alt_names", rec.alt_names,
                                    src.source_id, sc.wikipedia_weight, None, None)
                aliases += len(rec.alt_names)
            if rec.feast_day:
                # A feast date is an uncopyrightable fact -> no license needed.
                await db.add_claim(saint_id, "feast_day", rec.feast_day,
                                   src.source_id, sc.wikipedia_weight, None, None)
                feasts += 1
            await db.recompute_saint(saint_id)

    log.info("[saints] Wikipedia: %d processed | %d bio, %d alias(es), %d feast day(s), "
             "%d needs-review (no QID).", seen, bio, aliases, feasts, needs_review)

    # --- OrthodoxWiki enrichment (no HTTP: reuses crawled `pages`) -----------
    # Match crawled pages to existing saints by name/alias and emit a lower-weight
    # bio claim. Enrichment only — it never seeds saints (Wikipedia is the spine).
    if sc.orthodoxwiki_weight > 0:
        await _enrich_from_orthodoxwiki(config, db)

    total, with_bio = await db.saint_bio_coverage()
    log.info("[saints] coverage: %d/%d saints have a servable bio.", with_bio, total)


async def _enrich_from_orthodoxwiki(config, db) -> None:
    sc = config.saints
    owiki = await db.upsert_source(
        name="orthodoxwiki", base_license=ORTHODOXWIKI_LICENSE,
        attribution_template=None, requires_per_item_check=False,
        notes="OrthodoxWiki saint pages (bio enrichment, below Wikipedia).")
    saints = await db.all_saint_names()
    name_index = build_name_index(saints)
    # QID match is high-precision: a page's resolved QID only links if it equals a
    # saint we already seeded from Wikipedia, so a wrong resolve simply won't match.
    qid_index = {s["qid"]: s["id"] for s in saints if s.get("qid")}
    pages = await db.fetch_saint_candidate_pages()

    headers = {"User-Agent": config.scraper.user_agent}
    limiter = RateLimiter(requests_per_second=1.0, burst=2, max_concurrency=1)
    matched = by_name = by_qid = 0
    async with aiohttp.ClientSession(headers=headers) as session:
        http = _HttpJson(session, limiter)
        for p in pages:
            saint_id = name_index.get(normalize_name(p["title"]))
            via = "name"
            if saint_id is None and qid_index:
                # Name miss: resolve the page title to a QID and match by it.
                # ponytail: one resolve per unmatched page; cache by title if a
                # corpus has many same-titled pages (it won't).
                try:
                    qid = await resolve_qid(p["title"], http)
                except Exception as exc:  # noqa: BLE001 - a resolve hiccup skips one page
                    log.debug("QID resolve failed for %r: %s", p["title"], exc)
                    qid = None
                if qid:
                    saint_id = qid_index.get(qid)
                    via = "qid"
            if saint_id is None:
                continue
            lead = clean_wikitext_lead(p["content"])
            if not lead:
                continue
            attribution = p.get("attribution") or f"OrthodoxWiki, {ORTHODOXWIKI_LICENSE}"
            await db.add_claim(saint_id, "bio", lead, owiki.source_id,
                               sc.orthodoxwiki_weight, ORTHODOXWIKI_LICENSE, attribution)
            await db.recompute_saint(saint_id)
            matched += 1
            if via == "name":
                by_name += 1
            else:
                by_qid += 1
    log.info("[saints] OrthodoxWiki: %d matched (%d by name, %d by QID) of %d crawled.",
             matched, by_name, by_qid, len(pages))


async def run_stats(config, db) -> None:
    """Print a unified coverage report (read-only) — the saints/icons visibility."""
    c = await db.coverage()
    log.info("=" * 64)
    log.info("Coverage report")
    log.info("  Saints : %d total | %d with servable bio | %d with feast day | %d needs-review (no QID)",
             c["saints_total"], c["saints_with_bio"], c["saints_with_feast"],
             c["saints_needs_review"])
    log.info("  Icons  : %d total | %d approved | %d linked to a saint | %d orphan",
             c["icons_total"], c["icons_approved"], c["icons_linked"], c["icons_orphan"])
    log.info("  Claims : %d in the ledger", c["claims_total"])
    log.info("=" * 64)


# Canonical order: scrape, then ingest icons, then saints, then notify, then stats.
_MODES = {"wiki": run_wiki, "icons": run_icons, "saints": run_saints,
          "notify": run_notify, "stats": run_stats}


def resolve_modes(selected) -> list:
    """Expand 'all', dedupe, and force canonical order. None -> ['wiki']."""
    if not selected:
        return ["wiki"]
    if "all" in selected:
        # 'stats' is a read-only report, not part of an ingest run.
        return [m for m in _MODES if m != "stats"]
    return [m for m in _MODES if m in selected]


async def run_once(config, modes: list) -> None:
    db = await create_storage(config.database)
    try:
        await db.apply_schema()
        for mode in modes:
            await _MODES[mode](config, db)
    finally:
        await db.close()


async def main_async(args) -> int:
    config = load_config(args.config)
    modes = resolve_modes(args.mode)
    label = "+".join(modes)

    if args.loop:
        interval = parse_duration(args.loop).total_seconds()
        log.info("Running in loop mode (%s); %.0fs between passes.", label, interval)
        run_no = 0
        while True:
            run_no += 1
            log.info("---- Loop pass #%d (%s) ----", run_no, label)
            try:
                await run_once(config, modes)
            except Exception:  # noqa: BLE001 - keep the loop alive across failures
                log.exception("Pass #%d failed; will retry next interval.", run_no)
            log.info("Pass #%d done; sleeping %.0fs until next pass.", run_no, interval)
            await asyncio.sleep(interval)
    else:
        await run_once(config, modes)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="OrthodoxWiki category scraper")
    parser.add_argument("-c", "--config", default="scraper.conf",
                        help="Path to the HOCON config file (default: scraper.conf)")
    parser.add_argument("--mode", action="append", choices=[*_MODES, "all"],
                        help="What to run; repeatable (e.g. --mode wiki --mode icons). "
                             "'wiki' (OrthodoxWiki scraper, default), 'icons' (licensed "
                             "icon/saints ingestion), 'saints' (Wikipedia saint-name "
                             "roster, no icons/licensing), 'notify' (daily follower "
                             "notifications), or 'all'. Runs in order: wiki, icons, "
                             "saints, notify.")
    parser.add_argument("--loop", metavar="DURATION", default=None,
                        help="Run continuously, sleeping this long between passes "
                             "(e.g. '6h', '30 minutes'). Default: run once and exit.")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable debug logging.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    # ponytail: even under -v, the chatty third-party loggers drown our own
    # output; pin them to WARNING so the run's progress stays readable.
    for noisy in ("aiosqlite", "asyncio", "aiohttp", "asyncpg"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        log.info("Interrupted; exiting.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
