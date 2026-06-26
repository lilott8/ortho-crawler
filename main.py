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

from config import load_config, parse_duration
from storage import create_storage
from mediawiki import MediaWikiClient
from ratelimit import RateLimiter
from scraper import Scraper
from icon_pipeline import IconPipeline
from notifications import run_daily_notifications
from saint_sources import fetch_saint_names

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
    """One pass of saint-roster ingestion from a Wikipedia list article."""
    sc = config.saints
    if not sc.enabled:
        log.warning("saints.enabled is false in the config; nothing to ingest.")
        return
    headers = {"User-Agent": config.scraper.user_agent}
    limiter = RateLimiter(requests_per_second=1.0, burst=2, max_concurrency=1)
    count = 0
    async with aiohttp.ClientSession(headers=headers) as session:
        async for canonical_name, _display in fetch_saint_names(sc, session, limiter):
            await db.upsert_saint(canonical_name)
            count += 1
    log.info("[saints] upserted %d name(s).", count)


# Canonical order: scrape, then ingest icons, then saints, then notify.
_MODES = {"wiki": run_wiki, "icons": run_icons, "saints": run_saints, "notify": run_notify}


def resolve_modes(selected) -> list:
    """Expand 'all', dedupe, and force canonical order. None -> ['wiki']."""
    if not selected:
        return ["wiki"]
    if "all" in selected:
        return list(_MODES)
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
