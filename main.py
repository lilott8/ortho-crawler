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
import time
from dataclasses import dataclass, field

import aiohttp

from config import load_config, parse_duration, select_policy
from storage import CURATED_WEIGHT, create_storage
from mediawiki import MediaWikiClient
from ratelimit import RateLimiter, TokenBucket
from scraper import Scraper
from icon_pipeline import IconPipeline
from notifications import run_daily_notifications
from icon_sources import _HttpJson
from saint_sources import (ORTHODOXWIKI_LICENSE, WIKIPEDIA_LICENSE,
                           build_name_index, clean_wikitext_lead, fetch_saint_records,
                           normalize_name, resolve_qid)

log = logging.getLogger("ortho_scraper")

# Cap on stubs emitted by `--mode review` (enforces "high-value few" curation).
REVIEW_CAP = 50


@dataclass
class SaintRunStats:
    """Per-run counters for saint ingestion (drives the progress + summary logs)."""
    started: float = field(default_factory=time.monotonic)
    seen: int = 0
    resolved: int = 0           # had a QID (incl. via correction)
    needs_review: int = 0       # qid IS NULL
    rescued: int = 0            # QID supplied by a saint_qid correction
    bio: int = 0
    aliases: int = 0
    feasts: int = 0
    descriptions: int = 0
    feast_corrections: int = 0
    owiki_bio: int = 0

    def elapsed(self) -> float:
        return time.monotonic() - self.started


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
    # One limiter per source so a tight budget (WikiArt 400/hr) never throttles
    # the others; a multi-bucket limiter enforces requests/sec AND requests/hour.
    source_limiters = {name: _build_limiter(sc) for name, sc in ic.sources.items()}
    default_limiter = _build_limiter_from(ic.rate_limit, 0)
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        pipeline = IconPipeline(config, session, db).with_limiters(
            source_limiters, default_limiter)
        await pipeline.run()


def _build_limiter(sc) -> RateLimiter:
    return _build_limiter_from(sc.rate_limit, sc.hourly_cap)


def _build_limiter_from(rl, hourly_cap: int) -> RateLimiter:
    buckets = [TokenBucket(rl.requests_per_second, rl.burst)]
    if hourly_cap > 0:
        # Second bucket: ~cap/3600 per second, capacity = the hourly cap itself.
        buckets.append(TokenBucket(hourly_cap / 3600.0, hourly_cap))
    return RateLimiter(buckets=buckets, max_concurrency=rl.max_concurrency)


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
    corr = config.corrections
    headers = {"User-Agent": config.scraper.user_agent}
    rl = sc.rate_limit
    limiter = RateLimiter(requests_per_second=rl.requests_per_second, burst=rl.burst,
                          max_concurrency=rl.max_concurrency)

    # Wikipedia is a blanket CC BY-SA source (no per-item licensing); register it
    # so bio claims carry a source_id. `curated` carries operator corrections.
    src = await db.upsert_source(
        name="wikipedia", base_license=WIKIPEDIA_LICENSE,
        attribution_template=None, requires_per_item_check=False,
        notes="English Wikipedia / Wikidata saint data (text spine).")
    curated = await db.upsert_source(
        name="curated", base_license="curated", attribution_template=None,
        requires_per_item_check=False, notes="Operator corrections (config).")

    # A per-type policy can widen/withhold the bio license (e.g. reject during an
    # enwiki license dispute) without touching code.
    policy = select_policy(config.license_policies, "saint", "wikipedia", "bio")

    stats = SaintRunStats()
    log.info("=" * 64)
    log.info("Saint ingest starting | roster: %s (max %d) | corrections: %d qid, "
             "%d feast, %d owiki", ", ".join(sc.wikipedia_articles), sc.max_records,
             len(corr.saint_qid), len(corr.feast), len(corr.owiki_qid))
    log.info("=" * 64)

    async with aiohttp.ClientSession(headers=headers) as session:
        async for rec in fetch_saint_records(sc, session, limiter,
                                             qid_overrides=corr.saint_qid):
            stats.seen += 1
            try:
                if rec.qid:
                    stats.resolved += 1
                    if rec.qid_from_correction:
                        stats.rescued += 1
                    saint_id = await db.upsert_saint_by_qid(rec.qid, rec.display_name)
                else:
                    stats.needs_review += 1
                    saint_id = await db.upsert_saint(rec.display_name)  # qid stays NULL
                if rec.bio_text:
                    lic, attr = rec.license, rec.attribution
                    if policy and policy.decision == "rejected":
                        lic = None                   # stored for audit, not servable
                    elif policy and policy.decision == "approved":
                        lic = policy.license or rec.license
                        attr = policy.attribution or rec.attribution
                    await db.add_claim(saint_id, "bio", rec.bio_text, src.source_id,
                                       sc.wikipedia_weight, lic, attr)
                    stats.bio += 1
                if rec.qid:
                    # Replace this source's sets (empty list clears stale values).
                    # alt_names/feast_days/description are core-data facts -> no license
                    # (description carries CC0 for provenance).
                    await db.set_claims(saint_id, "alt_names", rec.alt_names,
                                        src.source_id, sc.wikipedia_weight, None, None)
                    await db.set_claims(saint_id, "feast_day", rec.feast_days,
                                        src.source_id, sc.wikipedia_weight, None, None)
                    stats.aliases += len(rec.alt_names)
                    stats.feasts += len(rec.feast_days)
                    if rec.description:
                        await db.add_claim(saint_id, "description", rec.description,
                                           src.source_id, sc.wikipedia_weight, "CC0", None)
                        stats.descriptions += 1
                await db.recompute_saint(saint_id)
                log.info("[saints %d] %-40s | qid=%-9s bio=%s feast=%d alias=%d desc=%s%s",
                         stats.seen, rec.display_name[:40], rec.qid or "—",
                         "y" if rec.bio_text else "—", len(rec.feast_days),
                         len(rec.alt_names), "y" if rec.description else "—",
                         "  [rescued]" if rec.qid_from_correction else "")
            except Exception as exc:  # noqa: BLE001 - one bad saint must not sink the run
                log.warning("[saints] skipping %r after error: %s", rec.display_name, exc)

    log.info("[saints] Wikipedia phase: %d processed | %d bio, %d alias, %d feast, "
             "%d desc | %d needs-review (%d rescued by correction).",
             stats.seen, stats.bio, stats.aliases, stats.feasts, stats.descriptions,
             stats.needs_review, stats.rescued)

    await _apply_feast_corrections(db, corr, curated.source_id, stats)

    # --- OrthodoxWiki enrichment (no HTTP unless a name-miss needs a QID) -----
    # Match crawled pages to existing saints and emit a lower-weight bio claim.
    # Enrichment only — it never seeds saints (Wikipedia is the spine).
    if sc.orthodoxwiki_weight > 0:
        await _enrich_from_orthodoxwiki(config, db, stats)

    await _log_saint_summary(db, stats)


async def _apply_feast_corrections(db, corr, curated_source_id, stats) -> None:
    """Curated feast days win the reducer outright (CURATED_WEIGHT)."""
    for qid, days in corr.feast.items():
        saint_id = await db.get_saint_id_by_qid(qid)
        if saint_id is None:
            log.warning("[saints] feast correction for QID %s skipped — no such "
                        "seeded saint.", qid)
            continue
        await db.set_claims(saint_id, "feast_day", days, curated_source_id,
                            CURATED_WEIGHT, None, None)
        await db.recompute_saint(saint_id)
        stats.feast_corrections += 1
    if corr.feast:
        log.info("[saints] applied %d/%d feast correction(s).",
                 stats.feast_corrections, len(corr.feast))


async def _log_saint_summary(db, stats: SaintRunStats) -> None:
    total, with_bio = await db.saint_bio_coverage()
    log.info("=" * 64)
    log.info("Saint ingest complete in %.1fs", stats.elapsed())
    log.info("  Processed : %d saints (%d resolved, %d needs-review)",
             stats.seen, stats.resolved, stats.needs_review)
    log.info("  Claims    : %d bio (+%d OrthodoxWiki), %d alias, %d feast, %d desc",
             stats.bio, stats.owiki_bio, stats.aliases, stats.feasts, stats.descriptions)
    log.info("  Corrections: %d rescued QIDs, %d feast", stats.rescued,
             stats.feast_corrections)
    log.info("  Coverage  : %d/%d saints have a servable bio", with_bio, total)
    log.info("=" * 64)


async def _enrich_from_orthodoxwiki(config, db, stats=None) -> None:
    sc = config.saints
    corr = config.corrections
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
    log.info("[saints] OrthodoxWiki enrichment: matching %d crawled page(s)...",
             len(pages))

    headers = {"User-Agent": config.scraper.user_agent}
    rl = sc.rate_limit
    limiter = RateLimiter(requests_per_second=rl.requests_per_second, burst=rl.burst,
                          max_concurrency=rl.max_concurrency)
    matched = by_override = by_name = by_qid = 0
    total = len(pages)
    started = time.monotonic()
    async with aiohttp.ClientSession(headers=headers) as session:
        http = _HttpJson(session, limiter)
        for i, p in enumerate(pages, 1):
            via, saint_id = "override", qid_index.get(corr.owiki_qid.get(p["title"], ""))
            if saint_id is None:
                via, saint_id = "name", name_index.get(normalize_name(p["title"]))
            if saint_id is None and qid_index:
                # Name miss: resolve the page title to a QID and match by it.
                try:
                    qid = await resolve_qid(p["title"], http)
                except Exception as exc:  # noqa: BLE001 - a resolve hiccup skips one page
                    log.debug("QID resolve failed for %r: %s", p["title"], exc)
                    qid = None
                if qid:
                    via, saint_id = "qid", qid_index.get(qid)
            if saint_id is not None:
                lead = clean_wikitext_lead(p["content"])
                if lead:
                    attribution = p.get("attribution") or f"OrthodoxWiki, {ORTHODOXWIKI_LICENSE}"
                    await db.add_claim(saint_id, "bio", lead, owiki.source_id,
                                       sc.orthodoxwiki_weight, ORTHODOXWIKI_LICENSE, attribution)
                    await db.recompute_saint(saint_id)
                    matched += 1
                    by_override += via == "override"
                    by_name += via == "name"
                    by_qid += via == "qid"
                    log.debug("[owiki] %r -> saint %d (%s)", p["title"], saint_id, via)
            # Periodic progress: this loop is network-bound (a rate-limited QID
            # resolve per name-miss) and can run for minutes with most pages
            # matching nothing, so silence here reads as a hang.
            if i % 25 == 0 or i == total:
                elapsed = time.monotonic() - started
                pct = 100.0 * i / total if total else 100.0
                log.info("[owiki %d/%d %4.1f%%] %d matched so far "
                         "(%d override, %d name, %d QID) in %.0fs.",
                         i, total, pct, matched, by_override, by_name, by_qid, elapsed)
    if stats is not None:
        stats.owiki_bio = matched
    log.info("[saints] OrthodoxWiki: %d matched (%d override, %d name, %d QID) of %d.",
             matched, by_override, by_name, by_qid, len(pages))


async def run_enrich(config, db) -> None:
    """OrthodoxWiki enrichment only, skipping the Wikipedia ingest phase.

    Reuses saints and pages already in the DB from prior `--mode saints` /
    `--mode wiki` runs; needs no HTTP unless a name-miss requires a QID resolve.
    Useful for re-running enrichment after a fresh `--mode wiki` crawl without
    re-fetching the Wikipedia roster.
    """
    sc = config.saints
    if not sc.enabled:
        log.warning("saints.enabled is false in the config; nothing to enrich.")
        return
    if sc.orthodoxwiki_weight <= 0:
        log.warning("saints.orthodoxwiki_weight is 0; enrichment is disabled.")
        return
    await _enrich_from_orthodoxwiki(config, db)


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


async def run_review(config, db) -> None:
    """Read-only worklist: emit HOCON correction stubs for the needs-review pile.

    The operator fills in the QIDs and pastes the block into their `.conf`; the
    next run applies them. Capped to enforce high-value-few curation.
    """
    c = await db.coverage()
    names = await db.needs_review_saints(REVIEW_CAP)

    # Unmatched OrthodoxWiki pages (offline name-match only; some still match by
    # QID at ingest, so these are candidates, not guaranteed misses).
    saints = await db.all_saint_names()
    name_index = build_name_index(saints)
    pages = await db.fetch_saint_candidate_pages()
    unmatched = [p["title"] for p in pages
                 if normalize_name(p["title"]) not in name_index]

    log.info("Review worklist: %d needs-review saint(s), %d unmatched OrthodoxWiki "
             "page(s); showing up to %d of each below.",
             c["saints_needs_review"], len(unmatched), REVIEW_CAP)
    _print_correction_stubs(names, unmatched[:REVIEW_CAP])


def _print_correction_stubs(saint_names, owiki_titles) -> None:
    # ponytail: print (not log) on purpose — this block is the command's product,
    # meant to be copy-pasted into a .conf; log timestamps would corrupt it.
    def esc(s):
        return s.replace('"', '\\"')
    print("\n# --- review worklist: fill in qid = \"Q…\" and paste into scraper.conf ---")
    print("corrections {")
    if saint_names:
        print("  saint_qid = [")
        for n in saint_names:
            print(f'    {{ name = "{esc(n)}", qid = "" }}')
        print("  ]")
    if owiki_titles:
        print("  owiki_qid = [")
        for t in owiki_titles:
            print(f'    {{ title = "{esc(t)}", qid = "" }}')
        print("  ]")
    print("}")


# Canonical order: scrape, then ingest icons, then saints, then notify.
_MODES = {"wiki": run_wiki, "icons": run_icons, "saints": run_saints,
          "enrich": run_enrich, "notify": run_notify, "stats": run_stats,
          "review": run_review}

# Modes excluded from an 'all' ingest run: read-only reports (stats, review),
# plus 'enrich', which 'saints' already runs internally when
# orthodoxwiki_weight > 0 — including it in 'all' would just redo that work.
_EXCLUDED_FROM_ALL = {"stats", "review", "enrich"}


def resolve_modes(selected) -> list:
    """Expand 'all', dedupe, and force canonical order. None -> ['wiki']."""
    if not selected:
        return ["wiki"]
    if "all" in selected:
        return [m for m in _MODES if m not in _EXCLUDED_FROM_ALL]
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
    config.icons.force_recrawl = bool(args.force_recrawl)
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
                             "roster + OrthodoxWiki enrichment, no icons/licensing), "
                             "'enrich' (OrthodoxWiki enrichment only, skipping the "
                             "Wikipedia ingest phase — reuses saints/pages already in "
                             "the DB), 'notify' (daily follower notifications), or "
                             "'all'. Runs in order: wiki, icons, saints, notify ('all' "
                             "excludes 'enrich', already covered by 'saints').")
    parser.add_argument("--loop", metavar="DURATION", default=None,
                        help="Run continuously, sleeping this long between passes "
                             "(e.g. '6h', '30 minutes'). Default: run once and exit.")
    parser.add_argument("--force-recrawl", action="store_true",
                        help="Icons: ignore icons.recrawl_after and re-fetch every "
                             "discovered record this run (conditional GET still "
                             "skips unchanged bytes).")
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
