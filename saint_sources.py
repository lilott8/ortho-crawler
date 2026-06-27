"""Saint roster ingestion: extract saint names from a Wikipedia list article.

Independent of the Icon & Saints licensing pipeline (icon_pipeline.py /
icon_sources.py) — there is no image, no license gate, just names flowing
into ``saints.canonical_name`` via ``Storage.upsert_saint``. enwiki text is
CC BY-SA/GFDL like OrthodoxWiki itself; that's a blanket fact about the
source, not a per-record signal worth threading through a gate.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import AsyncIterator, List, Optional, Tuple

import aiohttp

from config import SaintsConfig
from ratelimit import RateLimiter

log = logging.getLogger("ortho_scraper.saint_sources")

WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"

# enwiki prose is CC BY-SA 4.0 (+ GFDL) at the source level — a blanket fact, not
# a per-record signal. The claim carries it so the reducer can clear it.
WIKIPEDIA_LICENSE = "CC-BY-SA-4.0"


async def resolve_qid(label: str, http) -> Optional[str]:
    """Resolve a free-text saint label to a Wikidata QID (identity anchor).

    ``http`` is any object exposing ``async get(url, params) -> dict`` (e.g.
    ``icon_sources._HttpJson``), so this is shared by the icon pipeline. Returns
    None when there's no confident hit — the caller treats that as needs-review.
    """
    label = (label or "").strip()
    if not label:
        return None
    data = await http.get(WIKIDATA_API_URL, {
        "action": "wbsearchentities", "search": label, "language": "en",
        "format": "json", "limit": 1, "type": "item"})
    hits = data.get("search") or []
    return hits[0].get("id") if hits else None


_WD_P841_FEAST = "P841"   # Wikidata "feast day" -> a calendar-day item

_MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], start=1)}


def _parse_month_day(label: str):
    """Parse a Wikidata calendar-day label ('13 November', 'January 27') to MM-DD."""
    if not label:
        return None
    low = label.lower()
    month = next((num for name, num in _MONTHS.items() if name in low), None)
    daym = re.search(r"\b(\d{1,2})\b", label)
    if month and daym:
        day = int(daym.group(1))
        if 1 <= day <= 31:
            return f"{month:02d}-{day:02d}"
    return None


async def _wbget(qid: str, props: str, session: aiohttp.ClientSession,
                 limiter: RateLimiter):
    try:
        async with limiter:
            async with session.get(WIKIDATA_API_URL, params={
                "action": "wbgetentities", "ids": qid, "props": props,
                "languages": "en", "format": "json"}) as resp:
                resp.raise_for_status()
                data = await resp.json()
    except aiohttp.ClientError as exc:
        log.debug("[saints] wbgetentities(%s, %s) failed: %s", qid, props, exc)
        return None
    if "error" in data:
        return None
    return (data.get("entities") or {}).get(qid)


async def fetch_wikidata_facts(qid: str, session: aiohttp.ClientSession,
                               limiter: RateLimiter):
    """(aliases, feast_day_qids, description) for a saint QID — one wbgetentities call.

    Aliases feed the multi-valued ``alt_names`` field; every P841 value (a saint
    may have several feast traditions) is collected and resolved to MM-DD
    separately (and cached); the CC0 description is a core-data one-liner. All are
    uncopyrightable. Returns ([], [], None) on any failure.
    """
    ent = await _wbget(qid, "aliases|claims|descriptions", session, limiter)
    if ent is None:
        return [], [], None
    aliases, seen = [], set()
    for a in (ent.get("aliases") or {}).get("en") or []:
        v = (a.get("value") or "").strip()
        if v and v not in seen:
            seen.add(v)
            aliases.append(v)
    feast_qids, fseen = [], set()
    for claim in (ent.get("claims") or {}).get(_WD_P841_FEAST) or []:
        value = (((claim.get("mainsnak") or {}).get("datavalue") or {}).get("value") or {})
        fid = value.get("id")
        if fid and fid not in fseen:
            fseen.add(fid)
            feast_qids.append(fid)
    description = ((ent.get("descriptions") or {}).get("en") or {}).get("value") or None
    return aliases, feast_qids, description


async def resolve_feast_md(day_qid: str, session: aiohttp.ClientSession,
                           limiter: RateLimiter, cache: dict):
    """Resolve a calendar-day item QID to MM-DD, memoized per run (many saints
    share the same feast day)."""
    if day_qid in cache:
        return cache[day_qid]
    ent = await _wbget(day_qid, "labels", session, limiter)
    label = ((ent or {}).get("labels", {}).get("en", {}) or {}).get("value")
    md = _parse_month_day(label)
    cache[day_qid] = md
    return md


@dataclass
class SaintRecord:
    """A Wikipedia-sourced saint with identity + a (licensed) bio claim."""
    qid: Optional[str]            # Wikidata QID, the identity anchor; None = needs-review
    display_name: str             # resolved article title (redirects followed)
    bio_text: Optional[str]       # lead extract; None if the article had none
    license: str = WIKIPEDIA_LICENSE
    attribution: str = ""
    alt_names: List[str] = field(default_factory=list)   # Wikidata aliases (multi-valued)
    feast_days: List[str] = field(default_factory=list)  # MM-DD list, from Wikidata P841 (facts)
    description: Optional[str] = None                     # CC0 Wikidata one-liner (core data)
    qid_from_correction: bool = False                    # QID came from a saint_qid override

# ponytail: only mainspace article links are saints; everything else
# (Category:, File:, Template:, Help:, Portal:, Wikipedia:, List of ...) is
# page furniture. Good enough for enwiki saint lists; tighten if a list page
# turns out to link a lot of non-saint articles.
_WIKILINK_RE = re.compile(r"\[\[([^\[\]|#]+)(?:\|([^\[\]]+))?\]\]")
_SKIP_PREFIXES = ("Category:", "File:", "Template:", "Help:", "Portal:",
                   "Wikipedia:", "Talk:", "List of")


async def fetch_saint_names(cfg: SaintsConfig, session: aiohttp.ClientSession,
                             limiter: RateLimiter) -> AsyncIterator[Tuple[str, str]]:
    """Yield (canonical_name, display_name) pairs from the configured list articles."""
    emitted = 0
    for page_title in cfg.wikipedia_articles:
        try:
            async with limiter:
                async with session.get(WIKIPEDIA_API_URL, params={
                    "action": "parse",
                    "page": page_title,
                    "prop": "wikitext",
                    "format": "json",
                    "formatversion": "2",
                }) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
        except aiohttp.ClientError as exc:
            # One bad/missing article shouldn't sink the others.
            log.warning("[saints] skipping %r: %s", page_title, exc)
            continue
        # A missing page returns 200 with an "error" object, not a 4xx.
        if "error" in data:
            log.warning("[saints] skipping %r: %s", page_title,
                        data["error"].get("info", "no such page"))
            continue
        wikitext = data.get("parse", {}).get("wikitext", "")
        seen: set = set()
        for target, display in _WIKILINK_RE.findall(wikitext):
            if emitted >= cfg.max_records:
                break
            target = target.strip()
            if not target or target.startswith(_SKIP_PREFIXES) or target in seen:
                continue
            seen.add(target)
            emitted += 1
            yield target, (display.strip() or target)
        log.info("[saints] %r -> %d name(s) so far.", page_title, emitted)


async def fetch_saint_records(cfg: SaintsConfig, session: aiohttp.ClientSession,
                              limiter: RateLimiter,
                              qid_overrides: Optional[dict] = None) -> AsyncIterator[SaintRecord]:
    """Yield per-saint records (QID + licensed bio) for the configured roster.

    Builds on :func:`fetch_saint_names` for the roster, then one ``prop=
    extracts|pageprops`` call per name to pull the Wikidata QID and lead extract.
    A name that won't resolve to a QID yields ``qid=None`` -> needs-review, unless
    a ``qid_overrides`` correction (display title -> QID) supplies one.
    """
    qid_overrides = qid_overrides or {}
    feast_cache: dict = {}    # day-item QID -> MM-DD, shared across the roster
    async for target, _display in fetch_saint_names(cfg, session, limiter):
        try:
            async with limiter:
                async with session.get(WIKIPEDIA_API_URL, params={
                    "action": "query",
                    "prop": "extracts|pageprops",
                    "titles": target,
                    "exintro": "1",
                    "explaintext": "1",
                    "ppprop": "wikibase_item",
                    "redirects": "1",
                    "format": "json",
                    "formatversion": "2",
                }) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
        except aiohttp.ClientError as exc:
            log.warning("[saints] skipping %r: %s", target, exc)
            continue
        if "error" in data:  # missing page returns 200 + error object, not a 4xx
            log.warning("[saints] skipping %r: %s", target,
                        data["error"].get("info", "query error"))
            continue
        pages = data.get("query", {}).get("pages", [])
        if not pages:
            continue
        page = pages[0]
        if page.get("missing"):
            log.debug("[saints] %r is missing; skipping.", target)
            continue
        title = page.get("title") or target
        qid = (page.get("pageprops", {}) or {}).get("wikibase_item")
        bio = (page.get("extract") or "").strip() or None
        # A correction can supply a QID Wikipedia didn't carry (rescues a
        # needs-review saint and unlocks its Wikidata facts on this very run).
        from_correction = False
        if qid is None:
            override = qid_overrides.get(title) or qid_overrides.get(target)
            if override:
                qid, from_correction = override, True
            else:
                log.debug("[saints] %r has no Wikidata QID -> needs-review.", title)
        alt_names, feast_qids, description = (
            await fetch_wikidata_facts(qid, session, limiter) if qid else ([], [], None))
        feast_days = []
        for fq in feast_qids:
            md = await resolve_feast_md(fq, session, limiter, feast_cache)
            if md and md not in feast_days:
                feast_days.append(md)
        yield SaintRecord(
            qid=qid,
            display_name=title,
            bio_text=bio,
            attribution=_wikipedia_attribution(title),
            alt_names=alt_names,
            feast_days=feast_days,
            description=description,
            qid_from_correction=from_correction,
        )


def _wikipedia_attribution(title: str) -> str:
    url = "https://en.wikipedia.org/wiki/" + title.replace(" ", "_")
    return f'"{title}", Wikipedia (en.wikipedia.org), {WIKIPEDIA_LICENSE}, {url}'


# --- OrthodoxWiki enrichment helpers ----------------------------------------
# OrthodoxWiki text is GFDL + CC BY-SA 2.5 (a blanket source fact). The crawler
# already stores page wikitext + attribution; enrichment reuses that, matching
# pages to existing saints by name (no extra API calls — the alt_names from
# Wikidata power the match).
ORTHODOXWIKI_LICENSE = "CC-BY-SA-2.5"

_NAME_PREFIX_RE = re.compile(r"^(?:st\.?|saint|holy|the\s+venerable|venerable)\s+",
                             re.IGNORECASE)


def normalize_name(name: str) -> str:
    """Fold a saint name to a match key: lowercase, drop honorific prefixes and
    punctuation. Deliberately lossy — enrichment matching is low-stakes."""
    n = (name or "").strip().lower()
    n = _NAME_PREFIX_RE.sub("", n)
    n = re.sub(r"[^a-z0-9 ]+", " ", n)
    return re.sub(r"\s+", " ", n).strip()


def build_name_index(saints) -> dict:
    """Map normalized name -> saint_id from rows of {id, canonical_name, alt_names}.

    Indexes the canonical name and every alias. On collision the first wins (and
    canonical names are added first, so they beat aliases)."""
    import json
    index: dict = {}
    for s in saints:
        names = [s.get("canonical_name")]
        raw = s.get("alt_names")
        if raw:
            try:
                names += list(json.loads(raw))
            except (ValueError, TypeError):
                pass
        for name in names:
            key = normalize_name(name) if name else ""
            if key and key not in index:
                index[key] = s["id"]
    return index


def clean_wikitext_lead(wikitext: str, max_len: int = 2000):
    """Best-effort plain-text lead from OrthodoxWiki wikitext for a bio claim.

    Heuristic, not a real parser: take the text before the first section heading
    and strip templates, refs, links and markup. Good enough for a bio; tighten
    if specific pages render badly.
    """
    if not wikitext:
        return None
    text = re.split(r"\n==", wikitext, 1)[0]            # lead section only
    prev = None
    while prev != text:                                  # drop {{templates}} (nested)
        prev = text
        text = re.sub(r"\{\{[^{}]*\}\}", "", text)
    text = re.sub(r"<ref[^>]*>.*?</ref>", "", text, flags=re.S)
    text = re.sub(r"<ref[^>]*/>", "", text)
    text = re.sub(r"<!--.*?-->", "", text, flags=re.S)
    text = re.sub(r"\[\[(?:Category|File|Image):[^\]]*\]\]", "", text, flags=re.I)
    text = re.sub(r"\[\[(?:[^\[\]|]*\|)?([^\[\]|]+)\]\]", r"\1", text)   # [[a|b]]->b
    text = re.sub(r"\[https?://\S+\s+([^\]]+)\]", r"\1", text)           # [url label]->label
    text = re.sub(r"\[https?://\S+\]", "", text)
    text = text.replace("'''", "").replace("''", "")
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return None
    return text[:max_len].rstrip()
