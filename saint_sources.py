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
from typing import AsyncIterator, Tuple

import aiohttp

from config import SaintsConfig
from ratelimit import RateLimiter

log = logging.getLogger("ortho_scraper.saint_sources")

WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"

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
