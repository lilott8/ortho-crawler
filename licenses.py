"""Map OrthodoxWiki image license templates to normalized license info.

Image description pages tag their licensing with templates like ``{{cc by-sa}}``
or ``{{fairuse}}`` (documented at https://orthodoxwiki.org/Help:Image_licenses).
This module detects those templates in a File: page's wikitext and resolves them
to a normalized name / URL / redistributability flag, so the per-file attribution
sidecar carries an actionable license rather than just raw wikitext.

``free`` means "freely redistributable, including derivatives" (public domain or
a permissive CC/GFDL license). Non-commercial, no-derivatives, fair-use, limited,
permission-only, and unverified tags are flagged ``free = False`` — you must
review those before redistributing the file.
"""

from __future__ import annotations

import re
from typing import Dict, List

# Redistribution levels, most permissive first. Stored as the `redistribution`
# enum column on the media table, and exposed in each sidecar.
REDISTRIBUTION_LEVELS = ("public", "free", "restricted", "prohibited")
_LEVEL_RANK = {level: i for i, level in enumerate(REDISTRIBUTION_LEVELS)}

# Keys are normalized template names: lowercased, with '_' and '-' turned into
# spaces and runs of whitespace collapsed (so "cc-by-sa", "CC_BY_SA",
# "cc by-sa" all match "cc by sa"). Each carries a redistribution `level`.
_LICENSE_TEMPLATES: Dict[str, dict] = {
    "pd": {
        "name": "Public Domain", "url": None, "free": True, "level": "public",
    },
    "gfdl": {
        "name": "GFDL", "url": "https://www.gnu.org/licenses/fdl-1.3.html",
        "free": True, "level": "free",
    },
    "cc by sa": {
        "name": "CC BY-SA 2.5", "url": "https://creativecommons.org/licenses/by-sa/2.5/",
        "free": True, "level": "free",
    },
    "cc by nc sa": {
        "name": "CC BY-NC-SA 2.0", "url": "https://creativecommons.org/licenses/by-nc-sa/2.0/",
        "free": False, "level": "restricted", "note": "Non-commercial use only.",
    },
    "cc by nc nd": {
        "name": "CC BY-NC-ND 2.0", "url": "https://creativecommons.org/licenses/by-nc-nd/2.0/",
        "free": False, "level": "restricted", "note": "Non-commercial, no derivatives.",
    },
    "fairuse": {
        "name": "Fair use", "url": None, "free": False, "level": "prohibited",
        "note": "Claimed fair use under US copyright law; not a free license.",
    },
    "limited": {
        "name": "Limited license", "url": None, "free": False, "level": "restricted",
        "note": "Copyrighted with limited permission; see the description page.",
    },
    "unverified": {
        "name": "Unverified", "url": None, "free": False, "level": "prohibited",
        "note": "Source/license unverified; do not redistribute.",
    },
    "oca": {
        "name": "OCA.org (used by permission)", "url": "https://oca.org",
        "free": False, "level": "restricted",
        "note": "Used by permission of OCA.org; per-image permission required.",
    },
    "damickcopy": {
        "name": "Copyrighted, used by permission", "url": None, "free": False,
        "level": "restricted",
        "note": "Copyright retained by the uploader/owner; used by permission.",
    },
}

# Matches {{template}} or {{template|args}}, capturing the template name.
_TEMPLATE_RE = re.compile(r"\{\{\s*([^|}\n]+?)\s*(?:\|[^}]*)?\}\}")


def _normalize(name: str) -> str:
    return re.sub(r"\s+", " ", name.replace("_", " ").replace("-", " ").strip().lower())


def detect_licenses(wikitext: str) -> List[dict]:
    """Return the known license tags found in a File: page's wikitext.

    Each entry is ``{template, name, url, free[, note]}``. Unknown templates are
    ignored (the raw wikitext is preserved separately in the sidecar). The result
    is de-duplicated and order-stable.
    """
    if not wikitext:
        return []
    found: List[dict] = []
    seen = set()
    for raw in _TEMPLATE_RE.findall(wikitext):
        key = _normalize(raw)
        info = _LICENSE_TEMPLATES.get(key)
        if info is None or key in seen:
            continue
        seen.add(key)
        found.append({"template": key, **info})
    return found


def redistribution_level(licenses: List[dict]) -> str:
    """Most permissive redistribution level among detected licenses.

    Returns one of REDISTRIBUTION_LEVELS. With no recognized license, returns
    "prohibited" (treat unknown as not redistributable).
    """
    best = "prohibited"
    for lic in licenses:
        level = lic.get("level", "prohibited")
        if _LEVEL_RANK.get(level, 99) < _LEVEL_RANK[best]:
            best = level
    return best


def best_license_name(licenses: List[dict]) -> str:
    """Name of the most permissive recognized license, or None."""
    best = None
    best_rank = 99
    for lic in licenses:
        rank = _LEVEL_RANK.get(lic.get("level", "prohibited"), 99)
        if rank < best_rank:
            best_rank = rank
            best = lic.get("name")
    return best
