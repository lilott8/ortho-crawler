"""Configuration loading for the OrthodoxWiki scraper.

Reads a HOCON (.conf) file and exposes typed config objects. HOCON durations
such as ``7 days`` are parsed into ``datetime.timedelta`` values by us, since
pyhocon does not interpret duration units natively.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Dict, List, Optional

from pyhocon import ConfigFactory


def expand_path(value: str) -> str:
    """Expand ``~`` and ``$VARS`` in a configured filesystem path.

    Lets storage paths be written as ``~/data/ortho.db`` or ``$DATA_DIR/media``.
    Relative paths are left relative (resolved against the process working dir).
    """
    return os.path.expanduser(os.path.expandvars(value))


_DURATION_UNITS = {
    "ns": 1e-9,
    "nanos": 1e-9,
    "nanosecond": 1e-9,
    "nanoseconds": 1e-9,
    "us": 1e-6,
    "micros": 1e-6,
    "microsecond": 1e-6,
    "microseconds": 1e-6,
    "ms": 1e-3,
    "millis": 1e-3,
    "millisecond": 1e-3,
    "milliseconds": 1e-3,
    "s": 1.0,
    "sec": 1.0,
    "secs": 1.0,
    "second": 1.0,
    "seconds": 1.0,
    "m": 60.0,
    "min": 60.0,
    "mins": 60.0,
    "minute": 60.0,
    "minutes": 60.0,
    "h": 3600.0,
    "hour": 3600.0,
    "hours": 3600.0,
    "d": 86400.0,
    "day": 86400.0,
    "days": 86400.0,
    "w": 604800.0,
    "week": 604800.0,
    "weeks": 604800.0,
}

_DURATION_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([a-zA-Z]+)\s*$")


def parse_duration(value) -> timedelta:
    """Parse a HOCON-style duration ("7 days", "12 hours") into a timedelta.

    A bare number is interpreted as seconds.
    """
    if isinstance(value, timedelta):
        return value
    if isinstance(value, (int, float)):
        return timedelta(seconds=float(value))

    text = str(value).strip()
    # A bare number with no unit -> seconds.
    try:
        return timedelta(seconds=float(text))
    except ValueError:
        pass

    match = _DURATION_RE.match(text)
    if not match:
        raise ValueError(f"Cannot parse duration: {value!r}")
    amount, unit = match.group(1), match.group(2).lower()
    if unit not in _DURATION_UNITS:
        raise ValueError(f"Unknown duration unit {unit!r} in {value!r}")
    return timedelta(seconds=float(amount) * _DURATION_UNITS[unit])


_SIZE_UNITS = {
    "b": 1,
    "k": 1024, "kb": 1024, "kib": 1024,
    "m": 1024 ** 2, "mb": 1024 ** 2, "mib": 1024 ** 2,
    "g": 1024 ** 3, "gb": 1024 ** 3, "gib": 1024 ** 3,
}

_SIZE_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([a-zA-Z]*)\s*$")


def parse_size(value) -> int:
    """Parse a byte size ("25 MB", "500 KB", or a bare number of bytes).

    Returns the size in bytes. 0 means "no limit".
    """
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    match = _SIZE_RE.match(text)
    if not match:
        raise ValueError(f"Cannot parse size: {value!r}")
    amount, unit = match.group(1), match.group(2).lower()
    if not unit:
        return int(float(amount))
    if unit not in _SIZE_UNITS:
        raise ValueError(f"Unknown size unit {unit!r} in {value!r}")
    return int(float(amount) * _SIZE_UNITS[unit])


@dataclass
class MediaPolicy:
    """What media to download for a given category."""
    download: bool = False
    # Media classes to keep: any of image, audio, video, document, other, all.
    types: List[str] = field(default_factory=list)

    def wants(self, media_class: str) -> bool:
        return self.download and ("all" in self.types or media_class in self.types)


@dataclass
class MediaConfig:
    enabled: bool = False
    download_dir: str = "media"
    max_file_size: int = 25 * 1024 ** 2  # bytes; 0 = unlimited
    default: MediaPolicy = field(default_factory=MediaPolicy)
    per_category: dict = field(default_factory=dict)

    def policy_for(self, category: str) -> MediaPolicy:
        return self.per_category.get(category, self.default)

    def effective_types(self, roots) -> set:
        """Union of wanted media classes across the categories a page was found under."""
        if not self.enabled:
            return set()
        wanted = set()
        for root in roots:
            policy = self.policy_for(root)
            if policy.download:
                wanted |= set(policy.types)
        return wanted


@dataclass
class AttributionConfig:
    """Licensing / attribution metadata, captured so the data can be redistributed.

    OrthodoxWiki text is dual-licensed GFDL + CC BY-SA 2.5; image licenses may
    differ per file (see the file's description page).
    """
    site_name: str = "OrthodoxWiki"
    license_name: str = "CC BY-SA 2.5"
    license_url: str = "https://creativecommons.org/licenses/by-sa/2.5/"
    additional_license: str = "GFDL"
    copyright_page: str = "https://orthodoxwiki.org/OrthodoxWiki:Copyrights"
    image_license_note: str = (
        "Image licenses may differ from the page text; see the file's description "
        "page and https://orthodoxwiki.org/Help:Image_licenses"
    )
    # Fetch the full contributor list per page (one extra API call per crawl batch).
    fetch_contributors: bool = True


@dataclass
class RateLimitConfig:
    requests_per_second: float = 2.0
    burst: int = 4
    max_concurrency: int = 4


@dataclass
class HttpConfig:
    timeout: float = 30.0
    max_retries: int = 3
    retry_backoff: float = 1.0


@dataclass
class ScraperConfig:
    api_url: str
    user_agent: str
    categories: List[str] = field(default_factory=list)
    recurse_subcategories: bool = True
    max_subcategory_depth: int = 2
    recrawl_after: timedelta = timedelta(days=7)
    reconcile_deletions: bool = True
    media: MediaConfig = field(default_factory=MediaConfig)
    attribution: AttributionConfig = field(default_factory=AttributionConfig)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    http: HttpConfig = field(default_factory=HttpConfig)


@dataclass
class IconSourceConfig:
    """One ingest source for the Icon & Saints data layer.

    The license-relevant fields (``base_license``, ``requires_per_item_check``,
    ``allowed_licenses``, ``attribution_template``) are mirrored into the
    ``sources`` table and consumed by the license gate. The remaining fields are
    per-adapter crawl knobs (Met queries, Wikimedia search terms, ICONSAINT
    dataset path).
    """
    name: str
    enabled: bool = False
    base_license: str = "UNVERIFIED"
    requires_per_item_check: bool = True
    attribution_template: Optional[str] = None
    allowed_licenses: List[str] = field(default_factory=list)
    notes: Optional[str] = None
    # Met adapter
    queries: List[str] = field(default_factory=list)
    max_objects: int = 200
    # Wikimedia adapter
    search_terms: List[str] = field(default_factory=list)
    max_files: int = 200
    # ICONSAINT adapter (local dataset)
    dataset_path: str = ""
    manifest: str = ""


@dataclass
class LicensePolicy:
    """A coarse, per-type licensing override (looser than the per-record DB
    ``license_overrides``). Matches by ``target_type`` plus optional ``source``
    and ``field`` (omitted = wildcard); most-specific match wins. An ``approved``
    policy still carries attribution (a default is applied if none is given) —
    it can widen what's cleared, never drop attribution.
    """
    target_type: str                  # 'saint' | 'icon'
    decision: str                     # 'approved' | 'rejected'
    source: Optional[str] = None
    field: Optional[str] = None
    license: Optional[str] = None
    attribution: Optional[str] = None

    def specificity(self) -> int:
        return int(self.source is not None) + int(self.field is not None)

    def matches(self, target_type: str, source: Optional[str],
                field: Optional[str]) -> bool:
        if self.target_type != target_type:
            return False
        if self.source is not None and self.source != source:
            return False
        if self.field is not None and self.field != field:
            return False
        return True


def select_policy(policies, target_type: str, source: Optional[str],
                  field: Optional[str]):
    """Return the most-specific matching policy, or None. Ties keep config order."""
    best = None
    for p in policies:
        if p.matches(target_type, source, field) and (
                best is None or p.specificity() > best.specificity()):
            best = p
    return best


@dataclass
class IconsConfig:
    """Configuration for the Icon & Saints ingestion pipeline + notifications.

    Disabled by default; the wiki scraper is unaffected when ``enabled`` is
    false. Images are downloaded into a content-addressed tree like the wiki
    media, but only for records the license gate approves.
    """
    enabled: bool = False
    download_dir: str = "icons"
    max_file_size: int = 25 * 1024 ** 2  # bytes; 0 = unlimited
    sources: Dict[str, IconSourceConfig] = field(default_factory=dict)
    rate_limit: "RateLimitConfig" = field(default_factory=lambda: RateLimitConfig())
    http: "HttpConfig" = field(default_factory=lambda: HttpConfig())

    def enabled_sources(self) -> List[IconSourceConfig]:
        return [s for s in self.sources.values() if s.enabled]


@dataclass
class SaintsConfig:
    """Saint roster ingestion from a Wikipedia list article (no icons/licensing)."""
    enabled: bool = False
    wikipedia_articles: List[str] = field(
        default_factory=lambda: ["List of Eastern Orthodox saints"])
    max_records: int = 500
    # Weight of Wikipedia bio claims in the per-field reducer. Wikipedia is the
    # content spine, so it outranks enrichment sources by default.
    wikipedia_weight: int = 100
    # Weight of OrthodoxWiki bio claims (enrichment) — below Wikipedia, so it only
    # wins a saint's bio when Wikipedia has none. 0 disables the enrichment pass.
    orthodoxwiki_weight: int = 50


@dataclass
class DatabaseConfig:
    backend: str = "postgres"  # "postgres" or "sqlite"

    # SQLite
    path: str = "orthodoxwiki.db"

    # PostgreSQL
    host: str = "localhost"
    port: int = 5432
    name: str = "orthodoxwiki"
    user: str = "postgres"
    password: str = ""
    pool_min_size: int = 2
    pool_max_size: int = 8

    def dsn_kwargs(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "database": self.name,
            "user": self.user,
            "password": self.password or None,
        }


@dataclass
class Config:
    scraper: ScraperConfig
    database: DatabaseConfig
    icons: IconsConfig = field(default_factory=IconsConfig)
    saints: SaintsConfig = field(default_factory=SaintsConfig)
    license_policies: List[LicensePolicy] = field(default_factory=list)


def _policy_from(node) -> MediaPolicy:
    if node is None:
        return MediaPolicy()
    types = [str(t).lower() for t in node.get("types", [])]
    return MediaPolicy(download=bool(node.get("download", False)), types=types)


def _load_media(conf) -> MediaConfig:
    node = conf.get("scraper.media", None)
    if node is None:
        return MediaConfig()
    per_category = {}
    cp = node.get("categories_policy", {})
    for name in cp:  # iterating a ConfigTree yields its keys
        per_category[name] = _policy_from(cp[name])
    return MediaConfig(
        enabled=bool(node.get("enabled", False)),
        download_dir=expand_path(str(node.get("download_dir", "media"))),
        max_file_size=parse_size(node.get("max_file_size", "25 MB")),
        default=_policy_from(node.get("default", None)),
        per_category=per_category,
    )


def _load_attribution(conf) -> AttributionConfig:
    node = conf.get("scraper.attribution", None)
    d = AttributionConfig()
    if node is None:
        return d
    return AttributionConfig(
        site_name=str(node.get("site_name", d.site_name)),
        license_name=str(node.get("license_name", d.license_name)),
        license_url=str(node.get("license_url", d.license_url)),
        additional_license=str(node.get("additional_license", d.additional_license)),
        copyright_page=str(node.get("copyright_page", d.copyright_page)),
        image_license_note=str(node.get("image_license_note", d.image_license_note)),
        fetch_contributors=bool(node.get("fetch_contributors", d.fetch_contributors)),
    )


def _load_icon_source(name: str, node) -> IconSourceConfig:
    if node is None:
        return IconSourceConfig(name=name)
    return IconSourceConfig(
        name=name,
        enabled=bool(node.get("enabled", False)),
        base_license=str(node.get("base_license", "UNVERIFIED")),
        requires_per_item_check=bool(node.get("requires_per_item_check", True)),
        attribution_template=(str(node["attribution_template"])
                              if node.get("attribution_template", None) is not None else None),
        allowed_licenses=[str(x) for x in node.get("allowed_licenses", [])],
        notes=(str(node["notes"]) if node.get("notes", None) is not None else None),
        queries=[str(x) for x in node.get("queries", [])],
        max_objects=int(node.get("max_objects", 200)),
        search_terms=[str(x) for x in node.get("search_terms", [])],
        max_files=int(node.get("max_files", 200)),
        dataset_path=expand_path(str(node.get("dataset_path", ""))),
        manifest=str(node.get("manifest", "")),
    )


def _load_icons(conf) -> IconsConfig:
    node = conf.get("icons", None)
    if node is None:
        return IconsConfig()
    rl = node.get("rate_limit", {})
    http = node.get("http", {})
    sources_node = node.get("sources", {})
    sources: Dict[str, IconSourceConfig] = {}
    for name in sources_node:  # iterating a ConfigTree yields its keys
        sources[name] = _load_icon_source(name, sources_node[name])
    return IconsConfig(
        enabled=bool(node.get("enabled", False)),
        download_dir=expand_path(str(node.get("download_dir", "icons"))),
        max_file_size=parse_size(node.get("max_file_size", "25 MB")),
        sources=sources,
        rate_limit=RateLimitConfig(
            requests_per_second=float(rl.get("requests_per_second", 2.0)),
            burst=int(rl.get("burst", 4)),
            max_concurrency=int(rl.get("max_concurrency", 4)),
        ),
        http=HttpConfig(
            timeout=float(http.get("timeout", 30.0)),
            max_retries=int(http.get("max_retries", 3)),
            retry_backoff=float(http.get("retry_backoff", 1.0)),
        ),
    )


def _load_license_policies(conf) -> List[LicensePolicy]:
    node = conf.get("license_policies", None)
    if not node:
        return []
    policies: List[LicensePolicy] = []
    for item in node:  # a HOCON list of objects
        policies.append(LicensePolicy(
            target_type=str(item["target_type"]),
            decision=str(item["decision"]),
            source=(str(item["source"]) if item.get("source", None) is not None else None),
            field=(str(item["field"]) if item.get("field", None) is not None else None),
            license=(str(item["license"]) if item.get("license", None) is not None else None),
            attribution=(str(item["attribution"])
                         if item.get("attribution", None) is not None else None),
        ))
    return policies


def _load_saints(conf) -> SaintsConfig:
    node = conf.get("saints", None)
    if node is None:
        return SaintsConfig()
    d = SaintsConfig()
    return SaintsConfig(
        enabled=bool(node.get("enabled", False)),
        wikipedia_articles=[str(x) for x in node.get("wikipedia_articles",
                                                       d.wikipedia_articles)],
        max_records=int(node.get("max_records", d.max_records)),
        wikipedia_weight=int(node.get("wikipedia_weight", d.wikipedia_weight)),
        orthodoxwiki_weight=int(node.get("orthodoxwiki_weight", d.orthodoxwiki_weight)),
    )


def load_config(path: str) -> Config:
    conf = ConfigFactory.parse_file(path)

    rl = conf.get_config("scraper.rate_limit", {})
    http = conf.get_config("scraper.http", {})
    media = _load_media(conf)
    attribution = _load_attribution(conf)

    scraper = ScraperConfig(
        api_url=conf.get_string("scraper.api_url"),
        user_agent=conf.get_string("scraper.user_agent"),
        categories=list(conf.get_list("scraper.categories", [])),
        recurse_subcategories=conf.get_bool("scraper.recurse_subcategories", True),
        max_subcategory_depth=conf.get_int("scraper.max_subcategory_depth", 2),
        recrawl_after=parse_duration(conf.get("scraper.recrawl_after", "7 days")),
        reconcile_deletions=conf.get_bool("scraper.reconcile_deletions", True),
        media=media,
        attribution=attribution,
        rate_limit=RateLimitConfig(
            requests_per_second=float(rl.get("requests_per_second", 2.0)),
            burst=int(rl.get("burst", 4)),
            max_concurrency=int(rl.get("max_concurrency", 4)),
        ),
        http=HttpConfig(
            timeout=float(http.get("timeout", 30.0)),
            max_retries=int(http.get("max_retries", 3)),
            retry_backoff=float(http.get("retry_backoff", 1.0)),
        ),
    )

    database = DatabaseConfig(
        backend=conf.get_string("database.backend", "postgres").lower(),
        path=expand_path(conf.get_string("database.path", "orthodoxwiki.db")),
        host=conf.get_string("database.host", "localhost"),
        port=conf.get_int("database.port", 5432),
        name=conf.get_string("database.name", "orthodoxwiki"),
        user=conf.get_string("database.user", "postgres"),
        password=conf.get_string("database.password", ""),
        pool_min_size=conf.get_int("database.pool_min_size", 2),
        pool_max_size=conf.get_int("database.pool_max_size", 8),
    )

    return Config(scraper=scraper, database=database, icons=_load_icons(conf),
                  saints=_load_saints(conf),
                  license_policies=_load_license_policies(conf))
