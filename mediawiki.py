"""Thin async MediaWiki API client for OrthodoxWiki.

OrthodoxWiki runs MediaWiki, so we talk to ``api.php`` rather than scraping
rendered HTML. This is faster, lighter on the server, and gives us clean
structured data (page ids, revisions, wikitext, categories).
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass, field
from typing import AsyncIterator, Dict, List, Optional

import aiohttp

from ratelimit import RateLimiter

log = logging.getLogger("ortho_scraper.mediawiki")

# MediaWiki allows up to 50 titles/ids per query for normal users.
BATCH_SIZE = 50
# Content-bearing queries (File: page wikitext) are far heavier on the server,
# so we request fewer titles at a time to avoid 502s on the old backend.
DESCRIPTION_BATCH_SIZE = 10


@dataclass
class CategoryMember:
    pageid: int
    title: str
    ns: int
    is_subcat: bool


@dataclass
class PageContent:
    pageid: int
    title: str
    ns: int
    url: str
    revid: Optional[int]
    content: Optional[str]
    touched: Optional[str]
    categories: List[str] = field(default_factory=list)
    # Populated during the crawl for attribution; not returned by fetch_pages.
    contributors: List[str] = field(default_factory=list)
    attribution: Optional[str] = None


@dataclass
class MediaFile:
    title: str                  # e.g. "File:Saint_Nicholas.jpg"
    url: Optional[str]
    mime: Optional[str]
    size: Optional[int]
    sha1: Optional[str]
    descriptionurl: Optional[str] = None    # the File: description page (states the license)
    uploader: Optional[str] = None
    description_wikitext: Optional[str] = None  # File: page wikitext (license templates, source)

    @property
    def media_class(self) -> str:
        return classify_media(self.mime, self.title)


_DOC_MIMES = {
    "application/pdf", "application/msword", "application/rtf",
    "application/vnd.oasis.opendocument.text",
}
_DOC_EXTS = {".pdf", ".doc", ".docx", ".odt", ".rtf", ".txt", ".djvu"}


def classify_media(mime: Optional[str], title: str = "") -> str:
    """Bucket a file into image / audio / video / document / other."""
    m = (mime or "").lower()
    if m.startswith("image/"):
        return "image"
    if m.startswith("audio/"):
        return "audio"
    if m.startswith("video/"):
        return "video"
    if m in _DOC_MIMES or "officedocument" in m or m.startswith("text/"):
        return "document"
    # Fall back to the file extension when MIME is missing/ambiguous.
    ext = title.lower().rsplit(".", 1)
    if len(ext) == 2 and ("." + ext[1]) in _DOC_EXTS:
        return "document"
    return "other"


@dataclass
class ClientStats:
    """Cumulative HTTP counters for run reporting."""
    api_requests: int = 0       # successful API queries
    http_attempts: int = 0      # total GET attempts (API + downloads, incl. retries)
    retries: int = 0            # retried attempts
    downloads: int = 0          # successful file downloads
    download_bytes: int = 0     # bytes of media downloaded


class MediaWikiClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_url: str,
        limiter: RateLimiter,
        max_retries: int = 3,
        retry_backoff: float = 1.0,
    ):
        self._session = session
        self._api_url = api_url
        self._limiter = limiter
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff
        self.stats = ClientStats()

    def _backoff(self, attempt: int) -> float:
        """Exponential backoff with jitter to avoid synchronized retries."""
        base = self._retry_backoff * (2 ** (attempt - 1))
        return base + random.uniform(0, base)

    async def _get(self, params: dict) -> dict:
        """Issue a rate-limited GET against the API with retries."""
        params = {**params, "format": "json", "formatversion": "2"}
        attempt = 0
        while True:
            attempt += 1
            self.stats.http_attempts += 1
            try:
                async with self._limiter:
                    async with self._session.get(self._api_url, params=params) as resp:
                        if resp.status >= 500:
                            raise aiohttp.ClientResponseError(
                                resp.request_info, resp.history, status=resp.status,
                                message=f"server error {resp.status}",
                            )
                        resp.raise_for_status()
                        data = await resp.json()
                if "error" in data:
                    raise RuntimeError(f"API error: {data['error']}")
                self.stats.api_requests += 1
                return data
            except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as exc:
                if attempt > self._max_retries:
                    log.warning("API request failed after %d attempts: %s", attempt, exc)
                    raise
                self.stats.retries += 1
                delay = self._backoff(attempt)
                # Transient (usually 502/503 from the old backend); retried
                # quietly. The aggregate retry count is in the run summary.
                log.debug("Transient API error (%s); retry %d/%d in %.1fs",
                          exc, attempt, self._max_retries, delay)
                await asyncio.sleep(delay)

    async def iter_category_members(self, category: str) -> AsyncIterator[CategoryMember]:
        """Yield all pages and subcategories that are members of a category."""
        cmtitle = category if category.lower().startswith("category:") else f"Category:{category}"
        cont: Dict[str, str] = {}
        while True:
            params = {
                "action": "query",
                "list": "categorymembers",
                "cmtitle": cmtitle,
                "cmlimit": "500",
                "cmtype": "page|subcat",
                "cmprop": "ids|title|type",
                **cont,
            }
            data = await self._get(params)
            for m in data.get("query", {}).get("categorymembers", []):
                yield CategoryMember(
                    pageid=m["pageid"],
                    title=m["title"],
                    ns=m["ns"],
                    is_subcat=(m.get("type") == "subcat"),
                )
            if "continue" in data:
                cont = data["continue"]
            else:
                break

    async def check_existing(self, pageids: List[int]) -> set:
        """Return the subset of ``pageids`` that still exist on the wiki.

        Cheap metadata-only query (no content). Deleted pages either come back
        flagged ``missing`` or are dropped from the response entirely, so the
        caller can treat any id not in the returned set as gone.
        """
        if not pageids:
            return set()
        params = {
            "action": "query",
            "pageids": "|".join(str(p) for p in pageids),
            "prop": "info",
        }
        data = await self._get(params)
        existing = set()
        for page in data.get("query", {}).get("pages", []):
            if not page.get("missing"):
                existing.add(page["pageid"])
        return existing

    async def fetch_latest_revids(self, pageids: List[int]) -> Dict[int, int]:
        """Cheap metadata-only probe: {pageid: current lastrevid}.

        Used to detect whether a page changed since we last crawled it, without
        downloading its content. Pages that are missing are simply omitted.
        """
        if not pageids:
            return {}
        result: Dict[int, int] = {}
        for i in range(0, len(pageids), BATCH_SIZE):
            batch = pageids[i:i + BATCH_SIZE]
            params = {
                "action": "query",
                "pageids": "|".join(str(p) for p in batch),
                "prop": "info",
            }
            data = await self._get(params)
            for page in data.get("query", {}).get("pages", []):
                if page.get("missing") or "lastrevid" not in page:
                    continue
                result[page["pageid"]] = page["lastrevid"]
        return result

    async def fetch_pages(self, pageids: List[int]) -> List[PageContent]:
        """Fetch content + metadata for up to BATCH_SIZE page ids."""
        if not pageids:
            return []
        params = {
            "action": "query",
            "pageids": "|".join(str(p) for p in pageids),
            "prop": "revisions|info|categories",
            "rvprop": "ids|content",
            "rvslots": "main",  # modern MediaWiki; older wikis ignore this
            "inprop": "url",
            "cllimit": "max",
        }
        data = await self._get(params)
        results: List[PageContent] = []
        for page in data.get("query", {}).get("pages", []):
            if page.get("missing"):
                continue
            revisions = page.get("revisions", [])
            revid = None
            content = None
            if revisions:
                rev = revisions[0]
                revid = rev.get("revid")
                # Modern MediaWiki nests content under slots.main; older
                # versions (like OrthodoxWiki) put it directly on the revision.
                slot = rev.get("slots", {}).get("main", {})
                content = slot.get("content", rev.get("content"))
            cats = [c["title"] for c in page.get("categories", [])]
            results.append(PageContent(
                pageid=page["pageid"],
                title=page["title"],
                ns=page["ns"],
                url=page.get("fullurl", ""),
                revid=revid,
                content=content,
                touched=page.get("touched"),
                categories=cats,
            ))
        return results

    async def fetch_page_images(self, pageids: List[int]) -> Dict[int, List[str]]:
        """Return {pageid: [File titles used on the page]} for a batch of pages."""
        if not pageids:
            return {}
        result: Dict[int, List[str]] = {pid: [] for pid in pageids}
        cont: Dict[str, str] = {}
        while True:
            params = {
                "action": "query",
                "pageids": "|".join(str(p) for p in pageids),
                "prop": "images",
                "imlimit": "max",
                **cont,
            }
            data = await self._get(params)
            for page in data.get("query", {}).get("pages", []):
                pid = page.get("pageid")
                if pid is None:
                    continue
                for img in page.get("images", []):
                    result.setdefault(pid, []).append(img["title"])
            if "continue" in data:
                cont = data["continue"]
            else:
                break
        return result

    async def fetch_imageinfo(self, file_titles: List[str]) -> Dict[str, MediaFile]:
        """Resolve File: titles to lightweight metadata (no description wikitext).

        Captures URL / mime / size / sha1, the uploader, and the description-page
        URL. This is the cheap query used to decide which files to download; the
        heavier File: page wikitext is fetched separately, only for the files we
        actually keep, via :meth:`fetch_file_descriptions`.
        """
        if not file_titles:
            return {}
        info: Dict[str, MediaFile] = {}
        for i in range(0, len(file_titles), BATCH_SIZE):
            batch = file_titles[i:i + BATCH_SIZE]
            params = {
                "action": "query",
                "titles": "|".join(batch),
                "prop": "imageinfo",
                "iiprop": "url|size|mime|sha1|user",
            }
            data = await self._get(params)
            for page in data.get("query", {}).get("pages", []):
                if page.get("missing") or "imageinfo" not in page:
                    continue
                ii = page["imageinfo"][0]
                info[page["title"]] = MediaFile(
                    title=page["title"],
                    url=ii.get("url"),
                    mime=ii.get("mime"),
                    size=ii.get("size"),
                    sha1=ii.get("sha1"),
                    descriptionurl=ii.get("descriptionurl"),
                    uploader=ii.get("user"),
                    description_wikitext=None,
                )
        return info

    async def fetch_file_descriptions(self, file_titles: List[str]) -> Dict[str, str]:
        """Fetch File: page wikitext (license templates, source) for given titles.

        This is the heavy, content-bearing query, so it uses a smaller batch than
        metadata queries to avoid overloading the (old) MediaWiki backend, and is
        only called for files we're actually keeping.
        """
        if not file_titles:
            return {}
        result: Dict[str, str] = {}
        for i in range(0, len(file_titles), DESCRIPTION_BATCH_SIZE):
            batch = file_titles[i:i + DESCRIPTION_BATCH_SIZE]
            params = {
                "action": "query",
                "titles": "|".join(batch),
                "prop": "revisions",
                "rvprop": "content",
                "rvslots": "main",
            }
            data = await self._get(params)
            for page in data.get("query", {}).get("pages", []):
                if page.get("missing"):
                    continue
                revs = page.get("revisions", [])
                if not revs:
                    continue
                slot = revs[0].get("slots", {}).get("main", {})
                content = slot.get("content", revs[0].get("content"))
                if content is not None:
                    result[page["title"]] = content
        return result

    async def fetch_contributors(self, pageids: List[int]) -> Dict[int, List[str]]:
        """Return {pageid: [contributor usernames]} for attribution (BY-SA)."""
        if not pageids:
            return {}
        result: Dict[int, List[str]] = {pid: [] for pid in pageids}
        cont: Dict[str, str] = {}
        while True:
            params = {
                "action": "query",
                "pageids": "|".join(str(p) for p in pageids),
                "prop": "contributors",
                "pclimit": "max",
                **cont,
            }
            data = await self._get(params)
            for page in data.get("query", {}).get("pages", []):
                pid = page.get("pageid")
                if pid is None:
                    continue
                for c in page.get("contributors", []):
                    result.setdefault(pid, []).append(c["name"])
            if "continue" in data:
                cont = data["continue"]
            else:
                break
        return result

    async def download_file(self, url: str) -> bytes:
        """Fetch raw file bytes, rate-limited and retried like API calls."""
        attempt = 0
        while True:
            attempt += 1
            self.stats.http_attempts += 1
            try:
                async with self._limiter:
                    async with self._session.get(url) as resp:
                        if resp.status >= 500:
                            raise aiohttp.ClientResponseError(
                                resp.request_info, resp.history, status=resp.status,
                                message=f"server error {resp.status}",
                            )
                        resp.raise_for_status()
                        data = await resp.read()
                self.stats.downloads += 1
                self.stats.download_bytes += len(data)
                return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt > self._max_retries:
                    log.warning("Download failed after %d attempts: %s", attempt, exc)
                    raise
                self.stats.retries += 1
                delay = self._backoff(attempt)
                log.debug("Transient download error (%s); retry %d/%d in %.1fs",
                          exc, attempt, self._max_retries, delay)
                await asyncio.sleep(delay)
