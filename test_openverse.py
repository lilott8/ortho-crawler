"""Self-check for the Openverse icon source (gate + adapter pagination/dedup).

Offline (no network): fakes _HttpJson.get to drive OpenverseAdapter.records()
and exercises license_gate's approve/quarantine paths.
Run: ./.venv/bin/python test_openverse.py
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from config import IconSourceConfig
from icon_sources import OpenverseAdapter
from license_gate import APPROVED, QUARANTINED, LicenseGate, Source


class _FakeHttp:
    def __init__(self, pages):
        self._pages = pages  # list of response dicts, one per get() call
        self.calls = []

    async def get(self, url, params=None):
        self.calls.append(params)
        return self._pages.pop(0)


def test_adapter_dedup_and_pagination():
    cfg = IconSourceConfig(
        name="openverse", queries=["Byzantine icon"], max_objects=10,
        allowed_licenses=["cc0", "by"],
    )
    page1 = {
        "page_count": 2,
        "results": [
            {"id": "a", "title": "Icon of Saint Nicholas", "url": "http://x/a.jpg",
             "license": "cc0", "attribution": "attr a", "creator": "Someone"},
            {"id": "b", "title": "Byzantine mosaic", "url": "http://x/b.jpg",
             "license": "by", "attribution": "attr b", "creator": "Other"},
        ],
    }
    page2 = {
        "page_count": 2,
        "results": [
            {"id": "a", "title": "duplicate of a", "url": "http://x/a.jpg",
             "license": "cc0", "attribution": "attr a", "creator": "Someone"},
            {"id": "c", "title": "Theotokos icon", "url": "http://x/c.jpg",
             "license": "by-nc", "attribution": "attr c", "creator": "Third"},
        ],
    }
    http = _FakeHttp([page1, page2])
    adapter = OpenverseAdapter(cfg, http)

    async def run():
        return [r async for r in adapter.records()]

    records = asyncio.run(run())
    ids = [r.source_record_id for r in records]
    assert ids == ["a", "b", "c"], ids  # dedup dropped the repeated "a"
    assert records[0].saint_name == "Saint Nicholas", records[0].saint_name
    assert http.calls[0]["license"] == "cc0,by"  # allowed_licenses passed through


def test_gate_approves_allowed_and_quarantines_nc():
    gate = LicenseGate()
    source = Source(name="openverse", base_license="UNVERIFIED",
                     allowed_licenses={"cc0", "by"})

    ok = SimpleNamespace(
        source_record_id="a", title="Icon",
        license_signal={"license": "cc0", "attribution": "prebuilt attr",
                        "author": "Someone"})
    result = gate.evaluate(source, ok)
    assert result.status == APPROVED, result
    assert result.attribution == "prebuilt attr"  # Openverse's own string wins

    nc = SimpleNamespace(
        source_record_id="b", title="Icon",
        license_signal={"license": "by-nc", "attribution": "x", "author": "y"})
    result = gate.evaluate(source, nc)
    assert result.status == QUARANTINED, result


if __name__ == "__main__":
    test_adapter_dedup_and_pagination()
    test_gate_approves_allowed_and_quarantines_nc()
    print("OK")
