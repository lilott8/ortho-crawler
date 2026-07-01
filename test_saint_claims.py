"""Self-check for the saint claims ledger + reducer (slice 1).

Offline (no network): drives the SQLite backend through the money paths —
identity by QID, additive bio claim, fail-closed licensing, fact exemption,
needs-review merge. Run: ./.venv/bin/python test_saint_claims.py
"""

from __future__ import annotations

import asyncio
import os
import tempfile

import json
from types import SimpleNamespace

from config import (Corrections, DatabaseConfig, LicensePolicy, SaintsConfig,
                    select_policy)
from icon_pipeline import IconPipeline, IconRunStats
from icon_sources import RawRecord
from saint_sources import (_parse_month_day, build_name_index,
                           clean_wikitext_lead, normalize_name)
from storage import reduce_claims, materialized_saint_columns
from storage_sqlite import SqliteStorage


def test_reducer_pure():
    # Cleared low-weight bio beats uncleared high-weight bio (fail closed).
    claims = [
        {"field": "bio", "value": "good", "source_id": 1, "weight": 10,
         "license": "CC-BY-SA-4.0", "attribution": "a"},
        {"field": "bio", "value": "unlicensed", "source_id": 2, "weight": 99,
         "license": None, "attribution": None},
        # feast_day is a fact: servable with no license.
        {"field": "feast_day", "value": "11-13", "source_id": 1, "weight": 1,
         "license": None, "attribution": None},
    ]
    winners = reduce_claims(claims)
    assert winners["bio"][0]["value"] == "good", winners
    assert "feast_day" in winners and winners["feast_day"][0]["value"] == "11-13"


def test_multi_valued_reducer():
    # alt_names: union ordered by weight (desc), dedup preserving first occurrence.
    claims = [
        {"field": "alt_names", "value": "Chrysostom", "source_id": 1, "weight": 50,
         "license": None, "attribution": None},
        {"field": "alt_names", "value": "Golden Mouth", "source_id": 2, "weight": 100,
         "license": None, "attribution": None},
        {"field": "alt_names", "value": "Chrysostom", "source_id": 2, "weight": 100,
         "license": None, "attribution": None},   # duplicate from a heavier source
    ]
    cols = materialized_saint_columns(reduce_claims(claims))
    assert json.loads(cols["alt_names"]) == ["Golden Mouth", "Chrysostom"], cols["alt_names"]


async def test_storage():
    tmp = os.path.join(tempfile.mkdtemp(), "t.db")
    db = await SqliteStorage.connect(DatabaseConfig(backend="sqlite", path=tmp))
    await db.apply_schema()

    wiki = await db.upsert_source("wikipedia", "CC-BY-SA-4.0", None, False)
    sketch = await db.upsert_source("sketchy", "unknown", None, True)

    # Identity by QID is idempotent.
    sid = await db.upsert_saint_by_qid("Q43216", "John Chrysostom")
    assert await db.upsert_saint_by_qid("Q43216", "John Chrysostom") == sid

    # Licensed bio materializes; an unlicensed higher-weight bio does NOT win.
    await db.add_claim(sid, "bio", "Archbishop of Constantinople...", wiki.source_id,
                       100, "CC-BY-SA-4.0", "\"John Chrysostom\", Wikipedia")
    await db.add_claim(sid, "bio", "scraped junk", sketch.source_id,
                       999, None, None)
    await db.add_claim(sid, "feast_day", "11-13", wiki.source_id, 100, None, None)
    await db.recompute_saint(sid)

    row = await _saint(db, sid)
    assert row["bio_text"].startswith("Archbishop"), row["bio_text"]
    assert row["bio_license"] == "CC-BY-SA-4.0", row["bio_license"]
    assert row["bio_source_id"] == wiki.source_id
    assert json.loads(row["feast_day"]) == ["11-13"]   # fact, multi-valued JSON array

    # Withdraw the license on the winning bio -> slot clears (fail closed).
    await db.add_claim(sid, "bio", "Archbishop...", wiki.source_id, 100, None, None)
    await db.recompute_saint(sid)
    row = await _saint(db, sid)
    assert row["bio_text"] is None and row["bio_license"] is None, row["bio_text"]

    # needs-review merge: a name-only saint is adopted by a later QID resolve.
    nid = await db.upsert_saint("Mary of Egypt")          # qid NULL
    assert await db.upsert_saint_by_qid("Q236189", "Mary of Egypt") == nid
    merged = await _saint(db, nid)
    assert merged["qid"] == "Q236189", merged["qid"]

    total, with_bio = await db.saint_bio_coverage()
    assert total == 2, total
    assert with_bio == 0, with_bio   # we withdrew Chrysostom's license above

    await db.close()


def test_policy_specificity():
    policies = [
        LicensePolicy(target_type="icon", decision="rejected"),                    # wildcard
        LicensePolicy(target_type="icon", decision="approved", source="iconsaint"),
        LicensePolicy(target_type="saint", decision="approved", source="wikipedia",
                      field="bio"),
    ]
    # Most-specific wins: source-scoped beats the wildcard.
    assert select_policy(policies, "icon", "iconsaint", None).decision == "approved"
    assert select_policy(policies, "icon", "met_api", None).decision == "rejected"
    assert select_policy(policies, "saint", "wikipedia", "bio").decision == "approved"
    assert select_policy(policies, "saint", "wikipedia", "feast_day") is None


async def test_coverage():
    tmp = os.path.join(tempfile.mkdtemp(), "c.db")
    db = await SqliteStorage.connect(DatabaseConfig(backend="sqlite", path=tmp))
    await db.apply_schema()
    await db.upsert_saint_by_qid("Q1", "Resolved Saint")     # has qid
    await db.upsert_saint("Unresolved Saint")                # qid NULL -> needs-review
    c = await db.coverage()
    assert c["saints_total"] == 2 and c["saints_needs_review"] == 1, c
    assert c["icons_total"] == 0 and c["claims_total"] == 0, c
    assert c["saints_with_feast"] == 0, c           # feast coverage is reported
    await db.close()


async def test_icon_saint_linkage():
    """ICONSAINT label resolves to a QID -> linked; no hit -> image kept, link
    left for review (image storage is independent of saint_id)."""
    import icon_pipeline
    async def fake_resolve(label, http):
        return "Q43216" if "Chrysostom" in label else None
    orig = icon_pipeline.resolve_qid
    icon_pipeline.resolve_qid = fake_resolve
    try:
        tmp = os.path.join(tempfile.mkdtemp(), "i.db")
        db = await SqliteStorage.connect(DatabaseConfig(backend="sqlite", path=tmp))
        await db.apply_schema()
        # Bypass __init__ (which needs a full Config) — wire only what we test.
        # Icons link only to an already-seeded saint — create it first.
        await db.upsert_source("wikipedia", "CC-BY-SA-4.0", None, False)
        await db.upsert_saint_by_qid("Q43216", "John Chrysostom")
        pipe = IconPipeline.__new__(IconPipeline)
        pipe._db = db
        pipe._http = object()
        pipe._saint_cache = {}
        pipe._stats = IconRunStats()

        hit = RawRecord(source="iconsaint", source_record_id="a", title="t",
                        saint_name="John Chrysostom")
        miss = RawRecord(source="iconsaint", source_record_id="b", title="t",
                         saint_name="Nonexistent Person")
        assert await pipe._resolve_saint(hit) is not None
        assert await pipe._resolve_saint(miss) is None        # needs-review, not linked
        assert pipe._stats.saints == 1 and pipe._stats.unresolved == 1
        # Cached None must not re-resolve (the `name in cache` path).
        assert await pipe._resolve_saint(miss) is None
        assert pipe._stats.unresolved == 1
        await db.close()
    finally:
        icon_pipeline.resolve_qid = orig


async def test_multi_valued_storage():
    """set_claims for a multi-valued field: union materializes; shrinking the set
    removes values; emptying it clears the column (all via re-ingest)."""
    tmp = os.path.join(tempfile.mkdtemp(), "m.db")
    db = await SqliteStorage.connect(DatabaseConfig(backend="sqlite", path=tmp))
    await db.apply_schema()
    assert await db._unique_covers_value("saint_claims")   # fresh schema is multi-valued
    src = await db.upsert_source("wikipedia", "CC-BY-SA-4.0", None, False)
    sid = await db.upsert_saint_by_qid("Q43216", "John Chrysostom")

    await db.set_claims(sid, "alt_names", ["Chrysostom", "Golden Mouth"],
                        src.source_id, 100, None, None)
    await db.recompute_saint(sid)
    row = await _saint(db, sid)
    assert json.loads(row["alt_names"]) == ["Chrysostom", "Golden Mouth"], row["alt_names"]

    # Re-ingest a smaller set: the dropped alias must disappear.
    await db.set_claims(sid, "alt_names", ["Chrysostom"], src.source_id, 100, None, None)
    await db.recompute_saint(sid)
    row = await _saint(db, sid)
    assert json.loads(row["alt_names"]) == ["Chrysostom"], row["alt_names"]

    # Empty set clears the column entirely.
    await db.set_claims(sid, "alt_names", [], src.source_id, 100, None, None)
    await db.recompute_saint(sid)
    row = await _saint(db, sid)
    assert row["alt_names"] is None, row["alt_names"]
    await db.close()


def test_feast_day_parse():
    assert _parse_month_day("13 November") == "11-13"
    assert _parse_month_day("November 13") == "11-13"
    assert _parse_month_day("January 27") == "01-27"
    assert _parse_month_day("nonsense") is None
    assert _parse_month_day("") is None


def test_guess_saint_name():
    from icon_sources import guess_saint_name
    assert guess_saint_name("File:Icon of Saint Nicholas.jpg") == "Saint Nicholas"
    assert guess_saint_name("St. George and the Dragon") == "St. George"
    assert guess_saint_name("Random landscape painting.jpg") is None
    assert guess_saint_name(None) is None


async def test_multi_feast_storage():
    """feast_day is multi-valued: several dates materialize as a JSON array, and
    sync_recurring_events expands them into one event per date."""
    tmp = os.path.join(tempfile.mkdtemp(), "f.db")
    db = await SqliteStorage.connect(DatabaseConfig(backend="sqlite", path=tmp))
    await db.apply_schema()
    src = await db.upsert_source("wikipedia", "CC-BY-SA-4.0", None, False)
    sid = await db.upsert_saint_by_qid("Q1", "Twice-Feasted Saint")
    await db.set_claims(sid, "feast_day", ["11-13", "01-27"], src.source_id, 100, None, None)
    await db.recompute_saint(sid)
    row = await _saint(db, sid)
    assert json.loads(row["feast_day"]) == ["11-13", "01-27"], row["feast_day"]

    assert await db.sync_recurring_events() == 2          # one event per date
    assert len(await db.due_recurring_events("11-13")) == 1
    assert len(await db.due_recurring_events("01-27")) == 1
    await db.close()


def test_wikitext_cleaner():
    raw = ("'''John Chrysostom''' was an [[Archbishop]] of "
           "{{lang|Constantinople}}.<ref>cite</ref>\n"
           "[[Category:Saints]]\n==Life==\nlots more text")
    lead = clean_wikitext_lead(raw)
    assert "Archbishop" in lead, lead
    assert "Constantinople" not in lead          # template dropped
    assert "==" not in lead and "[[" not in lead and "'''" not in lead
    assert "more text" not in lead               # stopped at the heading
    assert clean_wikitext_lead("{{stub}}") is None  # nothing servable


async def test_orthodoxwiki_enrichment():
    """OrthodoxWiki bio is a lower-weight enrichment claim: it sits in the ledger
    behind Wikipedia, and only wins the bio slot if Wikipedia's is withdrawn."""
    tmp = os.path.join(tempfile.mkdtemp(), "e.db")
    db = await SqliteStorage.connect(DatabaseConfig(backend="sqlite", path=tmp))
    await db.apply_schema()
    wiki = await db.upsert_source("wikipedia", "CC-BY-SA-4.0", None, False)
    owiki = await db.upsert_source("orthodoxwiki", "CC-BY-SA-2.5", None, False)
    sid = await db.upsert_saint_by_qid("Q43216", "John Chrysostom")
    await db.set_claims(sid, "alt_names", ["Chrysostom"], wiki.source_id, 100, None, None)
    await db.add_claim(sid, "bio", "Wikipedia bio.", wiki.source_id, 100,
                       "CC-BY-SA-4.0", "wp")
    await db.recompute_saint(sid)

    # The name index (canonical + alias) is how a crawled page finds the saint.
    index = build_name_index(await db.all_saint_names())
    assert index.get(normalize_name("St. John Chrysostom")) == sid   # canonical + honorific
    assert index.get(normalize_name("Chrysostom")) == sid            # alias match

    lead = clean_wikitext_lead("'''John''' was an [[Archbishop]].\n==Life==\nx")
    await db.add_claim(sid, "bio", lead, owiki.source_id, 50, "CC-BY-SA-2.5", "ow")
    await db.recompute_saint(sid)
    row = await _saint(db, sid)
    assert row["bio_text"] == "Wikipedia bio.", row["bio_text"]      # WP outranks
    assert row["bio_source_id"] == wiki.source_id

    # Withdraw Wikipedia's bio -> OrthodoxWiki's enrichment claim takes over.
    await db.set_claims(sid, "bio", [], wiki.source_id, 100, None, None)
    await db.recompute_saint(sid)
    row = await _saint(db, sid)
    assert row["bio_source_id"] == owiki.source_id, row["bio_source_id"]
    assert "Archbishop" in row["bio_text"], row["bio_text"]
    await db.close()


async def test_orthodoxwiki_qid_match():
    """A page whose title matches no name/alias still attaches via QID resolution,
    and only because the resolved QID equals an already-seeded saint."""
    import main
    async def fake_resolve(title, http):
        return "Q43216" if title == "Ioannes Chrysostomos" else None
    orig = main.resolve_qid
    main.resolve_qid = fake_resolve
    try:
        tmp = os.path.join(tempfile.mkdtemp(), "q.db")
        db = await SqliteStorage.connect(DatabaseConfig(backend="sqlite", path=tmp))
        await db.apply_schema()
        await db.upsert_source("wikipedia", "CC-BY-SA-4.0", None, False)
        sid = await db.upsert_saint_by_qid("Q43216", "John Chrysostom")
        # A title that won't normalize-match "John Chrysostom" or any alias.
        await db._conn.execute(
            "INSERT INTO pages (pageid, title, url, namespace, content, attribution, "
            "first_seen, last_seen) VALUES (1, 'Ioannes Chrysostomos', 'http://x', 0, "
            "'He was a [[bishop]].', 'OrthodoxWiki', 't', 't')")
        await db._conn.commit()

        cfg = SimpleNamespace(
            scraper=SimpleNamespace(user_agent="t/0.1 (a@b.c)"),
            saints=SaintsConfig(orthodoxwiki_weight=50),
            corrections=Corrections())
        await main._enrich_from_orthodoxwiki(cfg, db)

        async with db._conn.execute(
            "SELECT s.name FROM saint_claims c JOIN sources s ON s.id = c.source_id "
            "WHERE c.field='bio' AND c.saint_id=?", (sid,)) as cur:
            sources = {r["name"] for r in await cur.fetchall()}
        assert "orthodoxwiki" in sources, sources    # linked via QID despite name miss
        await db.close()
    finally:
        main.resolve_qid = orig


async def test_description_and_needs_review():
    """description is a core-data fact (servable with no license); the
    needs-review worklist query returns only qid-NULL saints."""
    tmp = os.path.join(tempfile.mkdtemp(), "d.db")
    db = await SqliteStorage.connect(DatabaseConfig(backend="sqlite", path=tmp))
    await db.apply_schema()
    src = await db.upsert_source("wikipedia", "CC-BY-SA-4.0", None, False)
    sid = await db.upsert_saint_by_qid("Q1", "Saint One")
    await db.add_claim(sid, "description", "A church father.", src.source_id,
                       100, None, None)            # no license -> still served (fact)
    await db.recompute_saint(sid)
    row = await _saint(db, sid)
    assert row["description"] == "A church father.", row["description"]

    await db.upsert_saint("Unresolved One")        # qid stays NULL -> needs-review
    names = await db.needs_review_saints(10)
    assert "Unresolved One" in names and "Saint One" not in names, names
    await db.close()


async def test_curated_feast_correction():
    """A curated feast correction is applied at CURATED_WEIGHT: its date sorts
    first (authoritative), additively with any Wikipedia feast."""
    from config import Corrections
    from main import _apply_feast_corrections, SaintRunStats
    tmp = os.path.join(tempfile.mkdtemp(), "cf.db")
    db = await SqliteStorage.connect(DatabaseConfig(backend="sqlite", path=tmp))
    await db.apply_schema()
    wiki = await db.upsert_source("wikipedia", "CC-BY-SA-4.0", None, False)
    curated = await db.upsert_source("curated", "curated", None, False)
    sid = await db.upsert_saint_by_qid("Q1", "Saint One")
    await db.set_claims(sid, "feast_day", ["05-05"], wiki.source_id, 100, None, None)

    corr = Corrections(feast={"Q1": ["11-13"]})
    await _apply_feast_corrections(db, corr, curated.source_id, SaintRunStats())
    days = json.loads((await _saint(db, sid))["feast_day"])
    assert days[0] == "11-13", days          # curated first (top weight)
    assert "05-05" in days, days             # additive — Wikipedia date retained
    await db.close()


def test_http_calls_go_through_shared_retry_helper():
    """Guard against the saint_sources.py timeout crash recurring: a bare
    ``session.get()`` skips the retry/backoff + asyncio.TimeoutError handling
    that icon_sources._HttpJson (and mediawiki.MediaWikiClient._get) provide,
    so one slow response can kill an entire run. Only those two files — plus
    icon_pipeline.py's byte-download (streaming/ETag, not JSON, guarded by a
    broad except at its call site) — are allowed to call it directly.
    """
    allowed = {"mediawiki.py", "icon_sources.py", "icon_pipeline.py"}
    here = os.path.dirname(os.path.abspath(__file__))
    offenders = []
    for name in os.listdir(here):
        if name.endswith(".py") and name not in allowed and not name.startswith("test_"):
            with open(os.path.join(here, name)) as fh:
                if "session.get(" in fh.read():
                    offenders.append(name)
    assert not offenders, (
        f"{offenders} call session.get() directly, bypassing retry/timeout "
        "handling. Route through icon_sources._HttpJson instead (see how "
        "saint_sources.py does it).")


def test_corrections_config():
    from config import load_config
    conf = (
        'scraper { api_url = "https://x/api.php", user_agent = "t (a@b.c)" }\n'
        'database { backend = "sqlite", path = "x.db" }\n'
        'corrections {\n'
        '  saint_qid = [ { name = "Foo", qid = "Q1" } ]\n'
        '  feast = [ { qid = "Q1", days = ["11-13", "01-27"] } ]\n'
        '  owiki_qid = [ { title = "Bar", qid = "Q2" } ]\n'
        '}\n')
    p = os.path.join(tempfile.mkdtemp(), "c.conf")
    with open(p, "w") as fh:
        fh.write(conf)
    c = load_config(p)
    assert c.corrections.saint_qid == {"Foo": "Q1"}, c.corrections.saint_qid
    assert c.corrections.feast == {"Q1": ["11-13", "01-27"]}, c.corrections.feast
    assert c.corrections.owiki_qid == {"Bar": "Q2"}, c.corrections.owiki_qid


async def _saint(db, sid):
    async with db._conn.execute("SELECT * FROM saints WHERE id = ?", (sid,)) as cur:
        return await cur.fetchone()


if __name__ == "__main__":
    test_reducer_pure()
    test_multi_valued_reducer()
    test_policy_specificity()
    test_feast_day_parse()
    test_guess_saint_name()
    test_wikitext_cleaner()
    test_http_calls_go_through_shared_retry_helper()
    test_corrections_config()
    asyncio.run(test_storage())
    asyncio.run(test_coverage())
    asyncio.run(test_multi_valued_storage())
    asyncio.run(test_multi_feast_storage())
    asyncio.run(test_description_and_needs_review())
    asyncio.run(test_curated_feast_correction())
    asyncio.run(test_orthodoxwiki_enrichment())
    asyncio.run(test_orthodoxwiki_qid_match())
    asyncio.run(test_icon_saint_linkage())
    print("OK")
