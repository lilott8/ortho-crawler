# Saints & Icons redesign — multi-source claims, one ledger

Status: **multi-source merge working end-to-end.** Two real bio sources
(Wikipedia spine + OrthodoxWiki enrichment) flow through the claims ledger and
reducer onto QID-identified saints, with fail-closed licensing, per-type
overrides, multi-valued aliases, Wikidata feast days, image→saint linkage, and a
coverage report. All verified in `test_saint_claims.py` plus live probes. The
remaining work is breadth (more fields/sources, multi-feast, human review
tooling), not the core mechanism.

> **Resolver caveat:** free-text label → QID via `wbsearchentities` takes the
> top hit, which is occasionally the wrong entity (e.g. a label resolved to a
> namesake). This is exactly why ICONSAINT linkage and the OrthodoxWiki QID
> fallback are *low-trust* — they never mis-attach (a non-matching QID is
> dropped) but recall is bounded. Tighten with a Wikidata "instance of:
> human/saint" filter or a `title → QID` override table if needed.

## Implementation status

### Implemented & tested
- **Identity** — `saints.qid` (Wikidata QID), unique-when-set; `qid IS NULL` =
  needs-review. `upsert_saint_by_qid` adopts/backfills name-only rows (merge).
- **Claims ledger** — `saint_claims` table; `set_claims` (replace a source's
  whole contribution to a field) + `add_claim` (1-value convenience);
  `recompute_saint` materializes winners into `saints.*`, fail-closed.
- **Reducers** — per-field, weight-ordered; scalar `take_top`, multi-valued
  `union` (dedup, order-preserving → JSON). `FACT_FIELDS` (dates/names) servable
  without a license; everything else needs a cleared license.
- **Wikipedia producer** — `fetch_saint_records`: QID + lead-extract bio
  (CC BY-SA, attributed) + Wikidata aliases → `alt_names` (multi-valued) +
  Wikidata feast day (`P841` → calendar-day item → MM-DD, cached) → `feast_day`
  (a fact). Populating `saints.feast_day` lets the notify job's
  `sync_recurring_events` finally materialize feast events.
- **OrthodoxWiki enrichment** — reuses crawled `pages`; matches by name/alias
  then QID fallback; `clean_wikitext_lead`; CC BY-SA 2.5 bio at
  `orthodoxwiki_weight` (50). Never seeds.
- **Image→saint linkage** — `icon_pipeline._resolve_saint` resolves a record's
  label to a QID and links on a clean hit only; else image kept, link → review.
- **Licensing overrides** — per-type `license_policies` (config), most-specific
  wins, attribution mandatory; precedence: per-record DB → per-type → gate.
- **Visibility** — `storage.coverage()` + `--mode stats` (saints with bio,
  needs-review, icons linked/orphan, claims).
- **Migrations** — both backends in lockstep (`qid` column; ledger UNIQUE key
  incl. `value`; SQLite ledger rebuild; Postgres constraint swap).

### Remaining
- **Multi-feast / per-jurisdiction feast days.** `feast_day` is materialized as
  a *single* MM-DD (first `P841` value). Saints with several feast traditions
  lose the rest. The ledger + reducer already support multi-valued fields, so
  the work is: add `feast_day` to `MULTI_VALUED_FIELDS`, change `saints.feast_day`
  to a JSON array (or add `feast_days`), and teach `sync_recurring_events` to
  emit an event per date. *Pick up here:* `saint_sources.fetch_wikidata_facts`
  already breaks after the first `P841` — collect all, then thread the list
  through `SaintRecord.feast_day` → `set_claims`.
- **Feast coverage depends on which QID we resolved.** Wikidata completeness
  varies across "same saint" entities (observed: `Q43216` has no `P841`, the
  `wbsearchentities` hit `Q43706` does). This is the same QID-agreement issue as
  the OrthodoxWiki fallback; a curated `qid → feast` override table is the
  eventual backstop.
- **Met/Commons stay image-only.** Their adapters still emit `saint_name=None`,
  so their icons are valid but orphaned (no saint link). Converting them to emit
  a saint signal (or a depicts/`P180`-based hint) is unimplemented.
- **Saint `description` field** — not modeled on `saints`; only `bio` and
  `feast_day` have materialized homes today.
- **Needs-review workflow** — the pile is *counted* (`--mode stats`) but there's
  no tooling to work it down (resolve a QID, merge, or reject); today it's the
  per-type overrides + manual SQL.
- **OrthodoxWiki QID-fallback recall** — safe but low (see the enrichment
  section's caveat); a `title → QID` override table is the deferred fix.
- **README** — the new modes/knobs are documented in `scraper.conf` and here,
  but the README's saints/icons section is not yet updated.

### Deliberately not done (over-engineering avoided)
- **Generic `Claim | Artifact` producer-sink rewrite of `icon_pipeline`** — the
  value (images aware of saints) is achieved with the targeted `_resolve_saint`
  change; a full rewrite is churn without new behavior.
- **Unified in-run `RunStats` object** — both pipelines log consistent
  summaries; `--mode stats` gives the cross-pipeline view. Merge only if they
  diverge.
- **Read-time merge / a metrics-history table** — rejected during design (see
  the licensing and visibility sections).

## Problem (what's wrong today)

- `upsert_saint()` writes only `canonical_name`. Nothing ever populates
  `bio_text`/`feast_day`/`description`. `bio_text` is withheld while
  `bio_license IS NULL` (always). → **thousands of bare, content-less saints.**
- Saints come only from low-trust seeds: `saint_sources.py` scrapes Wikipedia
  *list articles* for names, and ICONSAINT contributes image-class labels.
  Met/Commons icons hardcode `saint_name=None` → orphan icons, attached to no
  saint.
- Identity is exact `canonical_name` UNIQUE. "St. John Chrysostom" ≠
  "John Chrysostom". No alias resolution, no merge.
- Sources never meet: each writes its own rows. No "additive algebra" — signals
  about the same saint stay "unlinked and unaware."
- Two divergent stats objects (`RunStats`, `IconRunStats`) + `ClientStats`. No
  unified logging; can't answer "why is this saint empty?"

## The model

Producers stay diverse; **they unify at a common sink (the claims ledger), not
a common crawler.** Two records about John Chrysostom become aware of each other
when they land as claims on the same Wikidata QID — not because one engine
fetched them.

```
producers ──emit──> Claim | Artifact ──> sink ──> ledger ──recompute──> saints.*
  wiki crawler          |                  │  (resolve QID)      (reducers,
  wikipedia             |                  │   route             write winners)
  commons / met / iconsaint               └── Artifact ─> gate ─> icons
```

### Identity
- **Wikidata QID is the saint's identity and Wikipedia is the source of truth.**
  Wikipedia seeds the universe (one saint per article, *arriving with a bio*).
  QID resolves aliases for free.
- Anything that can't resolve to a QID → **needs-review** (kept, audited, never
  served, counted in coverage). Never wrongly fused.

### Producers (uniform contract)
```
Producer.items() -> AsyncIterator[Claim | Artifact]
  Claim    = (saint_hint, field, value, source, weight, license, attribution)
  Artifact = (saint_hint, image_origin, source, license_signal)   # -> gate -> icons
  saint_hint = QID if known, else a raw label/title to resolve
```
- A producer emits any mix. Met → Artifacts only. Wikipedia → Claims (+maybe
  Artifacts). ICONSAINT → Artifact (image) + a **low-weight** depicts-saint
  Claim. Wiki crawler → Claims (a thin emitter bolted onto the existing crawl;
  **the crawl engine is not rewritten**).
- Producers never know about each other. They run **independently, on their own
  cadence**. Reduction is a separate, idempotent `recompute(touched_saints)`.
- "Modes" become source-selection + which post-steps run — not separate
  pipelines.

### The merge — per-field reducers (additive, not skyline)  *(built)*
Storage is uniform; **cardinality is a reducer policy, not a storage split.**
- scalar field (`bio_text`, `description`) → `take_top(weight)`
- multi-valued field (`alt_names`, per-jurisdiction `feast_day`) →
  `union, ordered by weight, dedup, drop nothing` (materialized as a JSON array)

`FACT_FIELDS` (uncopyrightable: dates, names) are servable without a license;
`MULTI_VALUED_FIELDS` keep the whole ordered set. The ledger's UNIQUE key is
`(saint_id, field, source_id, value)` and the writer (`set_claims`) replaces a
source's whole contribution to a field, so a re-ingest handles updated *and*
removed values for both cardinalities. First multi-valued producer: Wikidata
aliases → `alt_names`.
- Simple per-`(source, field)` weights. ICONSAINT carries a *high* weight for
  image artifacts and a *low* weight for saint linkage — same machinery, two
  weights for its two emissions. Its image always becomes a valid licensed icon;
  its saint-link auto-attaches only on a clean QID hit, else needs-review.

We explicitly **do not** build a skyline/Pareto engine. A scalar slot must pick
one winner, which is a total order (weight), not a Pareto frontier.

### The ledger
- One uniform table holds **every textual claim**:
  `(saint_id, field, value, source_id, weight, license, attribution, observed_at)`.
- **Images are excepted** — they remain first-class in `icons` (gated binaries,
  content-addressed, sidecar'd). A claim is a text assertion; an image is a file.
- **Write-time materialization, fail-closed.** Reducers write winners into
  `saints.*`; the read path is unchanged (still reads `saints.bio_text`).
  Recompute is idempotent — re-run when a weight/source/license changes.

### Licensing (the hard constraint)
- The gate **extends to text claims**. Reducers materialize **only
  license-cleared claims** into servable fields. An uncleared bio claim stays in
  the ledger (visible/audited) but `saints.bio_text` stays `NULL` — same
  fail-closed shape as icons.
- **Facts are exempt:** `feast_day` and bare dates are uncopyrightable, always
  servable.
- Wikipedia bios clear as CC BY-SA *because attribution is captured*.
- **Override precedence:** per-record (DB `license_overrides`, exists) →
  **per-type policy** (NEW: HOCON `(target_type, source?, field?)` wildcard
  block, most-specific-wins) → automated gate. Attribution stays **mandatory**
  on any approval; an override can widen what's cleared, never drop attribution.

### Visibility (the headline complaint)
- **One unified runtime `RunStats`** replaces the two divergent stats classes:
  per-producer + per-phase counters, summary banner, progress lines. Logging is
  an axis independent of merge-location.
- **Coverage is SQL over the ledger** — "saints with ≥1 cleared bio",
  "needs-review count", "per-source win-rate", "image-but-no-text". A
  `--mode stats` / `report()`. **No history table, no metrics platform** until
  trends are actually wanted.

## Migration
- Add `saints.qid` (nullable, UNIQUE-when-set). `canonical_name` demotes from
  identity to display/search. Both backends in lockstep, project migration
  pattern (`ADD COLUMN IF NOT EXISTS` / guarded `PRAGMA` + `ALTER`).
- One-time resolver over existing labels → QID. Resolved labels **merge** into
  the Wikipedia-seeded saint (images re-linked). Unresolved → **needs-review
  stubs**, counted in coverage.

## Accepted risks / open implementation details
- **Risk:** a large fraction of ICONSAINT's noisy labels won't auto-resolve and
  will sit in needs-review. Acceptable — the images are still valid icons; only
  their saint-links wait.
- Weight config shape → per-`(source, field)` (ICONSAINT already needs split
  weights).
- Needs-review resolution workflow → per-type overrides + a coverage query are
  the starting tools.
- `saints.qid` backfill/merge-collision mechanics → settle at build time.

## First build slice (prove the spine end-to-end)

Goal: **one real saint goes Wikipedia → QID → bio claim → ledger → recompute →
servable `saints.bio_text` with attribution**, before touching any other source.

1. **Schema (both backends, lockstep):** add `saints.qid`; add `saint_claims`
   ledger table `(saint_id, field, value, source_id, weight, license,
   attribution, observed_at)` with a uniqueness key on
   `(saint_id, field, source_id)`.
2. **Storage:** `add_claim(...)`, `recompute_saint(saint_id)` (runs the per-field
   reducers, materializes winners + `bio_source_id`/`bio_license` from the
   winning cleared claim), `upsert_saint_by_qid(qid, display_name)`.
3. **Wikipedia producer:** extend `saint_sources.py` from "names only" to: for
   each saint, fetch the QID (`pageprops.wikibase_item`) + lead/extract bio +
   CC BY-SA attribution; emit a `bio` Claim. Resolve/seed the saint by QID.
4. **Gate:** add a text-claim path (CC BY-SA cleared with attribution; facts
   exempt).
5. **Recompute + verify:** run against SQLite throwaway config on a handful of
   saints; confirm `bio_text` populated, `bio_license` set, attribution present,
   and the coverage query shows "N saints with cleared bio > 0".

Only after this spine is green: convert Met/Commons/ICONSAINT adapters to the
`Claim | Artifact` producer contract, add weights + per-type overrides, fold the
wiki crawler's emitter in, and unify `RunStats`.

## OrthodoxWiki enrichment  *(built)*

The saints job runs a second pass after the Wikipedia spine: it reads the pages
`--mode wiki` already crawled, matches each to an existing saint, extracts a
plain-text lead from the wikitext (`clean_wikitext_lead`, a heuristic cleaner),
and emits a CC BY-SA 2.5 bio claim at `orthodoxwiki_weight` (50, below
Wikipedia's 100). It **never seeds** saints — an unmatched page is skipped.

Matching is two-tier:
1. **By name/alias** (no HTTP) — normalize the page title and look it up against
   each saint's canonical name *and* Wikidata `alt_names`. Fast, free, lossy.
2. **By QID** (fallback, one resolve per name-miss) — resolve the page title via
   Wikidata `wbsearchentities` and match the QID against already-seeded saints.
   High precision: a wrong resolve just fails to match (the QID isn't ours), so
   it never mis-attaches; it only *recovers* pages whose titles diverge from
   every known name. The `N matched (X by name, Y by QID)` log line shows the
   split.

So a saint in both sources gets Wikipedia's bio (OrthodoxWiki sits in the ledger
as a fallback); a saint Wikipedia left without a bio gets OrthodoxWiki's. This is
the first real exercise of the cross-source scalar merge.

> **QID-fallback recall caveat.** The fallback resolves a page title with
> Wikidata `wbsearchentities` (top hit), but that QID does **not always agree**
> with the `pageprops.wikibase_item` QID we *seed* saints from. Observed live:
> `"Ioannes Chrysostomos" → Q43706`, `"John Chrysostom" → Q2630714`, while the
> seeded saint is `Q43216`. Consequence: the fallback is **safe but
> low-recall** — it never mis-attaches (a non-matching QID is simply dropped),
> but it only *recovers* a page when its resolved QID happens to equal the
> seeded one. Recovery rate is corpus-dependent.
>
> Why not resolve it "properly": the robust fix would resolve each OrthodoxWiki
> page through its own Wikidata sitelink, but OrthodoxWiki pages carry no
> Wikidata ID, so `wbsearchentities` is the only lever without a hand-maintained
> mapping. **Next step if recall proves insufficient** (watch the `by QID` count
> in the `[saints] OrthodoxWiki: …` log line): a small persisted `title → QID`
> override table for the stubborn cases — same shape as the per-record
> `license_overrides`. Deferred until the log shows it's needed.

## Deferred / remaining

See the **Implementation status** section at the top — "Remaining" lists the
outstanding work (feast_day producer, Met/Commons saint signal, description
field, needs-review tooling, QID-fallback recall, README) and "Deliberately not
done" lists what was intentionally avoided as over-engineering (generic
producer-sink rewrite, unified in-run `RunStats`, read-time merge).
