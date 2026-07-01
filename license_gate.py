"""License verification gate for the Icon & Saints data layer.

The gate runs at ingestion time, per record, *before* anything is committed as
servable. It is the enforcement point for this project's hard constraint: the
product must never serve content it isn't licensed to serve.

Design rules (from the PRD, non-negotiable):

1. **Fail closed.** Anything the gate can't positively classify is
   ``quarantined`` (stored for audit, never surfaced), never ``approved``.
2. **Attribution is mandatory** for every ``approved`` record — even PD/CC0
   sources carry sourcing (trust/UX), and CC BY sources legally require it.
3. **Source-level license changes invalidate cached approvals.** That bit is
   enforced by the pipeline/storage (re-flag on ``base_license`` change), but
   the gate fails closed defensively if a source row no longer looks verified.
4. **Images and text are licensed independently.** This module only clears
   *image* records; bios stay withheld until ``saints.bio_license`` is set.

The gate is intentionally a pure, synchronous classifier over a normalized
:class:`~icon_sources.RawRecord` — no I/O, easy to reason about and test. Human
overrides (``license_overrides`` table) are applied by the pipeline around the
gate, so automated and manual decisions stay separate and auditable.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional, Set

log = logging.getLogger("ortho_scraper.license_gate")

# crawl_status values, mirrored by the icons.crawl_status CHECK constraint.
PENDING = "pending_license_check"
APPROVED = "approved"
QUARANTINED = "quarantined"
REJECTED = "rejected"
CRAWL_STATUSES = (PENDING, APPROVED, QUARANTINED, REJECTED)


@dataclass
class Source:
    """A configured ingest source, mirrored into the ``sources`` table.

    ``allowed_licenses`` is the per-item allowlist consulted for sources that
    set ``requires_per_item_check`` (Met/Wikimedia). For blanket-grant sources
    (ICONSAINT) it is unused and ``base_license`` carries the grant.
    """
    name: str
    base_license: str
    attribution_template: Optional[str] = None
    requires_per_item_check: bool = True
    allowed_licenses: Set[str] = field(default_factory=set)
    notes: Optional[str] = None


@dataclass
class GateResult:
    """Outcome of evaluating one record. ``status`` is a crawl_status value."""
    status: str
    license: Optional[str] = None
    attribution: Optional[str] = None
    reason: Optional[str] = None

    @property
    def is_approved(self) -> bool:
        return self.status == APPROVED


def _render_attribution(template: Optional[str], default: str, **fields) -> str:
    """Render a source's attribution template, falling back to ``default``.

    Missing/empty fields render as "Unknown" so a malformed template can never
    silently drop required credit. An unknown placeholder in the template falls
    back to the default rather than raising mid-ingest.
    """
    safe = {k: (v if v not in (None, "") else "Unknown") for k, v in fields.items()}
    if not template:
        return default
    try:
        return template.format(**safe)
    except (KeyError, IndexError, ValueError):
        log.warning("Attribution template %r has unknown placeholders; using default.",
                    template)
        return default


class LicenseGate:
    """Classifies raw records into approved / quarantined / rejected."""

    def evaluate(self, source: Source, record) -> GateResult:
        """Dispatch to the per-source classifier. Unknown source → rejected."""
        if source.name == "met_api":
            return self._check_met(source, record)
        # Wikipedia-category images are Commons-hosted with the same per-file
        # extmetadata license tags, so the Commons check applies verbatim.
        if source.name in ("wikimedia", "wikipedia"):
            return self._check_wikimedia(source, record)
        if source.name == "wikiart":
            return self._check_wikiart(source, record)
        if source.name == "iconsaint":
            return self._check_iconsaint(source, record)
        if source.name == "openverse":
            return self._check_openverse(source, record)
        return GateResult(status=REJECTED, reason="unknown_source")

    # -- WikiArt: stub — fail-closed until the license signal is known. ---------
    def _check_wikiart(self, source: Source, record) -> GateResult:
        # ponytail: stub. No API access yet to learn WikiArt's license/date fields,
        # and its terms disclaim copyright (mostly in-copyright / fair-use). So
        # everything quarantines: the adapter still crawls all records, the read
        # side serves only 'approved', and items are promoted via license_overrides
        # / per-source policy. When a positive signal exists (e.g. artist
        # death-year + 70 < now, or a PD flag), add the auto-approve path here —
        # mirror _check_met's is_public_domain shape and return APPROVED with a
        # rendered attribution.
        return GateResult(status=QUARANTINED, reason="wikiart_license_unverified")

    # -- Met Open Access: public domain per-object; never bulk-assume. ---------
    def _check_met(self, source: Source, record) -> GateResult:
        if record.license_signal.get("is_public_domain") is True:
            attribution = _render_attribution(
                source.attribution_template,
                default=f"The Metropolitan Museum of Art, {record.source_record_id}",
                source_record_id=record.source_record_id,
                author=record.license_signal.get("author"),
                license="Public Domain",
                source="The Metropolitan Museum of Art",
                title=record.title,
            )
            return GateResult(status=APPROVED, license="PD", attribution=attribution)
        return GateResult(status=QUARANTINED, reason="not_flagged_public_domain")

    # -- Wikimedia Commons: per-file license tag against the allowlist. --------
    def _check_wikimedia(self, source: Source, record) -> GateResult:
        tag = record.license_signal.get("license_short")
        if tag and tag in source.allowed_licenses:
            attribution = _render_attribution(
                source.attribution_template,
                default=(f"{record.license_signal.get('author') or 'Unknown'}, "
                         f"via Wikimedia Commons, {tag}"),
                author=record.license_signal.get("author"),
                license=tag,
                source="Wikimedia Commons",
                source_record_id=record.source_record_id,
                title=record.title,
            )
            return GateResult(status=APPROVED, license=tag, attribution=attribution)
        return GateResult(status=QUARANTINED,
                          reason=f"unrecognized_or_restrictive_license:{tag or 'none'}")

    # -- ICONSAINT: blanket CC BY grant verified at the source level. ----------
    def _check_iconsaint(self, source: Source, record) -> GateResult:
        # Defensive: if the source row was ever edited to revoke the verified
        # CC BY grant, fail closed rather than trust a stale approval.
        if not source.base_license.upper().startswith("CC-BY"):
            return GateResult(status=QUARANTINED, reason="source_license_not_verified")
        attribution = _render_attribution(
            source.attribution_template,
            default=(
                "Icon image from the ICONSAINT dataset (Sidiropoulos, Apostolidis, "
                "Vrochidou & Papakostas, 2026, MLV Research Group, Democritus "
                "University of Thrace), CC BY. https://doi.org/10.3390/info17040340"
            ),
            author="MLV Research Group, Democritus University of Thrace",
            license=source.base_license,
            source="ICONSAINT dataset",
            source_record_id=record.source_record_id,
            title=record.title,
        )
        return GateResult(status=APPROVED, license=source.base_license,
                          attribution=attribution)

    # -- Openverse: per-record license code against the allowlist. -------------
    def _check_openverse(self, source: Source, record) -> GateResult:
        lic = record.license_signal.get("license")
        if lic and lic in source.allowed_licenses:
            # Openverse always builds a correctly-sourced attribution string
            # (creator/license/original source) for every result; prefer it over
            # a synthesized default, but still let attribution_template override.
            attribution = _render_attribution(
                source.attribution_template,
                default=record.license_signal.get("attribution"),
                author=record.license_signal.get("author"),
                license=lic,
                source="Openverse",
                source_record_id=record.source_record_id,
                title=record.title,
            )
            return GateResult(status=APPROVED, license=lic, attribution=attribution)
        return GateResult(status=QUARANTINED,
                          reason=f"unrecognized_or_restrictive_license:{lic or 'none'}")
