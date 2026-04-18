from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from storage import AssetRecord


ALLOWED_UPLOAD_STATUSES = {
    "public_domain",
    "cc_commercial",
    "licensed_stock",
    "owned",
    "ai_owned",
}


@dataclass
class RightsDecision:
    status: str
    reason: str
    attribution_required: bool
    attribution_text: str


class RightsValidator:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        source_config = config.get("sources", {})
        self.stock_sources = {
            name
            for name, settings in source_config.items()
            if settings.get("enabled") and settings.get("category") == "licensed_stock"
        }

    def validate(self, asset: AssetRecord) -> RightsDecision:
        source_name = asset.source_name.lower()
        license_value = (asset.license_type or "").lower()

        if source_name == "local_owned":
            return RightsDecision(
                status="owned",
                reason="Local owned media folder is explicitly trusted.",
                attribution_required=False,
                attribution_text="",
            )

        if source_name == "local_ai":
            return RightsDecision(
                status="ai_owned",
                reason="Local AI-generated media folder is explicitly trusted.",
                attribution_required=False,
                attribution_text="",
            )

        if source_name in self.stock_sources:
            if not asset.license_type:
                return RightsDecision(
                    status="unknown",
                    reason="Licensed provider entry is missing a license label.",
                    attribution_required=False,
                    attribution_text="",
                )
            return RightsDecision(
                status="licensed_stock",
                reason="Matched an approved licensed stock provider.",
                attribution_required=False,
                attribution_text="",
            )

        if self._contains_any(license_value, ["public domain", "cc0", "pdm", "no known copyright"]):
            return RightsDecision(
                status="public_domain",
                reason="Source metadata indicates public-domain reuse.",
                attribution_required=False,
                attribution_text="",
            )

        if "creativecommons.org" in (asset.license_url or "").lower() or "cc " in license_value or license_value.startswith("cc"):
            if self._contains_any(license_value, ["nc", "noncommercial"]):
                return RightsDecision(
                    status="rejected",
                    reason="Creative Commons license forbids commercial reuse.",
                    attribution_required=False,
                    attribution_text="",
                )
            if self._contains_any(license_value, ["nd", "no derivatives"]):
                return RightsDecision(
                    status="rejected",
                    reason="No-derivatives licenses are incompatible with trimming, resizing, and overlays.",
                    attribution_required=False,
                    attribution_text="",
                )
            attribution_text = self._build_attribution(asset)
            return RightsDecision(
                status="cc_commercial",
                reason="Creative Commons terms appear to allow commercial reuse and derivatives.",
                attribution_required=True,
                attribution_text=attribution_text,
            )

        if self._contains_any(license_value, ["all rights reserved", "copyright", "editorial only"]):
            return RightsDecision(
                status="rejected",
                reason="Copyrighted or editorial-only media cannot be reused here.",
                attribution_required=False,
                attribution_text="",
            )

        if asset.source_name.lower() in {"internet_archive", "wikimedia_commons"} and not asset.license_type:
            return RightsDecision(
                status="unknown",
                reason="Source metadata did not include usable license terms.",
                attribution_required=False,
                attribution_text="",
            )

        return RightsDecision(
            status="unknown",
            reason="License terms are unclear, so the asset must be skipped.",
            attribution_required=False,
            attribution_text="",
        )

    @staticmethod
    def is_upload_allowed(status: str) -> bool:
        return status in ALLOWED_UPLOAD_STATUSES

    @staticmethod
    def _contains_any(value: str, fragments: list[str]) -> bool:
        return any(fragment in value for fragment in fragments)

    @staticmethod
    def _build_attribution(asset: AssetRecord) -> str:
        creator = asset.creator or "Unknown creator"
        license_name = asset.license_type or "Creative Commons"
        return f"{asset.title} by {creator} via {asset.source_name} ({license_name}) {asset.source_url}".strip()
