from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from rights_validator import RightsValidator
from storage import AssetRecord, Storage


LOGGER = logging.getLogger(__name__)
VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm", ".avi", ".m4v"}


class SourceProvider(ABC):
    name: str

    def __init__(self, settings: dict[str, Any]) -> None:
        self.settings = settings

    @abstractmethod
    def search(self, query: str, limit: int) -> list[AssetRecord]:
        raise NotImplementedError


class WikimediaCommonsSource(SourceProvider):
    name = "wikimedia_commons"

    def search(self, query: str, limit: int) -> list[AssetRecord]:
        params = {
            "action": "query",
            "format": "json",
            "generator": "search",
            "gsrsearch": f"{query} filetype:video",
            "gsrnamespace": "6",
            "gsrlimit": str(limit),
            "prop": "imageinfo|info",
            "inprop": "url",
            "iiprop": "url|user|mime|extmetadata",
            "iiurlwidth": "320",
        }
        url = "https://commons.wikimedia.org/w/api.php?" + urlencode(params)
        payload = request_json(url)
        pages = payload.get("query", {}).get("pages", {})
        results: list[AssetRecord] = []
        for page in pages.values():
            info = (page.get("imageinfo") or [{}])[0]
            ext_meta = info.get("extmetadata", {})
            results.append(
                AssetRecord(
                    source_name=self.name,
                    source_asset_id=str(page.get("pageid")),
                    source_url=page.get("canonicalurl") or info.get("descriptionurl", ""),
                    title=clean_title(page.get("title", "")),
                    creator=extract_extmetadata_value(ext_meta, "Artist") or info.get("user", ""),
                    license_type=extract_extmetadata_value(ext_meta, "LicenseShortName"),
                    license_url=extract_extmetadata_value(ext_meta, "LicenseUrl"),
                    duration_seconds=coerce_float(extract_extmetadata_value(ext_meta, "PlaytimeSeconds")),
                    tags=extract_wikimedia_tags(page),
                    thumbnail_url=info.get("thumburl", ""),
                    media_url=info.get("url", ""),
                )
            )
        return results


class InternetArchiveSource(SourceProvider):
    name = "internet_archive"

    def search(self, query: str, limit: int) -> list[AssetRecord]:
        safe_query = query.replace('"', " ")
        lucene = (
            f'mediatype:(movies) AND ({safe_query}) AND '
            '(licenseurl:("publicdomain" OR "creativecommons.org/licenses/by" OR "creativecommons.org/publicdomain"))'
        )
        params = {
            "q": lucene,
            "fl[]": ["identifier", "title", "creator", "licenseurl", "description"],
            "sort[]": "downloads desc",
            "rows": str(limit),
            "output": "json",
        }
        url = "https://archive.org/advancedsearch.php?" + urlencode(params, doseq=True)
        payload = request_json(url)
        docs = payload.get("response", {}).get("docs", [])
        results: list[AssetRecord] = []
        for doc in docs:
            identifier = str(doc.get("identifier", ""))
            if not identifier:
                continue
            license_url = doc.get("licenseurl", "")
            media_url = ""
            try:
                media_url = lookup_archive_media_url(identifier)
            except Exception as exc:
                LOGGER.warning("Internet Archive media lookup failed for %s: %s", identifier, exc)
            results.append(
                AssetRecord(
                    source_name=self.name,
                    source_asset_id=identifier,
                    source_url=f"https://archive.org/details/{quote(identifier)}",
                    title=doc.get("title") or identifier,
                    creator=doc.get("creator", ""),
                    license_type=license_from_url(license_url),
                    license_url=license_url,
                    tags=tags_from_text(doc.get("description", "")),
                    thumbnail_url=f"https://archive.org/services/img/{quote(identifier)}",
                    media_url=media_url,
                )
            )
        return results


class PexelsSource(SourceProvider):
    name = "pexels"

    def search(self, query: str, limit: int) -> list[AssetRecord]:
        api_key = os.environ.get("PEXELS_API_KEY")
        if not api_key:
            raise RuntimeError("PEXELS_API_KEY is not set.")
        params = urlencode({"query": query, "per_page": str(limit), "orientation": "portrait"})
        url = f"https://api.pexels.com/videos/search?{params}"
        payload = request_json(url, headers={"Authorization": api_key})
        results: list[AssetRecord] = []
        for item in payload.get("videos", []):
            video_files = item.get("video_files") or []
            best_file = next((file for file in video_files if file.get("quality") == "sd"), None) or (video_files[0] if video_files else {})
            results.append(
                AssetRecord(
                    source_name=self.name,
                    source_asset_id=str(item.get("id")),
                    source_url=item.get("url", ""),
                    title=f"Pexels video {item.get('id')}",
                    creator=(item.get("user") or {}).get("name", ""),
                    license_type="Pexels License",
                    license_url="https://www.pexels.com/license/",
                    duration_seconds=coerce_float(item.get("duration")),
                    tags=tags_from_text(query),
                    thumbnail_url=item.get("image", ""),
                    media_url=best_file.get("link", ""),
                )
            )
        return results


class PixabaySource(SourceProvider):
    name = "pixabay"

    def search(self, query: str, limit: int) -> list[AssetRecord]:
        api_key = os.environ.get("PIXABAY_API_KEY")
        if not api_key:
            raise RuntimeError("PIXABAY_API_KEY is not set.")
        params = urlencode({"key": api_key, "q": query, "per_page": str(limit)})
        url = f"https://pixabay.com/api/videos/?{params}"
        payload = request_json(url)
        results: list[AssetRecord] = []
        for item in payload.get("hits", []):
            videos = item.get("videos") or {}
            medium = videos.get("medium") or videos.get("small") or {}
            results.append(
                AssetRecord(
                    source_name=self.name,
                    source_asset_id=str(item.get("id")),
                    source_url=item.get("pageURL", ""),
                    title=f"Pixabay video {item.get('id')}",
                    creator=item.get("user", ""),
                    license_type="Pixabay Content License",
                    license_url="https://pixabay.com/service/license-summary/",
                    duration_seconds=coerce_float(medium.get("duration")),
                    tags=[tag.strip() for tag in str(item.get("tags", "")).split(",") if tag.strip()],
                    thumbnail_url=medium.get("thumbnail", ""),
                    media_url=medium.get("url", ""),
                )
            )
        return results


class LocalFolderSource(SourceProvider):
    def __init__(self, settings: dict[str, Any], *, name: str) -> None:
        super().__init__(settings)
        self.name = name

    def search(self, query: str, limit: int) -> list[AssetRecord]:
        folder = Path(self.settings.get("folder", "")).expanduser()
        if not folder.exists():
            return []
        query_lower = query.lower().strip()
        results: list[AssetRecord] = []
        for path in sorted(folder.rglob("*")):
            if path.suffix.lower() not in VIDEO_EXTENSIONS or not path.is_file():
                continue
            searchable = path.stem.lower()
            if query_lower and query_lower not in searchable:
                continue
            source_id = hashlib.sha1(str(path.resolve()).encode("utf-8")).hexdigest()
            title = path.stem.replace("_", " ").replace("-", " ")
            results.append(
                AssetRecord(
                    source_name=self.name,
                    source_asset_id=source_id,
                    source_url=path.resolve().as_uri(),
                    title=title,
                    creator="Local owner",
                    license_type="Owned media" if self.name == "local_owned" else "AI-generated owned media",
                    license_url="",
                    duration_seconds=None,
                    tags=tags_from_text(title),
                    thumbnail_url="",
                    media_url="",
                    local_path=str(path.resolve()),
                )
            )
            if len(results) >= limit:
                break
        return results


def request_json(url: str, headers: dict[str, str] | None = None) -> dict[str, Any]:
    request = Request(
        url,
        headers={"User-Agent": "RightsSafeTikTokTool/1.0", **(headers or {})},
    )
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(f"HTTP error while requesting {url}: {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error while requesting {url}: {exc.reason}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON returned by {url}") from exc


def discover_assets(
    *,
    config: dict[str, Any],
    storage: Storage,
    validator: RightsValidator,
    query: str,
    per_source_limit: int,
) -> list[AssetRecord]:
    results: list[AssetRecord] = []
    candidate_results: list[tuple[float, AssetRecord]] = []
    for provider in build_sources(config):
        try:
            discovered = provider.search(query=query, limit=per_source_limit)
        except Exception as exc:
            storage.log_event(
                "warning",
                "search_failed",
                f"{provider.name} search failed: {exc}",
                details={"provider": provider.name, "query": query},
            )
            continue

        for asset in discovered:
            allowed, filter_reason = passes_content_filters(asset, config)
            if not allowed:
                storage.log_event(
                    "info",
                    "asset_filtered",
                    f"Asset skipped by content filters: {filter_reason}",
                    details={
                        "source_name": asset.source_name,
                        "source_asset_id": asset.source_asset_id,
                        "title": asset.title,
                        "query": query,
                    },
                )
                continue
            decision = validator.validate(asset)
            asset.rights_status = decision.status
            asset.rights_reason = decision.reason
            asset.attribution_required = decision.attribution_required
            asset.attribution_text = decision.attribution_text
            asset.source_attribution = decision.attribution_text
            asset.proposed_caption = build_caption(asset, config)
            asset.proposed_hashtags = build_hashtags(asset, config)
            candidate_results.append((compute_viral_score(asset, config), asset))

    candidate_results.sort(key=lambda item: item[0], reverse=True)
    for score, asset in candidate_results:
        stored = storage.upsert_asset(asset)
        storage.log_event(
            "info",
            "asset_discovered",
            (
                f"Stored asset {stored.title} from {stored.source_name} "
                f"with status {stored.rights_status} and viral score {score:.2f}."
            ),
            asset_id=stored.asset_id,
            details={"query": query, "source_url": stored.source_url, "viral_score": score},
        )
        if stored.rights_status in {"unknown", "rejected"}:
            storage.log_event(
                "info",
                "asset_skipped",
                f"Asset blocked by rights policy: {stored.rights_reason}",
                asset_id=stored.asset_id,
            )
        results.append(stored)
    return results


def build_sources(config: dict[str, Any]) -> list[SourceProvider]:
    source_config = config.get("sources", {})
    providers: list[SourceProvider] = []
    for name, settings in source_config.items():
        if not settings.get("enabled", False):
            continue
        if name == "wikimedia_commons":
            providers.append(WikimediaCommonsSource(settings))
        elif name == "internet_archive":
            providers.append(InternetArchiveSource(settings))
        elif name == "pexels":
            providers.append(PexelsSource(settings))
        elif name == "pixabay":
            providers.append(PixabaySource(settings))
        elif name == "local_owned":
            providers.append(LocalFolderSource(settings, name="local_owned"))
        elif name == "local_ai":
            providers.append(LocalFolderSource(settings, name="local_ai"))
    return providers


def build_caption(asset: AssetRecord, config: dict[str, Any]) -> str:
    template = (
        config.get("captions", {}).get("template")
        or "{title} | Source: {source_name}"
    )
    rendered = template.format(
        title=asset.title,
        creator=asset.creator or "Unknown creator",
        source_name=asset.source_name,
        tags=", ".join(asset.tags[:5]),
    ).strip()
    cta = config.get("captions", {}).get("call_to_action", "").strip()
    if cta:
        rendered = f"{rendered} | {cta}"
    return rendered[:150]


def build_hashtags(asset: AssetRecord, config: dict[str, Any]) -> str:
    configured = config.get("captions", {}).get("default_hashtags", [])
    tag_pool = [normalize_hashtag(tag) for tag in configured]
    tag_pool.extend(normalize_hashtag(tag) for tag in asset.tags[:4])
    unique = []
    seen = set()
    for tag in tag_pool:
        if not tag or tag in seen:
            continue
        unique.append(tag)
        seen.add(tag)
    return " ".join(unique[:8])


def normalize_hashtag(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "", value)
    return f"#{cleaned}" if cleaned else ""


def passes_content_filters(asset: AssetRecord, config: dict[str, Any]) -> tuple[bool, str]:
    filters = config.get("content_filters", {})
    excluded_sources = {value.lower() for value in filters.get("excluded_sources", [])}
    if asset.source_name.lower() in excluded_sources:
        return False, f"Source {asset.source_name} is excluded."

    max_duration = filters.get("max_duration_seconds")
    if max_duration and asset.duration_seconds and asset.duration_seconds > float(max_duration):
        return False, f"Duration {asset.duration_seconds:.1f}s exceeds the {max_duration}s limit."

    metadata_text = build_metadata_text(asset)
    if filters.get("require_english_metadata", False):
        if contains_non_english_signal(metadata_text):
            return False, "Metadata does not look English-first."

    preferred_countries = [normalize_country_code(value) for value in filters.get("preferred_countries", [])]
    detected_country = detect_country_hint(metadata_text)
    reject_nonpreferred = filters.get("reject_known_nonpreferred_countries", False)
    if preferred_countries and detected_country and detected_country not in preferred_countries and reject_nonpreferred:
        return False, f"Detected country {detected_country} is outside the preferred market list."

    required_keywords = [value.strip().lower() for value in filters.get("required_keywords_any", []) if value.strip()]
    if required_keywords:
        if not any(keyword in metadata_text.lower() for keyword in required_keywords):
            return False, "Asset did not match any required discovery keywords."

    return True, ""


def compute_viral_score(asset: AssetRecord, config: dict[str, Any]) -> float:
    filters = config.get("content_filters", {})
    keywords = [value.lower() for value in filters.get("viral_keywords", [])]
    metadata_text = build_metadata_text(asset).lower()
    score = 0.0

    if asset.duration_seconds:
        if asset.duration_seconds <= 20:
            score += 2.0
        elif asset.duration_seconds <= 40:
            score += 1.5
        elif asset.duration_seconds <= 60:
            score += 1.0

    score += min(2.5, sum(0.5 for keyword in keywords if keyword and keyword in metadata_text))

    if asset.source_name.lower() in {"pexels", "pixabay", "local_owned", "local_ai"}:
        score += 1.5
    elif asset.source_name.lower() == "internet_archive":
        score += 0.5

    if detect_country_hint(metadata_text) in {
        normalize_country_code(value) for value in filters.get("preferred_countries", [])
    }:
        score += 1.0

    if not contains_non_english_signal(metadata_text):
        score += 0.75

    return score


def build_metadata_text(asset: AssetRecord) -> str:
    return " ".join(
        value
        for value in [
            asset.title,
            asset.creator,
            asset.license_type,
            " ".join(asset.tags),
            asset.source_url,
        ]
        if value
    )


def contains_non_english_signal(value: str) -> bool:
    if re.search(r"[\u0400-\u04FF\u0600-\u06FF\u4E00-\u9FFF\u3040-\u30FF\uAC00-\uD7AF]", value):
        return True
    lowered = value.lower()
    return any(
        token in lowered
        for token in [
            " español",
            " deutsch",
            " français",
            " português",
            " italiano",
            " русский",
            " 한국",
            "日本語",
            "中文",
        ]
    )


def detect_country_hint(value: str) -> str:
    lowered = value.lower()
    hints = {
        "US": [" united states", " usa", " u.s.", " american", " america"],
        "GB": [" united kingdom", " uk", " britain", " british", " england", " london"],
        "CA": [" canada", " canadian"],
        "AU": [" australia", " australian"],
        "NZ": [" new zealand"],
        "IE": [" ireland", " irish"],
    }
    for code, patterns in hints.items():
        if any(pattern in lowered for pattern in patterns):
            return code
    return ""


def normalize_country_code(value: str) -> str:
    cleaned = value.strip().upper()
    if cleaned == "UK":
        return "GB"
    return cleaned


def extract_extmetadata_value(ext_meta: dict[str, Any], key: str) -> str:
    value = ext_meta.get(key, {})
    if isinstance(value, dict):
        return strip_html(str(value.get("value", "")))
    return strip_html(str(value))


def strip_html(value: str) -> str:
    return re.sub(r"<[^>]+>", "", value).strip()


def clean_title(value: str) -> str:
    return value.replace("File:", "").strip()


def tags_from_text(value: str) -> list[str]:
    candidates = re.split(r"[^a-zA-Z0-9]+", value)
    return [part.lower() for part in candidates if len(part) > 2][:10]


def extract_wikimedia_tags(page: dict[str, Any]) -> list[str]:
    title = clean_title(page.get("title", ""))
    return tags_from_text(title)


def license_from_url(license_url: str) -> str:
    value = license_url.lower()
    if "publicdomain" in value:
        return "Public Domain"
    if "creativecommons.org/publicdomain/zero" in value:
        return "CC0"
    if "creativecommons.org/licenses/by-sa" in value:
        return "CC BY-SA"
    if "creativecommons.org/licenses/by" in value:
        return "CC BY"
    return license_url


def lookup_archive_media_url(identifier: str) -> str:
    url = f"https://archive.org/metadata/{quote(identifier)}"
    payload = request_json(url)
    for file_info in payload.get("files", []):
        name = str(file_info.get("name", ""))
        lower_name = name.lower()
        if not any(lower_name.endswith(ext) for ext in VIDEO_EXTENSIONS):
            continue
        if file_info.get("private"):
            continue
        return f"https://archive.org/download/{quote(identifier)}/{quote(name)}"
    return ""


def coerce_float(value: Any) -> float | None:
    try:
        if value in ("", None):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
