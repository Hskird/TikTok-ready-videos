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
            decision = validator.validate(asset)
            asset.rights_status = decision.status
            asset.rights_reason = decision.reason
            asset.attribution_required = decision.attribution_required
            asset.attribution_text = decision.attribution_text
            asset.source_attribution = decision.attribution_text
            asset.proposed_caption = build_caption(asset, config)
            asset.proposed_hashtags = build_hashtags(asset, config)
            stored = storage.upsert_asset(asset)
            storage.log_event(
                "info",
                "asset_discovered",
                f"Stored asset {stored.title} from {stored.source_name} with status {stored.rights_status}.",
                asset_id=stored.asset_id,
                details={"query": query, "source_url": stored.source_url},
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
