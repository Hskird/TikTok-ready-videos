from __future__ import annotations

import json
import math
import mimetypes
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from storage import AssetRecord


YOUTUBE_UPLOAD_SCOPE = "https://www.googleapis.com/auth/youtube.upload"
TIKTOK_API_BASE = "https://open.tiktokapis.com"


@dataclass
class UploadRequest:
    asset: AssetRecord
    video_path: Path
    title: str = ""
    caption: str = ""
    description: str = ""
    hashtags: str = ""
    attribution: str = ""
    dry_run: bool = False
    consent_confirmed: bool = True
    platform_overrides: dict[str, Any] = field(default_factory=dict)


@dataclass
class UploadResult:
    success: bool
    uploader_name: str
    external_post_id: str = ""
    error_message: str = ""


class Uploader(ABC):
    name: str

    @abstractmethod
    def upload(self, request: UploadRequest) -> UploadResult:
        raise NotImplementedError


class MockUploader(Uploader):
    name = "mock"

    def upload(self, request: UploadRequest) -> UploadResult:
        label = "dryrun" if request.dry_run else "mock"
        return UploadResult(
            success=True,
            uploader_name=self.name,
            external_post_id=f"{label}-{request.asset.asset_id or request.asset.source_asset_id}",
        )


class MultiPlatformUploader(Uploader):
    name = "multi_platform"

    def __init__(self, uploaders: list[Uploader]) -> None:
        self.uploaders = uploaders

    def upload(self, request: UploadRequest) -> UploadResult:
        results: dict[str, dict[str, Any]] = {}
        failures: list[str] = []
        for uploader in self.uploaders:
            result = uploader.upload(request)
            results[uploader.name] = {
                "success": result.success,
                "external_post_id": result.external_post_id,
                "error_message": result.error_message,
            }
            if not result.success:
                failures.append(f"{uploader.name}: {result.error_message or 'upload failed'}")
        return UploadResult(
            success=not failures,
            uploader_name=self.name,
            external_post_id=json.dumps(results),
            error_message="; ".join(failures),
        )


class TikTokOfficialUploader(Uploader):
    name = "tiktok_official"

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.settings = config.get("social_posting", {}).get("tiktok", {})

    def upload(self, request: UploadRequest) -> UploadResult:
        if request.dry_run:
            return UploadResult(
                success=True,
                uploader_name=self.name,
                external_post_id=f"dryrun-tiktok-{request.asset.source_asset_id}",
            )
        if not request.consent_confirmed:
            return UploadResult(
                success=False,
                uploader_name=self.name,
                error_message="TikTok upload requires explicit user consent before posting.",
            )

        access_token = self.settings.get("access_token", "")
        if not access_token:
            return UploadResult(
                success=False,
                uploader_name=self.name,
                error_message="Missing TikTok access token. Set TIKTOK_ACCESS_TOKEN.",
            )

        video_path = request.video_path.resolve()
        if not video_path.exists():
            return UploadResult(
                success=False,
                uploader_name=self.name,
                error_message=f"Video file not found: {video_path}",
            )

        video_size = video_path.stat().st_size
        chunk_size, total_chunks = choose_tiktok_chunk_plan(video_size)
        title = build_tiktok_title(request)
        init_body = {
            "post_info": {
                "title": title,
                "privacy_level": self.settings.get("privacy_level", "SELF_ONLY"),
                "disable_comment": bool(self.settings.get("disable_comment", False)),
                "disable_duet": bool(self.settings.get("disable_duet", False)),
                "disable_stitch": bool(self.settings.get("disable_stitch", False)),
                "video_cover_timestamp_ms": int(self.settings.get("video_cover_timestamp_ms", 1000)),
            },
            "source_info": {
                "source": "FILE_UPLOAD",
                "video_size": video_size,
                "chunk_size": chunk_size,
                "total_chunk_count": total_chunks,
            },
        }
        init_response = self._request_json(
            f"{TIKTOK_API_BASE}/v2/post/publish/video/init/",
            access_token=access_token,
            payload=init_body,
        )
        upload_url = init_response.get("data", {}).get("upload_url", "")
        publish_id = init_response.get("data", {}).get("publish_id", "")
        if not upload_url or not publish_id:
            return UploadResult(
                success=False,
                uploader_name=self.name,
                error_message=f"TikTok init failed: {json.dumps(init_response)}",
            )

        try:
            upload_file_to_tiktok(upload_url, video_path, chunk_size, total_chunks)
        except Exception as exc:
            return UploadResult(
                success=False,
                uploader_name=self.name,
                error_message=f"TikTok media upload failed: {exc}",
            )

        return UploadResult(
            success=True,
            uploader_name=self.name,
            external_post_id=publish_id,
        )

    def _request_json(self, url: str, *, access_token: str, payload: dict[str, Any]) -> dict[str, Any]:
        request = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; charset=UTF-8",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"TikTok HTTP {exc.code}: {details}") from exc


class YouTubeOfficialUploader(Uploader):
    name = "youtube_official"

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.settings = config.get("social_posting", {}).get("youtube", {})

    def upload(self, request: UploadRequest) -> UploadResult:
        if request.dry_run:
            return UploadResult(
                success=True,
                uploader_name=self.name,
                external_post_id=f"dryrun-youtube-{request.asset.source_asset_id}",
            )

        client_secrets_file = self.settings.get("client_secrets_file", "")
        token_file = self.settings.get("token_file", "")
        if not client_secrets_file or not Path(client_secrets_file).exists():
            return UploadResult(
                success=False,
                uploader_name=self.name,
                error_message="Missing YouTube OAuth client secrets file.",
            )
        if not token_file:
            return UploadResult(
                success=False,
                uploader_name=self.name,
                error_message="Missing YouTube token file path in config.",
            )

        try:
            from google.auth.transport.requests import Request as GoogleRequest
            from google.oauth2.credentials import Credentials
            from google_auth_oauthlib.flow import InstalledAppFlow
            from googleapiclient.discovery import build
            from googleapiclient.http import MediaFileUpload
        except ImportError as exc:
            return UploadResult(
                success=False,
                uploader_name=self.name,
                error_message=(
                    "Missing Google API client libraries. "
                    "Install google-api-python-client, google-auth-oauthlib, and google-auth-httplib2."
                ),
            )

        credentials = None
        token_path = Path(token_file)
        if token_path.exists():
            credentials = Credentials.from_authorized_user_file(str(token_path), [YOUTUBE_UPLOAD_SCOPE])
        if not credentials or not credentials.valid:
            if credentials and credentials.expired and credentials.refresh_token:
                credentials.refresh(GoogleRequest())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    client_secrets_file,
                    [YOUTUBE_UPLOAD_SCOPE],
                )
                credentials = flow.run_local_server(port=0)
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text(credentials.to_json(), encoding="utf-8")

        youtube = build("youtube", "v3", credentials=credentials)

        hashtags = normalize_hashtags(request.hashtags)
        title_suffix = self.settings.get("title_suffix", " #shorts")
        title = (request.title or request.caption or request.asset.title).strip()
        if title_suffix and not title.lower().endswith(title_suffix.lower()):
            title = f"{title}{title_suffix}"
        description = build_description(request)
        tags = [tag.lstrip("#") for tag in hashtags]

        body = {
            "snippet": {
                "title": title[:100],
                "description": description[:5000],
                "tags": tags[:15],
                "categoryId": str(self.settings.get("category_id", "24")),
            },
            "status": {
                "privacyStatus": self.settings.get("privacy_status", "private"),
                "selfDeclaredMadeForKids": bool(self.settings.get("self_declared_made_for_kids", True)),
            },
        }

        media = MediaFileUpload(str(request.video_path.resolve()), chunksize=-1, resumable=True)
        insert_request = youtube.videos().insert(
            part="snippet,status",
            body=body,
            media_body=media,
        )

        response = None
        while response is None:
            _, response = insert_request.next_chunk()

        video_id = response.get("id", "")
        if not video_id:
            return UploadResult(
                success=False,
                uploader_name=self.name,
                error_message=f"YouTube upload returned no video id: {response}",
            )

        return UploadResult(
            success=True,
            uploader_name=self.name,
            external_post_id=video_id,
        )


def build_uploader(config: dict[str, Any], override_name: str | None = None) -> Uploader:
    uploader_name = override_name or config.get("uploader", {}).get("name", "mock")
    if uploader_name == "mock":
        return MockUploader()
    if uploader_name == "tiktok_official":
        return TikTokOfficialUploader(config)
    if uploader_name == "youtube_official":
        return YouTubeOfficialUploader(config)
    if uploader_name == "multi_platform":
        platform_names = config.get("social_posting", {}).get("default_platforms", ["youtube", "tiktok"])
        uploaders: list[Uploader] = []
        for platform_name in platform_names:
            if platform_name == "youtube":
                uploaders.append(YouTubeOfficialUploader(config))
            elif platform_name == "tiktok":
                uploaders.append(TikTokOfficialUploader(config))
        if not uploaders:
            raise ValueError("No platforms configured for multi_platform uploader.")
        return MultiPlatformUploader(uploaders)
    raise ValueError(f"Unsupported uploader: {uploader_name}")


def build_tiktok_title(request: UploadRequest) -> str:
    title = request.caption or request.description or request.title or request.asset.title
    hashtags = request.hashtags.strip()
    combined = f"{title} {hashtags}".strip()
    return combined[:2200]


def build_description(request: UploadRequest) -> str:
    parts = [request.description.strip()]
    if request.hashtags.strip():
        parts.append(request.hashtags.strip())
    if request.attribution.strip():
        parts.append(f"Attribution: {request.attribution.strip()}")
    return "\n\n".join(part for part in parts if part)


def normalize_hashtags(value: str) -> list[str]:
    return [tag for tag in value.replace("\n", " ").split(" ") if tag.startswith("#")]


def choose_tiktok_chunk_plan(video_size: int) -> tuple[int, int]:
    min_chunk = 5 * 1024 * 1024
    max_chunk = 64 * 1024 * 1024
    if video_size <= max_chunk:
        return video_size, 1

    chunk_size = 10 * 1024 * 1024
    remainder = video_size % chunk_size
    if remainder != 0 and remainder < min_chunk:
        total_chunks = max(1, video_size // chunk_size)
    else:
        total_chunks = math.ceil(video_size / chunk_size)
    return chunk_size, total_chunks


def upload_file_to_tiktok(upload_url: str, video_path: Path, chunk_size: int, total_chunks: int) -> None:
    mime_type = mimetypes.guess_type(str(video_path))[0] or "video/mp4"
    total_size = video_path.stat().st_size
    with video_path.open("rb") as handle:
        start = 0
        for chunk_index in range(total_chunks):
            if chunk_index == total_chunks - 1:
                chunk = handle.read()
            else:
                chunk = handle.read(chunk_size)
            if not chunk:
                break
            end = start + len(chunk) - 1
            request = urllib.request.Request(
                upload_url,
                data=chunk,
                method="PUT",
                headers={
                    "Content-Type": mime_type,
                    "Content-Length": str(len(chunk)),
                    "Content-Range": f"bytes {start}-{end}/{total_size}",
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=300):
                    pass
            except urllib.error.HTTPError as exc:
                details = exc.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"TikTok upload chunk failed with HTTP {exc.code}: {details}") from exc
            start = end + 1
