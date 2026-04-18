from __future__ import annotations

import hashlib
import logging
import mimetypes
import shutil
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from storage import AssetRecord, Storage


LOGGER = logging.getLogger(__name__)


class MediaProcessor:
    def __init__(self, config: dict[str, Any], storage: Storage) -> None:
        self.config = config
        self.storage = storage
        self.download_dir = Path(config["paths"]["download_dir"])
        self.processed_dir = Path(config["paths"]["processed_dir"])
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def ensure_local_media(self, asset: AssetRecord) -> Path:
        if asset.local_path:
            path = Path(asset.local_path)
            if not path.exists():
                raise FileNotFoundError(f"Local asset file is missing: {path}")
            return path

        if not asset.media_url:
            raise RuntimeError("Asset has no downloadable media URL.")

        extension = guess_extension(asset.media_url)
        destination = self.download_dir / f"{asset.source_name}_{asset.source_asset_id}{extension}"
        if destination.exists():
            return destination

        request = Request(asset.media_url, headers={"User-Agent": "RightsSafeTikTokTool/1.0"})
        with urlopen(request, timeout=120) as response, destination.open("wb") as handle:
            shutil.copyfileobj(response, handle)

        self.storage.update_asset(asset.asset_id or 0, local_path=str(destination))
        self.storage.log_event(
            "info",
            "download_completed",
            f"Downloaded media for asset {asset.title}",
            asset_id=asset.asset_id,
            details={"path": str(destination)},
        )
        return destination

    def prepare_asset(
        self,
        asset: AssetRecord,
        *,
        subtitle_file: str = "",
        dry_run: bool = False,
    ) -> Path:
        output_path = self.processed_dir / f"{asset.asset_id or asset.source_asset_id}_tiktok.mp4"
        ffmpeg_path = self.config["processing"].get("ffmpeg_path", "ffmpeg")
        max_duration = int(self.config["processing"].get("max_duration_seconds", 30))
        filters = [
            "scale=1080:1920:force_original_aspect_ratio=increase",
            "crop=1080:1920",
            "fps=30",
            "format=yuv420p",
        ]

        subtitle_path = subtitle_file or asset.subtitle_file
        font_path = self.config["processing"].get("drawtext_font_path", "")
        if subtitle_path:
            drawtext = (
                f"drawtext=textfile='{escape_filter_value(subtitle_path)}':"
                "reload=0:fontcolor=white:fontsize=42:"
                "box=1:boxcolor=black@0.55:boxborderw=20:"
                "x=(w-text_w)/2:y=h-(text_h*3)"
            )
            if font_path:
                drawtext = (
                    f"drawtext=fontfile='{escape_filter_value(font_path)}':"
                    f"textfile='{escape_filter_value(subtitle_path)}':"
                    "reload=0:fontcolor=white:fontsize=42:"
                    "box=1:boxcolor=black@0.55:boxborderw=20:"
                    "x=(w-text_w)/2:y=h-(text_h*3)"
                )
            filters.append(drawtext)

        if dry_run:
            input_hint = asset.local_path or asset.media_url or "<download-required>"
            command = [
                ffmpeg_path,
                "-y",
                "-i",
                input_hint,
                "-t",
                str(max_duration),
                "-vf",
                ",".join(filters),
                "-an",
                str(output_path),
            ]
            self.storage.log_event(
                "info",
                "prepare_dry_run",
                f"Dry-run prepared ffmpeg command for asset {asset.title}",
                asset_id=asset.asset_id,
                details={"command": command},
            )
            return output_path

        input_path = self.ensure_local_media(asset)

        command = [
            ffmpeg_path,
            "-y",
            "-i",
            str(input_path),
            "-t",
            str(max_duration),
            "-vf",
            ",".join(filters),
            "-an",
            str(output_path),
        ]

        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(f"ffmpeg failed: {completed.stderr.strip()}")

        media_hash = self.compute_sha256(output_path)
        self.storage.mark_processed(asset.asset_id or 0, str(output_path), media_hash)
        self.storage.log_event(
            "info",
            "prepare_completed",
            f"Prepared TikTok-ready media for asset {asset.title}",
            asset_id=asset.asset_id,
            details={"output_path": str(output_path)},
        )
        return output_path

    @staticmethod
    def compute_sha256(path: str | Path) -> str:
        digest = hashlib.sha256()
        with Path(path).open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()


def guess_extension(url: str) -> str:
    parsed = urlparse(url)
    suffix = Path(parsed.path).suffix
    if suffix:
        return suffix
    guessed = mimetypes.guess_extension(mimetypes.guess_type(url)[0] or "")
    return guessed or ".mp4"


def escape_filter_value(value: str) -> str:
    return str(Path(value)).replace("\\", "/").replace(":", "\\:").replace("'", "\\'")
