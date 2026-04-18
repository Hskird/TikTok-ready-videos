from __future__ import annotations

import asyncio
import json
import math
import subprocess
import textwrap
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass
class StoryScene:
    number: int
    title: str
    narration: str
    image_prompt: str
    image_filename: str


@dataclass
class StoryPackage:
    title: str
    theme: str
    audience: str
    moral: str
    scenes: list[StoryScene]

    @property
    def full_narration(self) -> str:
        return " ".join(scene.narration for scene in self.scenes).strip()


class KidsStoryGenerator:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.story_config = config.get("kids_story", {})
        self.narration_config = self.story_config.get("narration", {})
        self.animation_config = self.story_config.get("animation", {})
        self.ffmpeg_path = self.story_config.get(
            "ffmpeg_path",
            config.get("processing", {}).get("ffmpeg_path", "ffmpeg"),
        )
        self.ffprobe_path = self._derive_ffprobe_path(self.ffmpeg_path)

    def build_story(
        self,
        *,
        title: str,
        theme: str,
        character_name: str,
        audience: str = "kids",
    ) -> StoryPackage:
        lowered = theme.lower().strip()
        if "bed" in lowered or "sleep" in lowered or "night" in lowered:
            moral = "Kind hearts make bedtime feel safe and cozy."
            scenes = [
                StoryScene(
                    1,
                    "The Missing Moon Pillow",
                    f"{character_name} looked under the blankets and gasped. The moon pillow was gone just before bedtime.",
                    (
                        f"Use case: illustration-story\n"
                        f"Asset type: vertical kids short video scene\n"
                        f"Primary request: a gentle bedtime story scene for kids with {character_name}, a cute little animal hero, "
                        "searching a cozy glowing bedroom for a missing moon-shaped pillow\n"
                        "Style/medium: colorful 3D storybook illustration, adorable, polished, kid-safe\n"
                        "Composition/framing: vertical 9:16, centered character, clear foreground and background layers\n"
                        "Lighting/mood: warm bedtime lamp, dreamy moonlight, soft magical atmosphere\n"
                        "Constraints: no text, no watermark, no scary elements"
                    ),
                    "scene_01.png",
                ),
                StoryScene(
                    2,
                    "Tiny Clues",
                    f"On the floor were sparkly crumbs that led to the toy shelf, so {character_name} followed the glittery trail.",
                    (
                        f"Use case: illustration-story\n"
                        f"Asset type: vertical kids short video scene\n"
                        f"Primary request: {character_name}, a cute little animal hero, following magical sparkly clues across a toy shelf in a cozy bedroom\n"
                        "Style/medium: colorful 3D storybook illustration, adorable, polished, kid-safe\n"
                        "Composition/framing: vertical 9:16, dynamic path leading upward, playful toys around\n"
                        "Lighting/mood: soft gold sparkles, warm safe bedroom lighting\n"
                        "Constraints: no text, no watermark, no scary elements"
                    ),
                    "scene_02.png",
                ),
                StoryScene(
                    3,
                    "The Sleepy Dragon",
                    f"There was the moon pillow, tucked beneath a sleepy baby dragon who only wanted something soft to hug.",
                    (
                        f"Use case: illustration-story\n"
                        f"Asset type: vertical kids short video scene\n"
                        f"Primary request: {character_name}, a cute little animal hero, discovering a tiny sleepy baby dragon cuddling a moon-shaped pillow on a toy shelf\n"
                        "Style/medium: colorful 3D storybook illustration, adorable, polished, kid-safe\n"
                        "Composition/framing: vertical 9:16, emotional close scene, both characters visible\n"
                        "Lighting/mood: gentle moon glow, peaceful and heartwarming\n"
                        "Constraints: no text, no watermark, no scary elements"
                    ),
                    "scene_03.png",
                ),
                StoryScene(
                    4,
                    "A Cozy Share",
                    f"{character_name} shared the blanket, the dragon shared the pillow, and both friends drifted to sleep with happy smiles.",
                    (
                        f"Use case: illustration-story\n"
                        f"Asset type: vertical kids short video scene\n"
                        f"Primary request: a heartwarming bedtime ending with {character_name} and a tiny baby dragon peacefully sharing a blanket and moon pillow\n"
                        "Style/medium: colorful 3D storybook illustration, adorable, polished, kid-safe\n"
                        "Composition/framing: vertical 9:16, calm final shot, dreamy bedtime composition\n"
                        "Lighting/mood: cozy moonlight, peaceful, magical, gentle\n"
                        "Constraints: no text, no watermark, no scary elements"
                    ),
                    "scene_04.png",
                ),
            ]
        else:
            moral = "Helping others turns a small adventure into a big friendship."
            scenes = [
                StoryScene(
                    1,
                    "A Shiny Red Balloon",
                    f"{character_name} spotted a shiny red balloon floating over the park and raced after it with a giggle.",
                    (
                        f"Use case: illustration-story\n"
                        f"Asset type: vertical kids short video scene\n"
                        f"Primary request: {character_name}, a cute little animal hero, chasing a shiny red balloon through a sunny park\n"
                        "Style/medium: colorful 3D storybook illustration, adorable, polished, kid-safe\n"
                        "Composition/framing: vertical 9:16, lively movement, bright clean background\n"
                        "Lighting/mood: sunny cheerful daylight, playful atmosphere\n"
                        "Constraints: no text, no watermark, no scary elements"
                    ),
                    "scene_01.png",
                ),
                StoryScene(
                    2,
                    "A Bird in Trouble",
                    f"The balloon had wrapped around a little bird's nest, and the baby bird looked worried.",
                    (
                        f"Use case: illustration-story\n"
                        f"Asset type: vertical kids short video scene\n"
                        f"Primary request: {character_name}, a cute little animal hero, noticing a small bird nest gently tangled by a red balloon in a tree\n"
                        "Style/medium: colorful 3D storybook illustration, adorable, polished, kid-safe\n"
                        "Composition/framing: vertical 9:16, tree branch and characters clearly visible\n"
                        "Lighting/mood: kind and emotional, bright safe daytime lighting\n"
                        "Constraints: no text, no watermark, no scary elements"
                    ),
                    "scene_02.png",
                ),
                StoryScene(
                    3,
                    "A Clever Plan",
                    f"{character_name} climbed the slide, stretched up high, and gently freed the nest with one careful tug.",
                    (
                        f"Use case: illustration-story\n"
                        f"Asset type: vertical kids short video scene\n"
                        f"Primary request: {character_name}, a cute little animal hero, using a playground slide to reach a tree branch and carefully rescue a bird nest from a red balloon\n"
                        "Style/medium: colorful 3D storybook illustration, adorable, polished, kid-safe\n"
                        "Composition/framing: vertical 9:16, cinematic upward angle, action but gentle and safe\n"
                        "Lighting/mood: triumphant and warm, bright daylight\n"
                        "Constraints: no text, no watermark, no scary elements"
                    ),
                    "scene_03.png",
                ),
                StoryScene(
                    4,
                    "Friends in the Sky",
                    f"The grateful birds tied the balloon to a basket of berries, and {character_name} smiled all the way home.",
                    (
                        f"Use case: illustration-story\n"
                        f"Asset type: vertical kids short video scene\n"
                        f"Primary request: happy ending with {character_name}, a cute little animal hero, receiving a berry basket gift from grateful birds beside a red balloon in a sunny park\n"
                        "Style/medium: colorful 3D storybook illustration, adorable, polished, kid-safe\n"
                        "Composition/framing: vertical 9:16, joyful ending shot, clear characters and gift basket\n"
                        "Lighting/mood: warm glowing sunset, uplifting, magical\n"
                        "Constraints: no text, no watermark, no scary elements"
                    ),
                    "scene_04.png",
                ),
            ]

        return StoryPackage(
            title=title,
            theme=theme,
            audience=audience,
            moral=moral,
            scenes=scenes,
        )

    def write_story_package(
        self,
        package: StoryPackage,
        *,
        output_dir: str | Path,
        scene_dir: str | Path | None = None,
        create_placeholder_images: bool = False,
    ) -> dict[str, Path]:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        resolved_scene_dir = Path(scene_dir) if scene_dir else output_path / "scenes"
        resolved_scene_dir.mkdir(parents=True, exist_ok=True)

        story_json = output_path / "story.json"
        narration_txt = output_path / "narration.txt"
        prompts_txt = output_path / "image_prompts.txt"
        subtitles_srt = output_path / "captions.srt"

        story_json.write_text(json.dumps(asdict(package), indent=2), encoding="utf-8")
        narration_txt.write_text(package.full_narration, encoding="utf-8")
        prompts_txt.write_text(self._render_prompt_sheet(package), encoding="utf-8")
        subtitles_srt.write_text(self._render_srt(package), encoding="utf-8")

        if create_placeholder_images:
            self._create_placeholder_images(package, resolved_scene_dir)

        return {
            "story_json": story_json,
            "narration_txt": narration_txt,
            "prompts_txt": prompts_txt,
            "subtitles_srt": subtitles_srt,
            "scene_dir": resolved_scene_dir,
        }

    def synthesize_narration(
        self,
        package: StoryPackage,
        *,
        output_audio_path: str | Path,
        voice_name: str = "",
        rate: int = 0,
    ) -> Path:
        destination = Path(output_audio_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        preferred_engine = self.narration_config.get("engine", "edge_tts")
        resolved_voice = voice_name or self.narration_config.get("voice", "")
        resolved_rate = rate if rate != 0 else int(self.narration_config.get("rate", 0))

        edge_error = ""
        if preferred_engine in {"edge_tts", "auto"}:
            try:
                asyncio.run(
                    self._synthesize_with_edge_tts(
                        package.full_narration,
                        destination,
                        voice_name=resolved_voice or "en-US-AnaNeural",
                        rate=self.narration_config.get("edge_rate", "+0%"),
                    )
                )
                return destination
            except Exception as exc:
                edge_error = str(exc)

        text = package.full_narration.replace("'", "''")
        escaped_output = str(destination).replace("'", "''")
        voice_line = ""
        if resolved_voice:
            voice_line = f"$speaker.SelectVoice('{resolved_voice}');"

        command = (
            "Add-Type -AssemblyName System.Speech; "
            "$speaker = New-Object System.Speech.Synthesis.SpeechSynthesizer; "
            f"{voice_line}"
            f"$speaker.Rate = {int(resolved_rate)}; "
            f"$speaker.SetOutputToWaveFile('{escaped_output}'); "
            f"$speaker.Speak('{text}'); "
            "$speaker.Dispose();"
        )

        completed = subprocess.run(
            ["powershell.exe", "-NoProfile", "-Command", command],
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            details = completed.stderr.strip()
            if edge_error:
                details = f"{details} | edge_tts fallback error: {edge_error}"
            raise RuntimeError(f"Narration synthesis failed: {details}")
        return destination

    def render_story_video(
        self,
        package: StoryPackage,
        *,
        scene_dir: str | Path,
        audio_path: str | Path,
        subtitle_path: str | Path,
        output_video_path: str | Path,
    ) -> Path:
        scene_dir_path = Path(scene_dir)
        output_path = Path(output_video_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        audio_duration = self._probe_duration(Path(audio_path))
        per_scene_duration = max(3.2, audio_duration / max(1, len(package.scenes)))

        clips_dir = output_path.parent / "clips"
        clips_dir.mkdir(parents=True, exist_ok=True)
        concat_file = output_path.parent / "scene_concat.txt"
        concat_lines: list[str] = []

        subtitle_segments = parse_srt_segments(Path(subtitle_path))
        scene_captions = [segment["text"] for segment in subtitle_segments[: len(package.scenes)]]
        while len(scene_captions) < len(package.scenes):
            scene_captions.append("")

        for index, scene in enumerate(package.scenes):
            image_path = scene_dir_path / scene.image_filename
            if not image_path.exists():
                raise FileNotFoundError(f"Missing scene image: {image_path}")
            clip_path = clips_dir / f"scene_{index + 1:02d}.mp4"
            caption_text = scene_captions[index]
            self._render_scene_clip(
                image_path=image_path,
                caption_text=caption_text,
                duration_seconds=per_scene_duration,
                output_path=clip_path,
                motion_variant=index % 4,
            )
            concat_lines.append(f"file '{clip_path.resolve().as_posix()}'")

        concat_file.write_text("\n".join(concat_lines) + "\n", encoding="utf-8")
        command = [
            self.ffmpeg_path,
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(concat_file),
            "-i",
            str(audio_path),
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            "-c:v",
            "copy",
            "-c:a",
            "aac",
            "-b:a",
            "160k",
            "-shortest",
            "-movflags",
            "+faststart",
            str(output_path),
        ]
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
        if completed.returncode != 0:
            raise RuntimeError(f"Video render failed: {completed.stderr.strip()}")
        return output_path

    def _render_scene_clip(
        self,
        *,
        image_path: Path,
        caption_text: str,
        duration_seconds: float,
        output_path: Path,
        motion_variant: int,
    ) -> None:
        fps = int(self.animation_config.get("fps", 30))
        frame_count = max(1, int(duration_seconds * fps))
        fade_duration = float(self.animation_config.get("fade_duration_seconds", 0.35))
        zoom_speed = float(self.animation_config.get("zoom_speed", 0.0018))
        max_zoom = float(self.animation_config.get("max_zoom", 1.16))
        motion_profiles = [
            ("x='iw/2-(iw/zoom/2)-on*1.2':y='ih/2-(ih/zoom/2)-on*0.8'"),
            ("x='iw/2-(iw/zoom/2)+on*0.7':y='ih/2-(ih/zoom/2)-on*1.0'"),
            ("x='iw/2-(iw/zoom/2)-on*0.9':y='ih/2-(ih/zoom/2)+on*0.7'"),
            ("x='iw/2-(iw/zoom/2)+on*0.6':y='ih/2-(ih/zoom/2)+on*0.6'"),
        ]
        motion_profile = motion_profiles[motion_variant % len(motion_profiles)]
        base_vf = (
            "scale=1280:2272:force_original_aspect_ratio=increase,"
            "crop=1080:1920,"
            f"zoompan=z='min(zoom+{zoom_speed},{max_zoom})':"
            f"d={frame_count}:s=1080x1920:fps={fps}:"
            + motion_profile
            + f",fps={fps},"
            + f"fade=t=in:st=0:d={fade_duration},"
            + f"fade=t=out:st={max(duration_seconds - fade_duration, 0):.2f}:d={fade_duration},"
            "eq=saturation=1.15:brightness=0.03:contrast=1.05"
        )
        full_vf = base_vf
        escaped_caption = escape_drawtext(caption_text)
        if escaped_caption:
            full_vf += (
                ",drawtext="
                f"text='{escaped_caption}':"
                "fontcolor=white:fontsize=40:"
                "box=1:boxcolor=black@0.45:boxborderw=24:"
                "x=(w-text_w)/2:y=h-(text_h*2.7):"
                f"enable='between(t,0.2,{max(duration_seconds - 0.2, 0.2):.2f})'"
            )
        command = self._scene_clip_command(
            image_path=image_path,
            duration_seconds=duration_seconds,
            vf=full_vf,
            output_path=output_path,
        )
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
        if completed.returncode != 0 and "No such filter: 'drawtext'" in completed.stderr:
            fallback_command = self._scene_clip_command(
                image_path=image_path,
                duration_seconds=duration_seconds,
                vf=base_vf,
                output_path=output_path,
            )
            completed = subprocess.run(fallback_command, capture_output=True, text=True, check=False)
        if completed.returncode != 0:
            raise RuntimeError(f"Scene clip render failed for {image_path.name}: {completed.stderr.strip()}")

    def _scene_clip_command(
        self,
        *,
        image_path: Path,
        duration_seconds: float,
        vf: str,
        output_path: Path,
    ) -> list[str]:
        return [
            self.ffmpeg_path,
            "-y",
            "-loop",
            "1",
            "-i",
            str(image_path),
            "-t",
            f"{duration_seconds:.3f}",
            "-vf",
            vf,
            "-c:v",
            "libx264",
            "-preset",
            "medium",
            "-crf",
            "21",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            str(output_path),
        ]

    def _render_prompt_sheet(self, package: StoryPackage) -> str:
        blocks = []
        for scene in package.scenes:
            blocks.append(
                f"Scene {scene.number}: {scene.title}\n"
                f"Filename: {scene.image_filename}\n"
                f"Prompt:\n{scene.image_prompt}\n"
            )
        return "\n".join(blocks)

    def _render_srt(self, package: StoryPackage) -> str:
        start_seconds = 0.0
        total_words = max(1, len(package.full_narration.split()))
        segments = []
        for index, scene in enumerate(package.scenes, start=1):
            words = len(scene.narration.split())
            duration = max(2.5, words / total_words * max(16.0, total_words / 2.4))
            end_seconds = start_seconds + duration
            segments.append(
                f"{index}\n"
                f"{format_srt_time(start_seconds)} --> {format_srt_time(end_seconds)}\n"
                f"{textwrap.fill(scene.narration, width=36)}\n"
            )
            start_seconds = end_seconds
        return "\n".join(segments).strip() + "\n"

    def _create_placeholder_images(self, package: StoryPackage, scene_dir: Path) -> None:
        from PIL import Image, ImageDraw, ImageFont

        palette = [
            ("#ffecd2", "#fcb69f"),
            ("#c2ffd8", "#465efb"),
            ("#f6d365", "#fda085"),
            ("#a1c4fd", "#c2e9fb"),
        ]
        font = ImageFont.load_default()
        for scene, colors in zip(package.scenes, palette, strict=False):
            image = Image.new("RGB", (1080, 1920), colors[0])
            draw = ImageDraw.Draw(image)
            draw.rectangle((0, 0, 1080, 700), fill=colors[1])
            draw.rounded_rectangle((80, 820, 1000, 1540), radius=40, fill="white")
            draw.text((120, 880), scene.title, fill="black", font=font)
            body = textwrap.fill(scene.narration, width=28)
            draw.multiline_text((120, 980), body, fill="black", font=font, spacing=16)
            image.save(scene_dir / scene.image_filename)

    async def _synthesize_with_edge_tts(
        self,
        text: str,
        destination: Path,
        *,
        voice_name: str,
        rate: str,
    ) -> None:
        try:
            import edge_tts
        except ImportError as exc:
            raise RuntimeError("edge_tts is not installed.") from exc

        communicate = edge_tts.Communicate(text=text, voice=voice_name, rate=rate)
        await communicate.save(str(destination))

    @staticmethod
    def _derive_ffprobe_path(ffmpeg_path: str) -> str:
        ffmpeg = Path(ffmpeg_path)
        sibling = ffmpeg.with_name("ffprobe.exe" if ffmpeg.suffix.lower() == ".exe" else "ffprobe")
        return str(sibling) if sibling.exists() else "ffprobe"

    def _probe_duration(self, path: Path) -> float:
        command = [
            self.ffprobe_path,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ]
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
        if completed.returncode != 0:
            raise RuntimeError(f"ffprobe failed for {path}: {completed.stderr.strip()}")
        return float(completed.stdout.strip())


def format_srt_time(value: float) -> str:
    total_milliseconds = max(0, int(round(value * 1000)))
    hours, remainder = divmod(total_milliseconds, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, milliseconds = divmod(remainder, 1000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d},{milliseconds:03d}"


def parse_srt_segments(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    blocks = path.read_text(encoding="utf-8").strip().split("\n\n")
    segments: list[dict[str, str]] = []
    for block in blocks:
        lines = [line for line in block.splitlines() if line.strip()]
        if len(lines) < 3:
            continue
        segments.append({"text": " ".join(lines[2:]).strip()})
    return segments


def escape_drawtext(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace(":", r"\:")
        .replace("'", r"\'")
        .replace("%", r"\%")
        .replace("\n", " ")
    )
