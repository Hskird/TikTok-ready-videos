# AI Kids Shorts Generator

This project can generate short vertical kids-story videos with voice and save them to a local folder so you can post them manually.

The main workflow now is:

- generate a new short video each time you run the command
- save the final MP4 in a folder you choose
- let you post it manually yourself

It also still includes the earlier legal media pipeline code, but if your goal is kids shorts, the most useful part is the story generator.

## What It Does

- Creates a short kids story with 4 scenes
- Generates narration audio
- Builds a vertical 9:16 video with motion and fades
- Saves the result to a local folder
- Optionally prepares title, description, and hashtags
- Lets you keep posting manual if that is your preference

## Project Files

- [main.py](C:\Users\user\Documents\Codex\2026-04-18-are-u-plus-gpt-model\main.py)
- [search_sources.py](C:\Users\user\Documents\Codex\2026-04-18-are-u-plus-gpt-model\search_sources.py)
- [rights_validator.py](C:\Users\user\Documents\Codex\2026-04-18-are-u-plus-gpt-model\rights_validator.py)
- [media_processor.py](C:\Users\user\Documents\Codex\2026-04-18-are-u-plus-gpt-model\media_processor.py)
- [scheduler.py](C:\Users\user\Documents\Codex\2026-04-18-are-u-plus-gpt-model\scheduler.py)
- [uploader.py](C:\Users\user\Documents\Codex\2026-04-18-are-u-plus-gpt-model\uploader.py)
- [storage.py](C:\Users\user\Documents\Codex\2026-04-18-are-u-plus-gpt-model\storage.py)
- [config.yaml](C:\Users\user\Documents\Codex\2026-04-18-are-u-plus-gpt-model\config.yaml)

## Setup

Requirements:

- Python 3.14 works on this machine
- `ffmpeg` is already configured in `config.yaml`
- `PyYAML`, `pillow`, and `edge-tts` are used

Useful installs:

```bash
py -m pip install PyYAML pillow edge-tts
```

## How To Run

Generate a local kids short video:

```bash
py main.py generate-kids-story --title "Milo and the Moon Pillow" --theme bedtime --character "Milo the little bunny" --output-dir .\data\kids_story\milo_local --scene-dir .\data\kids_story\milo_local\scenes
```

This saves the final video inside your chosen folder:

```bash
.\data\kids_story\milo_local\kids_story_short.mp4
```

If you want a quick test with simple placeholder scenes:

```bash
py main.py generate-kids-story --title "Milo and the Moon Pillow" --theme bedtime --character "Milo the little bunny" --output-dir .\data\kids_story\milo_cards --create-placeholders
```

Important:

- `--create-placeholders` makes card-style scenes on purpose
- if you want a better-looking video, use real generated scene images in the `scenes` folder
- the best current example is:
  [kids_story_short.mp4](C:\Users\user\Documents\Codex\2026-04-18-are-u-plus-gpt-model\data\kids_story\milo_moon_ai\kids_story_short.mp4)

## Kids Story Shorts

The story generator creates:

- creates a small story package with title, scenes, prompts, narration text, and subtitles
- prefers a better English neural voice with `edge-tts` when available, then falls back to Windows speech synthesis
- assembles a vertical short video with sound, animated scene motion, and fades
- works with scene images you generate yourself for each scene

Example outputs are written under `data/kids_story/...` including:

- `story.json`
- `image_prompts.txt`
- `narration.wav`
- `kids_story_short.mp4`

## How To Make It Less Like Cards

If the video looks like cards, the reason is almost always that you used placeholder scene images.

To make it feel more animated:

1. Do not use `--create-placeholders`
2. Put real AI-generated story scenes in:
   - `scene_01.png`
   - `scene_02.png`
   - `scene_03.png`
   - `scene_04.png`
3. Run the generator again using `--scene-dir`

Current animation in the code:

- zoom in / zoom out motion
- pan motion
- fades
- color boost
- voice narration

What it is not yet:

- full character-by-character cartoon animation
- lip sync
- moving arms, faces, or bodies inside one image

If you want even more movement later, the next upgrade would be:

- parallax layers
- multiple images per scene
- animated overlays like sparkles, stars, smoke, or magic dust
- character cutout movement across the frame

## Manual Posting

If you only want local files and want to post yourself manually, do not use `--auto-post`.

Just run the generator and your video will be saved locally. Then upload it yourself to TikTok or YouTube Shorts.
