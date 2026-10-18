#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Render the README diagram. Requires Pillow >= 10.4; no external fonts.

Run from any directory:
    python3 -m pip install 'Pillow>=10.4,<13'
    python3 docs/assets/generate-session-flow.py

The diagram illustrates the API contract, rather than recording a live demo.
All paths and labels are defined here. The fixed seed makes the sketch strokes
repeatable, and Pillow's bundled font keeps rendering independent of OS fonts.
"""

from __future__ import annotations

import math
from pathlib import Path
import random

from PIL import Image, ImageDraw, ImageFont


WIDTH, HEIGHT, SCALE = 1200, 1040, 2
BACKGROUND = "#faf8f4"
INK = "#363a3b"
MUTED = "#737771"
BLUE = "#347fba"
GREEN = "#458565"
PURPLE = "#8964a9"
AMBER = "#b48d41"
GRAY = "#797d76"
OUTPUT = Path(__file__).resolve().parent
OVERVIEW = "One installation. Durable conversations. A project executor for each self-hosted Session."

CAPTIONS = [
    "1  Install both services together. The gateway initializes its PostgreSQL tables.",
    "2  Declare Agent, Environment and Profile. These recipes create no task Pod.",
    "3  Start a task from the Profile with a stable idempotency key.",
    "4  Commit input and its receipt to PostgreSQL, then return 202 Accepted.",
    "5  The gateway creates a Kubernetes Session from the approved Profile.",
    "6  The controller freezes approved settings and creates the remote OpenAI Session.",
    "7  Provision the application executor, prepare its repository, and connect its tools.",
    "8  The gateway delivers messages and saves turns. Read history to confirm completion.",
    "9  Follow up, close, or start fresh after cleanup. Saved conversation history remains.",
]


def font(size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.load_default(size=size * SCALE)


def text(draw: ImageDraw.ImageDraw, xy: tuple[int, int], value: str,
         size: int = 22, color: str = INK, center: bool = False) -> None:
    x, y = xy
    selected = font(size)
    if center:
        x -= draw.textlength(value, font=selected) / (2 * SCALE)
    draw.text((round(x * SCALE), y * SCALE), value, font=selected, fill=color)


def stroke(draw: ImageDraw.ImageDraw, start: tuple[float, float],
           end: tuple[float, float], color: str, seed: int,
           dashed: bool = False, width: float = 1.8) -> None:
    rng = random.Random(seed)
    length = math.dist(start, end)
    count = max(2, int(length / 22))
    points = []
    for index in range(count + 1):
        fraction = index / count
        jitter = 0 if index in (0, count) else rng.uniform(-0.55, 0.55)
        points.append((
            (start[0] + (end[0] - start[0]) * fraction + jitter) * SCALE,
            (start[1] + (end[1] - start[1]) * fraction + jitter) * SCALE,
        ))
    if dashed:
        segments = max(1, int(length / 10))
        for index in range(0, segments, 2):
            first, last = index / segments, min(index + 1, segments) / segments
            a = tuple((start[axis] + (end[axis] - start[axis]) * first) * SCALE
                      for axis in (0, 1))
            b = tuple((start[axis] + (end[axis] - start[axis]) * last) * SCALE
                      for axis in (0, 1))
            draw.line((a, b), fill=color, width=round(width * SCALE))
    else:
        draw.line(points, fill=color, width=round(width * SCALE), joint="curve")


def arrow(draw: ImageDraw.ImageDraw, start: tuple[int, int],
          end: tuple[int, int], color: str, seed: int,
          progress: float = 1, dashed: bool = False) -> None:
    tip = tuple(start[axis] + (end[axis] - start[axis]) * progress
                for axis in (0, 1))
    stroke(draw, start, tip, color, seed, dashed=dashed, width=2)
    if progress < 1:
        return
    angle = math.atan2(end[1] - start[1], end[0] - start[0])
    for delta in (-0.52, 0.52):
        tail = (end[0] - 10 * math.cos(angle + delta),
                end[1] - 10 * math.sin(angle + delta))
        stroke(draw, tail, end, color, seed + 1, width=2)


def box(draw: ImageDraw.ImageDraw, bounds: tuple[int, int, int, int],
        color: str, seed: int, dashed: bool = False) -> None:
    left, top, right, bottom = bounds
    draw.rectangle(tuple(value * SCALE for value in bounds), fill=BACKGROUND)
    for index, (start, end) in enumerate([
        ((left, top), (right, top)), ((right, top), (right, bottom)),
        ((right, bottom), (left, bottom)), ((left, bottom), (left, top)),
    ]):
        stroke(draw, start, end, color, seed + index, dashed=dashed)
    # A second light stroke gives a hand-drawn edge without blurring the text.
    if not dashed:
        stroke(draw, (left + 1, top + 2), (right - 1, top + 1), color,
               seed + 8, width=0.6)


def card(draw: ImageDraw.ImageDraw, x: int, y: int, title: str, line1: str,
         line2: str, color: str, seed: int, title_size: int = 25) -> None:
    box(draw, (x, y, x + 240, y + 120), color, seed)
    text(draw, (x + 120, y + 15), title, title_size, color, center=True)
    text(draw, (x + 120, y + 59), line1, 19, center=True)
    text(draw, (x + 120, y + 90), line2, 18, MUTED, center=True)


def route(draw: ImageDraw.ImageDraw, points: list[tuple[int, int]], color: str,
          seed: int, progress: float = 1, two_way: bool = False,
          dashed: bool = False) -> None:
    lengths = [math.dist(start, end) for start, end in zip(points, points[1:])]
    remaining = sum(lengths) * progress
    for index, (start, end, length) in enumerate(zip(points, points[1:], lengths)):
        if remaining <= 0:
            break
        part = min(1, remaining / length)
        if index == len(lengths) - 1:
            arrow(draw, start, end, color, seed + index, part, dashed=dashed)
        else:
            tip = tuple(start[axis] + (end[axis] - start[axis]) * part
                        for axis in (0, 1))
            stroke(draw, start, tip, color, seed + index, dashed=dashed, width=2)
        remaining -= length
    if two_way and progress == 1:
        # Add a reverse arrowhead on the first segment without retracing a route.
        start, following = points[:2]
        angle = math.atan2(start[1] - following[1], start[0] - following[0])
        for delta in (-0.52, 0.52):
            tail = (start[0] - 10 * math.cos(angle + delta),
                    start[1] - 10 * math.sin(angle + delta))
            stroke(draw, tail, start, color, seed + 20, width=2)


def render(stage: int, progress: float = 1, caption: str | None = None) -> Image.Image:
    image = Image.new("RGB", (WIDTH * SCALE, HEIGHT * SCALE), BACKGROUND)
    draw = ImageDraw.Draw(image)
    text(draw, (48, 27), "Codex Session Operator", 36)
    text(draw, (48, 80), "my-go-project: installation, conversations, and self-hosted execution", 23, MUTED)
    draw.rounded_rectangle((48 * SCALE, 128 * SCALE, 1152 * SCALE, 167 * SCALE),
                           radius=5 * SCALE, fill="#efeee8")
    text(draw, (62, 134), caption or CAPTIONS[stage], 20)

    def connect(first_stage: int, points: list[tuple[int, int]], color: str,
                seed: int, **options: bool) -> None:
        if stage >= first_stage:
            route(draw, points, color, seed,
                  progress=progress if stage == first_stage else 1, **options)

    # Installation boundaries are independent of conversation state.
    box(draw, (318, 216, 882, 416), GRAY, 10, dashed=True)
    text(draw, (600, 229), "One install, separate images", 18, MUTED, center=True)
    connect(1, [(168, 460), (168, 390)], GRAY, 30)
    connect(2, [(288, 330), (336, 330)], BLUE, 40)
    connect(0, [(456, 390), (456, 460)], BLUE, 50, two_way=True)
    connect(4, [(576, 360), (610, 360), (610, 520), (624, 520)], BLUE, 60)
    connect(5, [(744, 460), (744, 390)], GREEN, 70)
    connect(5, [(864, 330), (912, 330)], GREEN, 80)
    connect(6, [(864, 360), (892, 360), (892, 520), (912, 520)], GREEN, 90)
    connect(6, [(1032, 390), (1032, 460)], PURPLE, 100, two_way=True)
    connect(6, [(1032, 580), (1032, 650)], GREEN, 110, two_way=True)
    connect(7, [(456, 270), (456, 192), (1032, 192), (1032, 270)], PURPLE,
            120, two_way=True)
    if stage >= 7:
        text(draw, (744, 171), "messages + turns", 16, PURPLE, center=True)

    card(draw, 336, 460, "PostgreSQL", "messages + history", "external or local", BLUE, 130)
    card(draw, 336, 270, "Gateway Deployment", "input + history",
         "gateway image", BLUE, 150, title_size=22)
    card(draw, 624, 270, "Controller Deployment", "resource lifecycle",
         "controller image", GREEN, 160, title_size=21)
    if stage >= 1:
        card(draw, 48, 270, "Client", "start from Profile", "stable request key", GRAY, 170)
        card(draw, 48, 460, "my-go-project", "Agent + Environment",
             "Profile + spend policy", GRAY, 180)
    if stage > 3 or (stage == 3 and progress == 1):
        card(draw, 48, 650, "202 Accepted", "input saved durably",
             "completion comes later", BLUE, 190)
    if stage >= 4:
        card(draw, 624, 460, "Kubernetes Session", "one conversation attempt",
             "created by the gateway", GREEN, 200, title_size=22)
    if stage >= 5:
        card(draw, 912, 270, "OpenAI", "managed agent loop", "remote Session", PURPLE, 210)
    if stage >= 6:
        card(draw, 912, 460, "Executor Pod", "Codex + project tools",
             "selfHosted only", GREEN, 220)
        card(draw, 912, 650, "Workspace PVC", "source + edits + commits",
             "per-Session storage", GREEN, 230, title_size=24)
        text(draw, (48, 809), "Image layers: shared Codex runtime -> my-go-project tools (Go, Make, compiler).", 22)
        text(draw, (48, 849), "Repository source is checked out per Session; it is not baked into the shared image.",
             19, MUTED)
    if stage >= 7:
        card(draw, 336, 650, "Read history", "inspect the root turn",
             "confirm its outcome", BLUE, 240)
    if stage >= 8:
        for x, title, detail, color, seed in [
            (48, "Follow up", "same conversation + workspace", BLUE, 250),
            (430, "Close", "stop input; retain saved history", GRAY, 260),
            (812, "Start fresh after cleanup", "new Session + workspace", AMBER, 270),
        ]:
            box(draw, (x, 901, x + 340, 1005), color, seed)
            text(draw, (x + 170, 917), title, 23, color, center=True)
            text(draw, (x + 170, 965), detail, 18, center=True)

    return image.resize((WIDTH, HEIGHT), Image.Resampling.LANCZOS)


def main() -> None:
    poster = render(len(CAPTIONS) - 1, caption=OVERVIEW)
    # The first frame is the full overview, so static previews remain useful.
    frames, durations = [poster], [1000]
    for stage in range(len(CAPTIONS)):
        # Draw the connecting arrows in three steps, then allow time to read.
        for progress, duration in [(0.35, 110), (0.7, 110), (1, 1500)]:
            frames.append(render(stage, progress))
            durations.append(duration)
    durations[-1] = 5000
    poster.save(OUTPUT / "session-flow.png", optimize=True)
    # Use one palette for every frame to avoid color flicker while looping.
    palette = poster.quantize(colors=256, method=Image.Quantize.MEDIANCUT)
    indexed = [frame.quantize(palette=palette, dither=Image.Dither.NONE)
               for frame in frames]
    indexed[0].save(OUTPUT / "session-flow.gif", save_all=True,
                    append_images=indexed[1:], duration=durations, loop=0,
                    optimize=True, disposal=1)
    print(f"Wrote {OUTPUT / 'session-flow.gif'} and session-flow.png")


if __name__ == "__main__":
    main()
