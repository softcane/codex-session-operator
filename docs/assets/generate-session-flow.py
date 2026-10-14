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


WIDTH, HEIGHT, SCALE = 1200, 820, 2
BACKGROUND = "#faf8f4"
INK = "#363a3b"
MUTED = "#737771"
BLUE = "#347fba"
GREEN = "#458565"
PURPLE = "#8964a9"
AMBER = "#b48d41"
GRAY = "#797d76"
OUTPUT = Path(__file__).resolve().parent
OVERVIEW = "Choose an approved profile. Save input, create a Session, and keep its history."

CAPTIONS = [
    "1  Choose an approved profile: Agent, Environment, Vaults and spend policy.",
    "2  Send the first message to the gateway.",
    "3  Commit the message and receipt to PostgreSQL.",
    "4  Return 202 Accepted with a stable conversation ID.",
    "5  Create a Kubernetes Session in the background.",
    "6  Freeze the approved settings and create the remote Session.",
    "7  For self hosted execution, provision a Pod and optional workspace.",
    "8  Send follow-ups to the same conversation; read history for completion.",
    "9  Close and keep history. Restart after cleanup to create a new attempt.",
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


def card(draw: ImageDraw.ImageDraw, x: int, title: str, line1: str,
         line2: str, color: str, seed: int) -> None:
    box(draw, (x, 196, x + 240, 330), color, seed)
    text(draw, (x + 120, 215), title, 26, color, center=True)
    text(draw, (x + 120, 263), line1, 20, center=True)
    text(draw, (x + 120, 293), line2, 19, MUTED, center=True)


def render(stage: int, progress: float = 1, caption: str | None = None) -> Image.Image:
    image = Image.new("RGB", (WIDTH * SCALE, HEIGHT * SCALE), BACKGROUND)
    draw = ImageDraw.Draw(image)
    text(draw, (48, 27), "Codex Session Operator", 36)
    text(draw, (48, 80), "From a first message to a managed conversation", 23, MUTED)
    draw.rounded_rectangle((48 * SCALE, 128 * SCALE, 1152 * SCALE, 167 * SCALE),
                           radius=5 * SCALE, fill="#efeee8")
    text(draw, (62, 134), caption or CAPTIONS[stage], 20)

    card(draw, 48, "Approved profile", "Agent + Environment", "Vaults + spend policy", GRAY, 10)
    if stage >= 1:
        arrow(draw, (288, 263), (332, 263), BLUE, 20,
              progress=progress if stage == 1 else 1)
        card(draw, 336, "Gateway", "API + browser UI", "accept first message", BLUE, 30)
    if stage >= 2:
        arrow(draw, (456, 333), (456, 384), BLUE, 40,
              progress=progress if stage == 2 else 1)
        box(draw, (336, 390, 576, 511), BLUE, 50)
        text(draw, (456, 407), "PostgreSQL", 25, BLUE, center=True)
        text(draw, (456, 453), "inputs + receipts", 20, center=True)
        text(draw, (456, 482), "history + approvals", 19, MUTED, center=True)
    if stage >= 3:
        box(draw, (48, 390, 288, 511), GREEN, 60)
        text(draw, (168, 407), "202 Accepted", 25, GREEN, center=True)
        text(draw, (168, 453), "after database commit", 19, center=True)
        text(draw, (168, 482), "stable conversation ID", 18, MUTED, center=True)
    if stage >= 4:
        arrow(draw, (576, 263), (620, 263), GREEN, 70,
              progress=progress if stage == 4 else 1)
        card(draw, 624, "Kubernetes", "Session + operator", "freeze approved settings", GREEN, 80)
    if stage >= 5:
        arrow(draw, (864, 263), (908, 263), PURPLE, 90,
              progress=progress if stage == 5 else 1)
        card(draw, 912, "OpenAI", "Remote Session", "model + tools", PURPLE, 100)
    if stage >= 6:
        arrow(draw, (744, 333), (744, 384), GREEN, 110,
              progress=progress if stage == 6 else 1, dashed=True)
        arrow(draw, (1032, 384), (1032, 334), PURPLE, 120, dashed=True)
        box(draw, (624, 390, 1152, 511), GREEN, 130, dashed=True)
        text(draw, (888, 405), "Self hosted only", 20, GREEN, center=True)
        text(draw, (888, 444), "Executor Pod", 25, center=True)
        text(draw, (888, 482), "optional workspace PVC", 19, MUTED, center=True)

    if stage >= 7:
        text(draw, (48, 561), "Read history to confirm a completed turn.", 25)
        text(draw, (48, 602), "A receipt confirms saved input; completion comes later.", 20, MUTED)
        box(draw, (48, 661, 388, 765), BLUE, 140)
        text(draw, (218, 674), "Follow up", 25, BLUE, center=True)
        text(draw, (218, 722), "same conversation", 20, center=True)
    if stage >= 8:
        box(draw, (430, 661, 770, 765), GRAY, 150)
        text(draw, (600, 674), "Close", 25, GRAY, center=True)
        text(draw, (600, 722), "retain saved history", 20, center=True)
        box(draw, (812, 661, 1152, 765), AMBER, 160)
        text(draw, (982, 674), "Restart after cleanup", 25, AMBER, center=True)
        text(draw, (982, 722), "new attempt + Session", 20, center=True)

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
