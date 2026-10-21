#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Render the README's illustrated SelfHosted lifecycle, not a live recording.

Requires Pillow >= 10.4; uses its bundled font, with no OS font dependency.
Run from any directory (outputs live beside this script):
    python3 -m pip install 'Pillow>=10.4,<13'
    python3 docs/assets/generate-session-flow.py

Edit STEPS and render() together when the lifecycle changes. The first GIF
frame and PNG show the complete overview; subsequent frames walk each step.
"""

from __future__ import annotations

import math
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


WIDTH, HEIGHT, SCALE = 1200, 1320, 2
BACKGROUND = "#faf8f4"
INK = "#363a3b"
MUTED = "#686e68"
BLUE = "#2677af"
GREEN = "#357b59"
PURPLE = "#8352a4"
AMBER = "#916a25"
OUTPUT = Path(__file__).resolve().parent
STEPS = [
    ("1 / 10   Install the services and their dependencies",
     "Controller + gateway ship together; teams manage PostgreSQL; install Agent Sandbox separately."),
    ("2 / 10   Declare reusable configuration",
     "Choose Agent + Environment + Profile; approve a versioned Sandbox template and pool."),
    ("3 / 10   Start a conversation through the gateway",
     "Send a Profile name, first message and stable idempotency key over HTTP."),
    ("4 / 10   Save the request before acknowledging it",
     "Commit input + receipt to PostgreSQL, then return 202 with a conversation URL."),
    ("5 / 10   Create the Kubernetes Session",
     "The gateway worker creates one Session from the accepted Profile selection."),
    ("6 / 10   Create the remote OpenAI Session",
     "Our controller resolves Ready dependencies, freezes settings and persists a creation claim."),
    ("7 / 10   Request a workspace through Agent Sandbox",
     "Our controller creates a SandboxClaim; Agent Sandbox creates the Sandbox, Pod and PVC."),
    ("8 / 10   Prepare the repository and connect the executor",
     "Verify ownership, preparation, workspace identity and the current OpenAI connection."),
    ("9 / 10   Deliver input and read the saved result",
     "The gateway delivers messages, saves turns and handles approvals; read the new root turn."),
    ("10 / 10   Continue the conversation or close and clean up",
     "Close stops input and suspends execution; cleanup follows the workspace deletion policy."),
]
OVERVIEW = (
    "From a Profile to a completed turn and workspace cleanup",
    "SelfHosted flow: our controller requests compute; Agent Sandbox manages Pods and storage.",
)


def text(draw: ImageDraw.ImageDraw, xy: tuple[float, float], value: str,
         size: int = 22, color: str = INK, center: bool = False) -> None:
    selected = ImageFont.load_default(size=size * SCALE)
    x, y = xy
    width = draw.textlength(value, font=selected) / SCALE
    if center:
        x -= width / 2
    if x < 0 or x + width > WIDTH or y + size > HEIGHT:
        raise ValueError(f"Text outside canvas: {value!r}")
    draw.text((round(x * SCALE), round(y * SCALE)), value,
              font=selected, fill=color)


def box(draw: ImageDraw.ImageDraw, bounds: tuple[int, int, int, int],
        color: str, fill: str = BACKGROUND, width: int = 2) -> None:
    draw.rounded_rectangle(tuple(value * SCALE for value in bounds),
                           radius=7 * SCALE, fill=fill, outline=color,
                           width=width * SCALE)


def route(draw: ImageDraw.ImageDraw, points: list[tuple[int, int]],
          color: str, progress: float = 1, two_way: bool = False) -> None:
    lengths = [math.dist(a, b) for a, b in zip(points, points[1:])]
    remaining = sum(lengths) * progress
    for a, b, length in zip(points, points[1:], lengths):
        if remaining <= 0:
            break
        part = min(1, remaining / length)
        tip = tuple((a[axis] + (b[axis] - a[axis]) * part) * SCALE
                    for axis in (0, 1))
        draw.line([(a[0] * SCALE, a[1] * SCALE), tip],
                  fill=color, width=2 * SCALE)
        remaining -= length
    if progress < 1:
        return

    def head(a: tuple[int, int], b: tuple[int, int]) -> None:
        angle = math.atan2(b[1] - a[1], b[0] - a[0])
        tails = [(b[0] - 10 * math.cos(angle + delta),
                  b[1] - 10 * math.sin(angle + delta))
                 for delta in (-0.5, 0.5)]
        draw.line([(round(x * SCALE), round(y * SCALE))
                   for x, y in [tails[0], b, tails[1]]],
                  fill=color, width=2 * SCALE)

    head(points[-2], points[-1])
    if two_way:
        head(points[1], points[0])


def card(draw: ImageDraw.ImageDraw, x: int, y: int, title: str,
         line1: str, line2: str, color: str, title_size: int = 24,
         active: bool = False) -> None:
    box(draw, (x, y, x + 240, y + 120), color, width=3 if active else 2)
    for value, size, offset, tone in [
        (title, title_size, 16, color),
        (line1, 19, 59, INK),
        (line2, 18, 90, MUTED),
    ]:
        selected = ImageFont.load_default(size=size * SCALE)
        if draw.textlength(value, font=selected) > 220 * SCALE:
            raise ValueError(f"Text outside card: {value!r}")
        text(draw, (x + 120, y + offset), value, size, tone, center=True)


def render(stage: int, progress: float = 1, overview: bool = False) -> Image.Image:
    image = Image.new("RGB", (WIDTH * SCALE, HEIGHT * SCALE), BACKGROUND)
    draw = ImageDraw.Draw(image)
    text(draw, (48, 26), "Codex Session Operator", 36)
    text(draw, (48, 78), "Step by step: configuration, durable input, execution and cleanup", 23, MUTED)
    box(draw, (48, 123, 1152, 215), "#e7e5de", fill="#efeee8", width=1)
    title, detail = OVERVIEW if overview else STEPS[stage]
    text(draw, (64, 138), title, 25)
    text(draw, (64, 180), detail, 19)
    for index in range(len(STEPS)):
        left = 48 + index * 111
        draw.rounded_rectangle((left * SCALE, 228 * SCALE,
                                (left + 105) * SCALE, 233 * SCALE),
                               radius=2 * SCALE,
                               fill=BLUE if overview or index <= stage else "#e2e0d9")

    # A service boundary is separate from the upstream execution boundary.
    box(draw, (318, 292, 882, 458), "#d9d8d1", fill="#f3f2ed", width=1)
    text(draw, (600, 301), "OUR SERVICES / separate Deployments + images", 18, MUTED, center=True)
    if stage >= 6:
        box(draw, (318, 704, 1176, 1108), "#d0ded3", fill="#f0f4ed", width=1)
        text(draw, (744, 718), "AGENT SANDBOX / separate install", 21, GREEN, center=True)

    def connect(first: int, points: list[tuple[int, int]], color: str,
                two_way: bool = False) -> None:
        if stage >= first:
            route(draw, points, color,
                  progress if stage == first and not overview else 1, two_way)

    connect(0, [(456, 454), (456, 520)], BLUE, two_way=True)
    connect(1, [(168, 520), (168, 454)], MUTED)
    connect(2, [(288, 394), (336, 394)], BLUE)
    connect(4, [(576, 420), (600, 420), (600, 580), (624, 580)], BLUE)
    connect(5, [(744, 520), (744, 454)], GREEN)
    connect(5, [(864, 394), (912, 394)], GREEN)
    # The Session owns its claim. Agent Sandbox binds the claim to a Sandbox.
    connect(6, [(744, 640), (744, 677), (456, 677), (456, 776)], GREEN)
    connect(6, [(288, 836), (336, 836)], MUTED)
    connect(6, [(576, 836), (624, 836)], GREEN)
    connect(6, [(864, 836), (912, 836)], GREEN)
    connect(6, [(744, 896), (744, 1036), (912, 1036)], GREEN)
    connect(7, [(1032, 776), (1032, 454)], PURPLE, two_way=True)
    connect(7, [(1032, 896), (1032, 976)], GREEN, two_way=True)
    connect(8, [(360, 334), (360, 266), (1032, 266), (1032, 334)], PURPLE, two_way=True)
    if stage >= 6:
        text(draw, (600, 650), "owns", 17, GREEN, center=True)
        text(draw, (600, 810), "binds", 16, GREEN, center=True)
        text(draw, (888, 810), "owns", 16, GREEN, center=True)
        text(draw, (820, 1008), "owns PVC", 17, GREEN, center=True)
    if stage >= 7:
        text(draw, (1100, 576), "tool requests", 18, PURPLE, center=True)
        text(draw, (1100, 604), "+ results", 18, PURPLE, center=True)
        text(draw, (1090, 937), "mounts", 17, GREEN, center=True)
    if stage >= 8:
        text(draw, (744, 243), "messages + turns + approvals", 17, PURPLE, center=True)

    card(draw, 336, 334, "Gateway Deployment", "input + saved history",
         "gateway image", BLUE, 22, active=stage in (2, 3, 4, 8))
    card(draw, 624, 334, "Controller Deployment", "resource lifecycle",
         "controller image", GREEN, 21, active=stage in (5, 6))
    card(draw, 336, 520, "PostgreSQL", "messages + receipts",
         "history + approvals", BLUE, active=stage == 3)
    if stage >= 1:
        card(draw, 48, 334, "Caller", "start from a Profile",
             "HTTP + stable key", MUTED, active=stage == 2)
        card(draw, 48, 520, "Configuration", "Agent + Environment",
             "Profile + spend policy", MUTED, active=stage == 1)
        card(draw, 48, 776, "Approved recipe", "SandboxTemplate + pool",
             "image, repo, Secrets, disk", MUTED, 23, active=stage == 1)
    if stage >= 3:
        card(draw, 48, 976, "202 Accepted", "input saved + URL",
             "read history for completion", BLUE, active=stage == 3)
    if stage >= 4:
        card(draw, 624, 520, "Kubernetes Session", "conversation attempt",
             "gateway worker creates", GREEN, 22, active=stage == 4)
    if stage >= 5:
        card(draw, 912, 334, "OpenAI", "managed agent loop",
             "remote Session", PURPLE, active=stage == 5)
    if stage >= 6:
        card(draw, 336, 776, "SandboxClaim", "our controller creates",
             "Session owns the claim", GREEN, active=stage == 6)
        card(draw, 624, 776, "Sandbox", "Agent Sandbox creates",
             "owns Pod + workspace", GREEN, active=stage == 6)
        card(draw, 912, 776, "Executor Pod", "Codex + project tools",
             "template prepares repo", GREEN, active=stage == 7)
        card(draw, 912, 976, "Workspace PVC", "source + edits + commits",
             "Sandbox owns storage", GREEN, active=stage == 7)
    if stage >= 7:
        text(draw, (340, 925), "Delivery waits for verified ownership,", 20, GREEN)
        text(draw, (340, 955), "preparation + current connection.", 20, GREEN)
    if stage >= 8:
        text(draw, (340, 1026), "Read the saved new root turn", 21, BLUE)
        text(draw, (340, 1060), "to confirm its outcome.", 20, MUTED)

    if stage >= 9:
        text(draw, (48, 1140), "10  Continue or close", 24)
    for x, title, lines, color in [
        (48, "Follow up", ["Same conversation + workspace.", "History stays in PostgreSQL."], BLUE),
        (430, "Close + clean up", ["Delete: remove claim + runtime + files.",
                                  "Retain: stop Pod; keep Sandbox + files.",
                                  "Detach Sandbox, then delete its claim."], GREEN),
        (812, "Start fresh", ["Wait for cleanup to finish.", "New Session + separate workspace."], AMBER),
    ]:
        if stage >= 9:
            box(draw, (x, 1182, x + 340, 1294), color)
            text(draw, (x + 170, 1196), title, 23, color, center=True)
            size, top, spacing = (16, 1228, 21) if len(lines) == 3 else (17, 1234, 27)
            for offset, line in enumerate(lines):
                text(draw, (x + 170, top + offset * spacing), line, size, center=True)
    return image.resize((WIDTH, HEIGHT), Image.Resampling.LANCZOS)


def main() -> None:
    poster = render(len(STEPS) - 1, overview=True)
    frames, durations = [poster], [3000]
    for stage in range(len(STEPS)):
        for progress, duration in [(0.4, 140), (0.75, 140), (1, 3700)]:
            frames.append(render(stage, progress))
            durations.append(duration)
    durations[-1] = 6500
    poster.save(OUTPUT / "session-flow.png", optimize=True)
    palette = poster.quantize(colors=256, method=Image.Quantize.MEDIANCUT)
    indexed = [frame.quantize(palette=palette, dither=Image.Dither.NONE)
               for frame in frames]
    indexed[0].save(OUTPUT / "session-flow.gif", save_all=True,
                    append_images=indexed[1:], duration=durations, loop=0,
                    optimize=True, disposal=1)
    print(f"Wrote {OUTPUT / 'session-flow.gif'} and session-flow.png")


if __name__ == "__main__":
    main()
