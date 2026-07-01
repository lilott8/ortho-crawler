"""Shared content-addressed storage helpers.

Files live at ``<download_dir>/<content_id[:2]>/<content_id><ext>`` so identical
bytes always land at one path (free dedupe), regardless of how many DB rows
reference them. Used by both the wiki media downloader (``scraper.py``) and the
icon pipeline (``icon_pipeline.py``). All functions here are blocking — call
them via ``asyncio.to_thread`` from async code, never directly.
"""

from __future__ import annotations

import json
import os


def content_path(download_dir: str, content_id: str, ext: str) -> str:
    return os.path.join(download_dir, content_id[:2], content_id + ext)


def read_bytes(path: str) -> bytes:
    with open(path, "rb") as fh:
        return fh.read()


def write_bytes(path: str, data: bytes) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(data)


def write_sidecar(path: str, payload: dict) -> None:
    """Write a pretty-printed JSON sidecar, unless one already exists."""
    if os.path.exists(path):
        return
    write_bytes(path, json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8"))
