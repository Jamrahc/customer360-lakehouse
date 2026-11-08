"""Watermarks for incremental extraction, stored as a small JSON file in the lake."""

from __future__ import annotations

import json
import os

from .config import Settings


def _path(settings: Settings) -> str:
    return os.path.join(settings.state, "watermarks.json")


def get_watermark(settings: Settings, key: str, default: str = "1900-01-01 00:00:00") -> str:
    try:
        with open(_path(settings)) as f:
            return json.load(f).get(key, default)
    except FileNotFoundError:
        return default


def set_watermark(settings: Settings, key: str, value: str) -> None:
    os.makedirs(settings.state, exist_ok=True)
    data: dict[str, str] = {}
    if os.path.exists(_path(settings)):
        with open(_path(settings)) as f:
            data = json.load(f)
    data[key] = value
    tmp = _path(settings) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, _path(settings))  # atomic: a crash never leaves a half-written file
