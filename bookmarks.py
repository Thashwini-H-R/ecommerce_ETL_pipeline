"""Simple bookmark utility to store last-processed ids/timestamps for incremental ingestion."""
from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional


BOOKMARKS_FILE = os.environ.get("ETL_BOOKMARKS_FILE", "./staging/bookmarks.json")


def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def read_bookmarks() -> Dict[str, Any]:
    _ensure_dir(BOOKMARKS_FILE)
    if not os.path.exists(BOOKMARKS_FILE):
        return {}
    with open(BOOKMARKS_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            return {}


def write_bookmark(source: str, value: Any) -> None:
    _ensure_dir(BOOKMARKS_FILE)
    data = read_bookmarks()
    data[source] = value
    with open(BOOKMARKS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, default=str)


def get_bookmark(source: str) -> Optional[Any]:
    return read_bookmarks().get(source)
