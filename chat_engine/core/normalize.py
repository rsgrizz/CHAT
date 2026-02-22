"""
Created by: Randy Grizzelli
Email: grizzellir@gmail.com
GitHub: https://github.com/rsgrizz
Version: 0.1
Date: 2026-02-21
Purpose: Communication Heuristics Analysis for Triage (CHAT) engine for deterministic prioritization of exported communications.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from zoneinfo import ZoneInfo

from chat_engine.core.ingest import IngestRow


@dataclass(frozen=True)
class SchemaMapping:
    """
    Maps tool export columns to CHAT normalized fields.
    Column names must match the source headers exactly.

    If uniqid_col is not provided or empty, CHAT generates a stable msg_id
    using source_row and a deterministic key.
    """
    timestamp_col: str
    from_col: str
    to_col: str
    message_col: str
    uniqid_col: Optional[str] = None
    thread_col: Optional[str] = None


@dataclass(frozen=True)
class NormalizedMessage:
    """
    Stable internal record for the rest of the pipeline.
    ts_utc is ISO 8601 in UTC if parsed, otherwise None.
    ts_raw is preserved exactly as provided.
    """
    msg_id: str
    source_row: int

    ts_raw: str
    ts_utc: Optional[str]

    sender: str
    recipient: str
    body: str

    thread_id: str


def normalize_row(
    row: IngestRow,
    mapping: SchemaMapping,
    assume_tz: str = "America/New_York",
) -> NormalizedMessage:
    data = row.data

    ts_raw = (data.get(mapping.timestamp_col, "") or "").strip()
    sender = _clean_party(data.get(mapping.from_col, ""))
    recipient = _clean_party(data.get(mapping.to_col, ""))
    body = (data.get(mapping.message_col, "") or "").strip()

    thread_id = ""
    if mapping.thread_col:
        thread_id = (data.get(mapping.thread_col, "") or "").strip()

    msg_id = ""
    if mapping.uniqid_col:
        msg_id = (data.get(mapping.uniqid_col, "") or "").strip()

    if not msg_id:
        msg_id = f"ROW{row.source_row}:{_stable_key(ts_raw, sender, recipient)}"

    ts_utc = parse_timestamp_to_utc_iso(ts_raw, assume_tz=assume_tz)

    return NormalizedMessage(
        msg_id=msg_id,
        source_row=row.source_row,
        ts_raw=ts_raw,
        ts_utc=ts_utc,
        sender=sender,
        recipient=recipient,
        body=body,
        thread_id=thread_id,
    )


def parse_timestamp_to_utc_iso(
    ts_raw: str,
    assume_tz: str = "America/New_York",
) -> Optional[str]:
    """
    Parse common export timestamps to UTC ISO 8601.

    Behavior:
    - If timezone present, convert to UTC
    - If naive, assume assume_tz then convert to UTC
    - Returns UTC ISO string ending in Z, or None
    """
    s = (ts_raw or "").strip()
    if not s:
        return None

    dt = _try_fromiso(s)
    if dt is None:
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M",
            "%m/%d/%y %H:%M:%S",
            "%m/%d/%y %H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
        ):
            dt = _try_strptime(s, fmt)
            if dt is not None:
                break

    if dt is None:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(assume_tz))

    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.isoformat().replace("+00:00", "Z")


def _clean_party(v: str) -> str:
    s = (v or "").strip()
    return " ".join(s.split())


def _stable_key(ts_raw: str, sender: str, recipient: str) -> str:
    base = f"{ts_raw}|{sender}|{recipient}"
    return str(_fnv1a_32(base))


def _fnv1a_32(text: str) -> int:
    """
    Deterministic 32 bit FNV 1a hash.
    """
    h = 2166136261
    for b in text.encode("utf-8", errors="replace"):
        h ^= b
        h = (h * 16777619) & 0xFFFFFFFF
    return h


def _try_fromiso(s: str) -> Optional[datetime]:
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _try_strptime(s: str, fmt: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s, fmt)
    except Exception:
        return None