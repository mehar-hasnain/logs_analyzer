import gzip
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

TS_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)\s")
BLOCK_START_RE = re.compile(r"Start syncing the balance\s*\{", re.IGNORECASE)
PROC_MSG_RE = re.compile(r"INFO\s+Processing message\s+(?P<msgid>[a-f0-9-]{36})", re.IGNORECASE)
SKIP_RE = re.compile(r"Skipping the balance sync for create subscription", re.IGNORECASE)

# Capture simple key: value lines inside the transaction block (loose, single quotes tolerated)
KV_LINE_RE = re.compile(r"^\s*([A-Za-z_]\w*)\s*:\s*(.+?)(?:,)?\s*$")

MONEY_FIELDS = ("amount", "vat", "oldBalance", "newBalance", "paymentBalance")
CURR_DECIMALS = {"SAR": 3, "BHD": 4}  # default will be 2 dp
ZERO_TOL = 1e-9

# module level logger
logger = logging.getLogger(__name__)


def parse_iso(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def coerce_scalar(val: str):
    v = val.strip()
    # Strip enclosing quotes (single or double)
    if (v.startswith("'") and v.endswith("'")) or (v.startswith('"') and v.endswith('"')):
        return v[1:-1]
    # Try int
    try:
        return int(v)
    except ValueError:
        pass
    # Try float
    try:
        return float(v)
    except ValueError:
        pass
    # true/false
    if v.lower() in ("true", "false"):
        return v.lower() == "true"
    # null-like
    if v.lower() in ("null", "none"):
        return None
    return v


def parse_transaction_block(lines: List[str]) -> Dict[str, Any]:
    """
    Extract the balanced 'transaction: { ... }' object and parse flat key: value pairs.
    This is robust to braces inside quoted strings (e.g., metadata JSON),
    because we scan until the *matching* closing brace instead of using a non-greedy regex.
    """
    content = "\n".join(lines)

    # Find the literal start of 'transaction: {'
    m = re.search(r"transaction\s*:\s*\{", content, re.IGNORECASE)
    if not m:
        return {}

    # Index of the first '{' after 'transaction:'
    start = content.find("{", m.end() - 1)
    if start == -1:
        return {}

    # Balanced-brace scan to the matching '}'
    brace = 0
    end = None
    for idx in range(start, len(content)):
        ch = content[idx]
        if ch == "{":
            brace += 1
        elif ch == "}":
            brace -= 1
            if brace == 0:
                end = idx
                break

    if end is None:
        # Unbalanced block; fail gracefully
        return {}

    # Inner text of the transaction object, without the outer braces
    tblock_text = content[start + 1:end]
    tblock = tblock_text.splitlines()

    out: Dict[str, Any] = {}
    for ln in tblock:
        ln = ln.strip()
        if not ln or ln.startswith("//"):
            continue
        m_kv = KV_LINE_RE.match(ln)
        if not m_kv:
            continue
        key = m_kv.group(1)
        raw = m_kv.group(2).strip()
        # drop a trailing comma (already optional in regex, keep for safety)
        if raw.endswith(","):
            raw = raw[:-1].rstrip()
        out[key] = coerce_scalar(raw)

    # Best-effort parse of metadata if it looks like JSON-in-a-string
    meta = out.get("metadata")
    if isinstance(meta, str):
        s = meta.strip()
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                import json
                out["metadata_json"] = json.loads(s)
            except Exception:
                pass
    out = _round_money(out)
    return out


def open_maybe_gz(path: str | Path, mode: str = "rt", encoding: str = "utf-8", errors: str = "ignore"):
    """
    Open a file that may be gzipped (even if it lacks a .gz extension).

    This inspects the first two bytes of the file to detect the gzip magic
    number (0x1f, 0x8b). If detected, it uses ``gzip.open``; otherwise it
    falls back to the built-in ``open``. Always returns a text-mode file-like
    object.
    """
    p = Path(path)
    try:
        with p.open("rb") as fh:
            magic = fh.read(2)
    except FileNotFoundError:
        raise

    is_gz = magic == b"\x1f\x8b"
    if is_gz:
        return gzip.open(p, mode, encoding=encoding, errors=errors)
    return p.open(mode, encoding=encoding, errors=errors)

def _round_money(tx: Dict[str, Any]) -> Dict[str, Any]:
    """Clamp near-zero float noise and round money fields per currency using built-in round()."""
    out = dict(tx)
    cur = str(out.get("currency") or "").upper()
    dps = CURR_DECIMALS.get(cur, 3)

    for k in MONEY_FIELDS:
        v = out.get(k)
        if v is None or v == "":
            continue
        try:
            x = float(v)
        except (TypeError, ValueError):
            continue
        if abs(x) < ZERO_TOL:
            x = 0.0
        out[k] = round(x, dps)
    return out


def parse_logs(log_dir: str | Path) -> List[Dict[str, Any]]:
    """
    Recursively scan ``log_dir`` for candidate log files (.log, .txt or gzip),
    parse each and return a list of event dictionaries ordered by user and
    timestamp. Supports both plain text and gzip-compressed files, even if
    they lack a .gz extension.

    Parameters
    ----------
    log_dir: str or Path
        Directory containing log files. Will be resolved to an absolute Path.

    Returns
    -------
    List[Dict[str, Any]]
        Parsed events in chronological order by userId and timestamp.
    """
    log_dir_path = Path(log_dir).expanduser().resolve()
    logger.info(f"Scanning log directory: {log_dir_path}")
    events: List[Dict[str, Any]] = []
    for root, _, files in os.walk(log_dir_path):
        for fn in files:
            # Only consider known log file extensions; gzipped files without extension
            if not fn.lower().endswith((".log", ".txt", ".gz")):
                continue
            path = Path(root) / fn
            try:
                f = open_maybe_gz(path, "rt", encoding="utf-8", errors="ignore")
            except Exception as exc:
                logger.warning(f"Failed to open {path}: {exc}")
                continue
            with f:
                buf = f.read().splitlines()
            logger.debug(f"Read {len(buf)} lines from {path}")

            i = 0
            last_msg_id: Optional[str] = None
            while i < len(buf):
                line = buf[i]
                ts_match = TS_RE.match(line)
                ts = parse_iso(ts_match.group("ts")) if ts_match else None

                # Track message id context if near
                mmsg = PROC_MSG_RE.search(line)
                if mmsg:
                    last_msg_id = mmsg.group("msgid")

                if SKIP_RE.search(line):
                    # Represent as a 'skip' event for completeness
                    events.append({
                        "timestamp": ts,
                        "messageId": last_msg_id,
                        "eventType": "SKIP_CREATE_SUBSCRIPTION",
                        "raw": line
                    })
                    i += 1
                    continue

                if BLOCK_START_RE.search(line):
                    # Collect until closing brace of the top-level block
                    block_lines = [line]
                    j = i + 1
                    brace_depth = 1
                    while j < len(buf) and brace_depth > 0:
                        block_lines.append(buf[j])
                        brace_depth += buf[j].count("{")
                        brace_depth -= buf[j].count("}")
                        j += 1
                    tx = parse_transaction_block(block_lines)
                    if tx:
                        events.append({
                            "timestamp": ts,
                            "messageId": last_msg_id,
                            "eventType": "BALANCE_SYNC",
                            **tx
                        })
                    i = j
                    continue

                i += 1

    # Normalize types & defaults
    for e in events:
        if e.get("currency") is None:
            e["currency"] = "UNKNOWN"
        if "type" in e and e["type"] is not None:
            e["type"] = str(e["type"]).upper()
        if "source" in e and e["source"] is not None:
            e["source"] = str(e["source"]).upper()
    # sort deterministically by user, timestamp, id and messageId
    events.sort(key=lambda x: (
        x.get("userId", ""),
        x.get("timestamp") or datetime.min.replace(tzinfo=timezone.utc),
        str(x.get("id", "")),
        str(x.get("messageId", ""))
    ))
    logger.info(f"Parsed {len(events)} events from {log_dir_path}")
    return events
