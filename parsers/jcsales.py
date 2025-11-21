# parsers/jcsales.py
# JC Sales PDF parser → returns (rows_df, invoice_number)
# rows_df columns: ITEM, DESCRIPTION, PACK, COST, UNIT

from __future__ import annotations
from typing import Tuple, List, Optional
import re
import numpy as np
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None

WANT_COLS = ["ITEM", "DESCRIPTION", "PACK", "COST", "UNIT"]

_MONEY = r"(\$?\d{1,3}(?:,\d{3})*\.\d{2}|\$?\d+\.\d{2})"

# UPDATED: Made the middle pack optional "(?:(?P<pack>\d+)\s+)?"
# This allows the regex to match lines where the pack count is at the very end.
_NUMERIC_TAIL = re.compile(
    rf"""
    \b(?P<rqty>\d+)\s+(?P<sqty>\d+)\s+(?P<um>[A-Z]+)\s+
    (?:(?P<pack>\d+)\s+)?
    (?P<unit>{_MONEY})\s+(?P<cost>{_MONEY})\s+{_MONEY}
    (?:\s+(?P<pack2>\d+))?
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

PRIMARY_LINE_RE = re.compile(
    rf"""
    ^\s*
    \d+\s+                       # LINE #
    (?:[A-Z]\s+)?                # optional flag (T/C)
    (?P<item>\d{{4,6}})\s+       # ITEM (4–6 digits)
    (?P<desc>.+?)\s+             # DESCRIPTION (lazy)
    (?P<rqty>\d+)\s+(?P<sqty>\d+)\s+(?P<um>[A-Z]+)\s+
    (?:(?P<pack>\d+)\s+)?
    (?P<unit>{_MONEY})\s+(?P<cost>{_MONEY})\s+{_MONEY}
    (?:\s+(?P<pack2>\d+))?
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

FALLBACK_LINE_RE = re.compile(
    rf"""
    ^\s*
    (?:\d+\s+[A-Z]\s+|\d+\s+)?   # optional LINE# and/or flag
    (?P<item>\d{{4,6}})\s+       # ITEM (4–6 digits)
    (?P<desc>.+?)\s+
    (?P<rqty>\d+)\s+(?P<sqty>\d+)\s+(?P<um>[A-Z]+)\s+
    (?:(?P<pack>\d+)\s+)?
    (?P<unit>{_MONEY})\s+(?P<cost>{_MONEY})\s+{_MONEY}
    (?:\s+(?P<pack2>\d+))?
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

INVOICE_RE = re.compile(r"\b(OSI\d{5,})\b", re.IGNORECASE)


def _to_float(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return np.nan
    if isinstance(x, (int, float, np.number)):
        return float(x)
    s = str(x).replace("$", "").replace(",", "").strip()
    s = re.sub(r"[^\d.\-]", "", s)
    try:
        return float(s)
    except Exception:
        return np.nan


def _to_int(x, default=0):
    if x is None:
        return default
    try:
        return int(round(float(str(x).replace(",", "").strip())))
    except Exception:
        return default


def _extract_invoice_number(all_text: str) -> Optional[str]:
    m = INVOICE_RE.search(all_text or "")
    return m.group(1).upper() if m else None


def _fix_merged_qty_tokens(line: str) -> str:
    # insert a space at any letter→digit boundary (e.g., "OZ1" → "OZ 1")
    line = re.sub(r'(?<=[A-Za-z])(?=\d)', ' ', line)
    # insert a space at any digit→letter boundary (e.g., "1PK" → "1 PK")
    line = re.sub(r'(?<=\d)(?=[A-Za-z])', ' ', line)
    return " ".join(line.split())


def _is_header_footer(l: str) -> bool:
    u = l.upper()
    if not u.strip():
        return True
    if "LINE # ITEM DESCRIPTION" in u:
        return True
    if "PAGE" in u and "PRINTED" in u:
        return True
    if "BILL TO:" in u or "SHIP TO:" in u:
        return True
    if "JCSALES" in u and "CUSTOMER COPY" in u:
        return True
    return False


def _stitch_logical_lines(raw_lines: List[str]) -> List[str]:
    """
    Build logical lines that end with the full numeric tail.
    """
    logical = []
    buf = ""

    def have_tail(s: str) -> bool:
        return bool(_NUMERIC_TAIL.search(s))

    def flush():
        nonlocal buf
        if buf.strip():
            logical.append(buf.strip())
        buf = ""

    i = 0
    while i < len(raw_lines):
        s = " ".join(raw_lines[i].split())
        i += 1

        # always fix merged tokens first
        s = _fix_merged_qty_tokens(s)

        if _is_header_footer(s):
            # if current buffer already complete, flush; else keep accumulating across the header
            if have_tail(buf):
                flush()
            continue

        # append to buffer
        if not buf:
            buf = s
        else:
            buf = (buf + " " + s).strip()

        # if buffer now complete, flush it
        if have_tail(buf):
            flush()

        # if EOF and something remains, flush whatever is there
        if i == len(raw_lines) and buf.strip():
            # Only flush if it looks like an item line: contains an ITEM code and at least one money value
            if re.search(r"\b\d{4,6}\b", buf) and re.search(r"\d+\.\d{2}", buf):
                flush()
            else:
                buf = ""

    return logical


def _parse_lines(text: str) -> pd.DataFrame:
    logical_lines = _stitch_logical_lines((text or "").splitlines())

    rows: List[dict] = []
    for idx, raw in enumerate(logical_lines):
        line = raw.strip()
        if not line:
            continue

        m = PRIMARY_LINE_RE.match(line)
        if not m:
            m = FALLBACK_LINE_RE.match(line)
        if not m:
            continue

        item = m.group("item").strip()
        desc = m.group("desc").strip()

        # UPDATED LOGIC: Pack might be in 'pack' (middle) or 'pack2' (end)
        pack = _to_int(m.group("pack"), default=0)
        pack2 = m.group("pack2")
        
        # If pack2 exists, it usually overrides or is the only pack available
        if pack2:
            p2 = _to_int(pack2)
            if p2 > 0:
                pack = p2
        
        # If pack is still 0 or missing, default to 1 to avoid division by zero later
        if pack <= 0:
            pack = 1

        unit = _to_float(m.group("unit"))
        cost = _to_float(m.group("cost"))

        # sanity checks
        if not item or not desc or unit is None or np.isnan(unit) or cost is None or np.isnan(cost):
            continue
        
        # Sometimes unit/cost are swapped in extraction or logic, ensure unit is smaller (Cost per item)
        if unit > cost and pack > 1:
            unit, cost = cost, unit

        rows.append({
            "ITEM": item,
            "DESCRIPTION": desc,
            "PACK": int(pack),
            "COST": float(cost),
            "UNIT": float(unit),
            "_order": idx
        })

    if not rows:
        return pd.DataFrame(columns=WANT_COLS)

    df = pd.DataFrame(rows).sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)
    return df[WANT_COLS]


class JCSalesParser:
    name = "JC Sales"

    def parse(self, uploaded_pdf) -> Tuple[pd.DataFrame, Optional[str]]:
        if pdfplumber is None:
            return pd.DataFrame(columns=WANT_COLS), None
        try:
            uploaded_pdf.seek(0)
        except Exception:
            pass

        fname = (getattr(uploaded_pdf, "name", "") or "").lower()
        if not fname.endswith(".pdf"):
            return pd.DataFrame(columns=WANT_COLS), None

        try:
            with pdfplumber.open(uploaded_pdf) as pdf:
                texts = [(p.extract_text() or "") for p in pdf.pages]
                all_text = "\n".join(texts)
                df = _parse_lines(all_text)
                inv = _extract_invoice_number(all_text)
                return df, inv
        except Exception:
            return pd.DataFrame(columns=WANT_COLS), None
