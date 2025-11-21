# parsers/jcsales.py
#
# JC Sales PDF parser → returns (rows_df, invoice_number)
# rows_df columns (exactly what app.py expects from JC): ITEM, DESCRIPTION, PACK, COST, UNIT
#
# Handles:
#  • ITEM can be 4–6 digits (e.g., 0522, 86624, 116870)
#  • OCR-merged tokens before the first quantity (e.g., "16 OZ1 1 PK" → "16 OZ 1 1 PK")
#  • Wrapped lines: if the numeric tail (R-QTY S-QTY UM #/UM UNIT_P UM_P EXT_P [PACK2]) is on the next line,
#    we join lines until the tail is present.
#
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

# Prices like 2.39, 28.68, with optional $ and thousands
_MONEY = r"(\$?\d{1,3}(?:,\d{3})*\.\d{2}|\$?\d+\.\d{2})"

# Full numeric tail after DESCRIPTION we want to detect on a (possibly-stitched) logical line
_NUMERIC_TAIL = re.compile(
    rf"""
    \b(?P<rqty>\d+)\s+(?P<sqty>\d+)\s+(?P<um>[A-Z]+)\s+(?P<pack>\d+)\s+
    (?P<unit>{_MONEY})\s+(?P<cost>{_MONEY})\s+{_MONEY}(?:\s+(?P<pack2>\d+))?
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Main line pattern: LINE# [flag] ITEM DESCRIPTION TAIL
PRIMARY_LINE_RE = re.compile(
    rf"""
    ^\s*
    \d+\s+                       # LINE #
    (?:[A-Z]\s+)?                # optional flag (T/C)
    (?P<item>\d{{4,6}})\s+       # ITEM (4–6 digits)
    (?P<desc>.+?)\s+             # DESCRIPTION (lazy)
    (?P<rqty>\d+)\s+(?P<sqty>\d+)\s+(?P<um>[A-Z]+)\s+(?P<pack>\d+)\s+
    (?P<unit>{_MONEY})\s+(?P<cost>{_MONEY})\s+{_MONEY}(?:\s+(?P<pack2>\d+))?
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Accept lines that may be missing the explicit leading line number / flag
FALLBACK_LINE_RE = re.compile(
    rf"""
    ^\s*
    (?:\d+\s+[A-Z]\s+|\d+\s+)?   # optional LINE# and/or flag
    (?P<item>\d{{4,6}})\s+       # ITEM (4–6 digits)
    (?P<desc>.+?)\s+
    (?P<rqty>\d+)\s+(?P<sqty>\d+)\s+(?P<um>[A-Z]+)\s+(?P<pack>\d+)\s+
    (?P<unit>{_MONEY})\s+(?P<cost>{_MONEY})\s+{_MONEY}(?:\s+(?P<pack2>\d+))?
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
    try:
        return int(round(float(str(x).replace(",", "").strip())))
    except Exception:
        return default


def _extract_invoice_number(all_text: str) -> Optional[str]:
    m = INVOICE_RE.search(all_text or "")
    return m.group(1).upper() if m else None


def _fix_merged_qty_tokens(line: str) -> str:
    """
    Fix OCR-merged patterns right before R-QTY (first integer in the numeric tail).
    - Insert a space at any letter→digit boundary.
    Example: "... 16 OZ1 1 PK ..." → "... 16 OZ 1 1 PK ..."
    """
    line = re.sub(r'(?<=[A-Za-z])(?=\d)', ' ', line)
    return " ".join(line.split())


def _is_header_footer(l: str) -> bool:
    u = l.upper()
    if not u.strip():
        return True
    # Skip typical JC headers / footers
    if "LINE # ITEM DESCRIPTION" in u:
        return True
    if "JCSALES" in u and "CUSTOMER COPY" in u:
        return True
    if "PAGE" in u and "PRINTED" in u:
        return True
    if "BILL TO:" in u or "SHIP TO:" in u:
        return True
    if "INVOICE" in u and "OSI" in u:
        # don't drop the line entirely; but often the useful data is elsewhere
        return False
    return False


def _stitch_logical_lines(raw_lines: List[str]) -> List[str]:
    """
    Turn PDF text lines into logical lines that end with the full numeric tail.
    If a candidate line looks like a JC item (has an ITEM code) but lacks the tail,
    keep appending the next physical line until the tail is present or we hit a hard break.
    """
    logical = []
    buf = ""

    def finalize_buffer():
        nonlocal buf
        if buf.strip():
            logical.append(buf.strip())
        buf = ""

    i = 0
    while i < len(raw_lines):
        s = " ".join(raw_lines[i].split())
        i += 1
        if _is_header_footer(s):
            # Finish any existing buffer and skip headers/footers
            finalize_buffer()
            continue

        # Always fix merged tokens early
        s = _fix_merged_qty_tokens(s)

        # Start or extend buffer
        if not buf:
            buf = s
        else:
            buf = (buf + " " + s).strip()

        # If this buffer already contains a numeric tail, we can finalize it.
        if _NUMERIC_TAIL.search(buf):
            finalize_buffer()
            continue

        # Otherwise, keep looping and appending subsequent lines until tail appears.
        # The loop above will keep accumulating until _NUMERIC_TAIL matches or we run out.
        # At EOF, finalize whatever we have (may not parse).
        if i == len(raw_lines):
            finalize_buffer()

    return logical


def _parse_lines(text: str) -> pd.DataFrame:
    # Split, stitch, then parse
    raw_lines = (text or "").splitlines()
    logical_lines = _stitch_logical_lines(raw_lines)

    rows: List[dict] = []
    for idx, raw in enumerate(logical_lines):
        line = raw.strip()
        if not line:
            continue

        # Try primary, then fallback
        m = PRIMARY_LINE_RE.match(line)
        if not m:
            m = FALLBACK_LINE_RE.match(line)
        if not m:
            continue

        item = m.group("item").strip()
        desc = m.group("desc").strip()
        pack = _to_int(m.group("pack"))
        pack2 = m.group("pack2")
        if pack2:
            pack = _to_int(pack2) or pack

        unit = _to_float(m.group("unit"))
        cost = _to_float(m.group("cost"))

        if not item or not desc or pack <= 0 or unit is None or np.isnan(unit) or cost is None or np.isnan(cost):
            continue

        # sanity: if UNIT > COST (rare OCR quirk), swap
        if unit > cost:
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
        """
        Returns (items_df, invoice_number)
        items_df has columns: ITEM, DESCRIPTION, PACK, COST, UNIT
        """
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
