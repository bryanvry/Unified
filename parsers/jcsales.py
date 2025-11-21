# parsers/jcsales.py
#
# JC Sales PDF parser → returns (rows_df, invoice_number)
# rows_df columns (exactly what app.py expects from JC): ITEM, DESCRIPTION, PACK, COST, UNIT
#
# Improvements in this version:
#  • ITEM can be 4–6 digits (e.g., 0522, 86624, 116870)
#  • Global fix for OCR-merged tokens like "OZ1 1" → "OZ 1 1"
#  • Multi-line parsing: DESCRIPTION can span lines; we match the numeric tail anywhere after DESCRIPTION
#  • Fallback stitching kept for tricky cases
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

# Numeric tail: R-QTY S-QTY UM #/UM UNIT_P UM_P EXT_P [PACK2]
_TAIL_RE = re.compile(
    rf"""
    \b(?P<rqty>\d+)\s+(?P<sqty>\d+)\s+(?P<um>[A-Z]+)\s+(?P<pack>\d+)\s+
    (?P<unit>{_MONEY})\s+(?P<cost>{_MONEY})\s+{_MONEY}(?:\s+(?P<pack2>\d+))?
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Multi-line capture: start-of-line LINE#, optional flag, ITEM(4–6), then any desc lazily, then the tail
# We *don't* anchor the tail to the end, and we enable DOTALL so desc may include line breaks.
MULTILINE_BLOCK_RE = re.compile(
    rf"""
    ^\s*
    \d+\s+                       # LINE #
    (?:[A-Z]\s+)?                # optional flag (T/C)
    (?P<item>\d{{4,6}})\s+       # ITEM (4–6 digits)
    (?P<body>.+?)                # everything until we can find the numeric tail inside
    (?P<tail>
        \b\d+\s+\d+\s+[A-Z]+\s+\d+\s+{_MONEY}\s+{_MONEY}\s+{_MONEY}(?:\s+\d+)?
    )
    """,
    re.IGNORECASE | re.VERBOSE | re.MULTILINE | re.DOTALL,
)

INVOICE_RE = re.compile(r"\b(OSI\d{5,})\b", re.IGNORECASE)

# ---------- helpers ----------
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

def _fix_merged_tokens(text: str) -> str:
    # Insert space at any letter→digit boundary globally (fixes "OZ1 1", "MM1", etc.)
    text = re.sub(r'(?<=[A-Za-z])(?=\d)', ' ', text)
    # Normalize multiple spaces
    return re.sub(r"[ \t]+", " ", text)

# ---------- parsing core ----------
def _parse_multiline(all_text: str) -> pd.DataFrame:
    rows: List[dict] = []
    order = 0

    for m in MULTILINE_BLOCK_RE.finditer(all_text):
        item = (m.group("item") or "").strip()
        body = (m.group("body") or "").strip()

        # Find the numeric tail inside the body+tail chunk
        tail_chunk = m.group("tail")
        t = _TAIL_RE.search(tail_chunk)
        if not t:
            # Extremely unlikely since MULTILINE_BLOCK_RE guaranteed a tail-shaped chunk
            continue

        # Description is body up to the *start* of the tail inside (desc may include newlines → squash spaces)
        # We re-search tail inside (body + tail) to find exact start index.
        bt = body + " " + tail_chunk
        t2 = _TAIL_RE.search(bt)
        if not t2:
            continue
        desc = bt[: t2.start()].strip()
        desc = re.sub(r"\s+", " ", desc)

        pack = _to_int(t.group("pack"))
        pack2 = t.group("pack2")
        if pack2:
            pack = _to_int(pack2) or pack

        unit = _to_float(t.group("unit"))
        cost = _to_float(t.group("cost"))

        if not item or not desc or pack <= 0 or unit is None or np.isnan(unit) or cost is None or np.isnan(cost):
            continue

        # sanity: sometimes OCR swaps unit/cost
        if unit > cost:
            unit, cost = cost, unit

        rows.append({
            "ITEM": item,
            "DESCRIPTION": desc,
            "PACK": int(pack),
            "COST": float(cost),
            "UNIT": float(unit),
            "_order": order
        })
        order += 1

    if not rows:
        return pd.DataFrame(columns=WANT_COLS)

    df = pd.DataFrame(rows).sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)
    return df[WANT_COLS]

# Fallback single-line path (kept, but rarely hit after multiline)
_SINGLELINE_TAIL = re.compile(
    rf"""
    (?P<rqty>\d+)\s+(?P<sqty>\d+)\s+(?P<um>[A-Z]+)\s+(?P<pack>\d+)\s+
    (?P<unit>{_MONEY})\s+(?P<cost>{_MONEY})\s+{_MONEY}(?:\s+(?P<pack2>\d+))?\s*$
    """,
    re.IGNORECASE | re.VERBOSE
)
_SINGLELINE_HEAD = re.compile(
    r"^\s*(?:\d+\s+[A-Z]\s+|\d+\s+)?(?P<item>\d{4,6})\s+(?P<desc>.+?)\s+",
    re.IGNORECASE
)

def _parse_singleline_stitched(all_text: str) -> pd.DataFrame:
    lines = all_text.splitlines()
    # simple stitch: join consecutive lines until we see something tail-like
    logical: List[str] = []
    buf = ""
    def flush():
        nonlocal buf
        if buf.strip():
            logical.append(buf.strip())
        buf = ""
    for s in lines:
        s = s.strip()
        if not s:
            flush()
            continue
        # headers/footers
        u = s.upper()
        if ("LINE # ITEM DESCRIPTION" in u) or ("JCSALES" in u and "CUSTOMER COPY" in u) or ("BILL TO:" in u) or ("SHIP TO:" in u) or ("PAGE" in u and "PRINTED" in u):
            flush()
            continue
        s = _fix_merged_tokens(s)
        buf = (buf + " " + s).strip() if buf else s
        if _SINGLELINE_TAIL.search(buf):
            flush()
    flush()

    rows: List[dict] = []
    order = 0
    for ln in logical:
        head = _SINGLELINE_HEAD.match(ln)
        tail = _SINGLELINE_TAIL.search(ln)
        if not head or not tail:
            continue
        item = head.group("item")
        # desc is between head end and tail start
        desc = ln[head.end(): tail.start()].strip()
        desc = re.sub(r"\s+", " ", desc)

        pack = _to_int(tail.group("pack"))
        pack2 = tail.group("pack2")
        if pack2:
            pack = _to_int(pack2) or pack
        unit = _to_float(tail.group("unit"))
        cost = _to_float(tail.group("cost"))
        if not item or not desc or pack <= 0 or unit is None or np.isnan(unit) or cost is None or np.isnan(cost):
            continue
        if unit > cost:
            unit, cost = cost, unit
        rows.append({
            "ITEM": item,
            "DESCRIPTION": desc,
            "PACK": int(pack),
            "COST": float(cost),
            "UNIT": float(unit),
            "_order": order
        })
        order += 1

    if not rows:
        return pd.DataFrame(columns=WANT_COLS)
    df = pd.DataFrame(rows).sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)
    return df[WANT_COLS]

# ---------- public API ----------
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
                texts = []
                for p in pdf.pages:
                    t = p.extract_text() or ""
                    texts.append(t)
                all_text = "\n".join(texts)
        except Exception:
            return pd.DataFrame(columns=WANT_COLS), None

        # Global merge-fix BEFORE parsing
        all_text = _fix_merged_tokens(all_text)

        # Try multi-line first
        df = _parse_multiline(all_text)

        # Fallback to stitched single-line if still short
        if df.empty or len(df) < 150:  # heuristic to catch your 144/157 situation
            alt = _parse_singleline_stitched(all_text)
            if not alt.empty:
                # Merge unique rows by ITEM + COST + PACK to avoid duplicates
                if df.empty:
                    df = alt
                else:
                    df = pd.concat([df, alt], ignore_index=True)
                    df.drop_duplicates(subset=["ITEM", "PACK", "COST"], keep="first", inplace=True)
                    df.reset_index(drop=True, inplace=True)

        inv = _extract_invoice_number(all_text)
        return (df[WANT_COLS] if not df.empty else pd.DataFrame(columns=WANT_COLS)), inv
