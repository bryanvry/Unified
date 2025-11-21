# parsers/jcsales.py
#
# JC Sales PDF parser → returns (rows_df, invoice_number)
# rows_df columns (exactly what app.py expects from JC): ITEM, DESCRIPTION, PACK, COST, UNIT
#
# Line format (examples):
#   1 14158 AXION DISH LIQUID LEMON 900ML 1 1 PK 12 2.39 28.68 28.68 12
#   100 T 118815 TOY DOCTOR PLAY SET ASST COLOR 1 1 PK 24 0.85 20.40 20.40 24
# Problem cases (OCR-merged):
#   "... 16 OZ1 1 PK ..."  → needs a space before the first qty: "OZ 1 1 PK"
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

_money = r"(\$?\d{1,3}(?:,\d{3})*\.\d{2}|\$?\d+\.\d{2})"

PRIMARY_LINE_RE = re.compile(
    rf"""
    ^\s*
    \d+\s+                       # LINE #
    (?:[A-Z]\s+)?                # optional flag (T/C)
    (?P<item>\d{{5,6}})\s+       # ITEM (5 or 6 digits)
    (?P<desc>.+?)\s+             # DESCRIPTION (greedy until numeric cols)
    (?P<rqty>\d+)\s+             # R-QTY
    (?P<sqty>\d+)\s+             # S-QTY
    (?P<um>[A-Z]+)\s+            # UM (e.g., PK)
    (?P<pack>\d+)\s+             # #/UM
    (?P<unit>{_money})\s+        # UNIT_P
    (?P<cost>{_money})\s+        # UM_P (case)
    {_money}                     # EXT_P (ignored)
    (?:\s+(?P<pack2>\d+))?       # optional trailing pack override
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

FALLBACK_LINE_RE = re.compile(
    rf"""
    ^\s*
    (?:\d+\s+[A-Z]\s+|\d+\s+)?   # optional LINE# and/or flag
    (?P<item>\d{{5,6}})\s+       # ITEM
    (?P<desc>.+?)\s+
    (?P<rqty>\d+)\s+(?P<sqty>\d+)\s+(?P<um>[A-Z]+)\s+(?P<pack>\d+)\s+
    (?P<unit>{_money})\s+(?P<cost>{_money})\s+{_money}
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
    Typical bad case: "... 16 OZ1 1 PK ..."  → add a space before the '1'.
    Generic rule:
      - Insert a space between a letter and a digit when that digit is the
        start of the R-QTY/S-QTY cluster.
    We’ll first do a conservative pass inserting spaces at letter→digit
    boundaries, then collapse multi-spaces.
    """
    # Only add space when a letter is immediately followed by a digit.
    line = re.sub(r'(?<=[A-Za-z])(?=\d)', ' ', line)
    # Also handle cases like "OZ1 1 PK" where the first digit got glued to the unit token:
    # After the above, "OZ 1 1 PK" is fine.
    return " ".join(line.split())


def _parse_lines(text: str) -> pd.DataFrame:
    rows: List[dict] = []
    for idx, raw in enumerate((text or "").splitlines()):
        if not raw:
            continue
        pre = " ".join(raw.split())
        if not pre or "LINE # ITEM DESCRIPTION" in pre.upper():
            continue

        # Fix common OCR merges *before* applying the regex
        line = _fix_merged_qty_tokens(pre)

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

        # sanity: if UNIT > COST, swap (rare OCR oddities)
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
