# parsers/jcsales.py
# Robust JC Sales PDF parser
# - Parses every item line under the "LINE # ITEM DESCRIPTION ..." table across all pages.
# - Accepts optional leading flags (T, C, etc.) after the line number.
# - Uses the #/UM that appears immediately after UM as authoritative PACK.
# - COST = UM_P (case pack price); UNIT = COST / PACK; RETAIL = UNIT * 2 (app can recompute if needed).
# - Returns (rows, invoice_no) where rows is a list of dicts with the minimal columns the app expects.

from __future__ import annotations

import re
from typing import List, Tuple, Dict, Any

import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None


class JCSalesParser:
    """
    Extracts JC Sales invoice line items from PDF text.

    Expected header somewhere on the page:
      LINE # ITEM DESCRIPTION Crv R-QTY S-QTY UM #/UM UNIT_P UM_P EXT_P

    Typical item row (examples from user):
      1 14158 AXION DISH LIQUID LEMON 900ML 1 1 PK 2.39 28.68 28.68 12
      100 T 118815 TOY DOCTOR PLAY SET ASST COLOR 1 1 PK 0.85 20.40 20.40 24

    Notes:
      - There may be a single letter flag (T, C, etc.) after the line number.
      - We treat UM_P as COST; UNIT is recomputed as COST / PACK to avoid rounding drift.
      - The final pack echo (the very last integer) isn't guaranteed; PACK is taken from #/UM after UM.
    """

    # compile a permissive regex for lines
    # groups:
    #  1: line number
    #  2: (optional) flag like 'T' or 'C'
    #  3: ITEM (digits, can include leading zeros)
    #  4: DESCRIPTION (greedy, trimmed)
    #  5: R-QTY (int)
    #  6: S-QTY (int)
    #  7: UM (e.g., PK)
    #  8: #/UM (PACK, int)
    #  9: UNIT_P (float like 2.39)
    # 10: UM_P (float like 28.68)  <-- this is COST
    # 11: EXT_P (optional float)
    # 12: trailing PACK echo (optional int)
    _ROW_RE = re.compile(
        r"""
        ^\s*
        (\d+)\s+                        # line number
        (?:(?:([A-Z]))\s+)?             # optional flag (e.g., 'T', 'C')
        (\d{3,})\s+                     # ITEM number (3+ digits)
        (.+?)\s+                        # DESCRIPTION (lazy until the qtys)
        (\d+)\s+                        # R-QTY
        (\d+)\s+                        # S-QTY
        ([A-Z]{1,3})\s+                 # UM (PK, etc.)
        (\d+)\s+                        # #/UM (PACK authoritative)
        (\d+\.\d{2})\s+                 # UNIT_P
        (\d+\.\d{2})                    # UM_P  (COST)
        (?:\s+(\d+\.\d{2}))?            # EXT_P optional
        (?:\s+(\d+))?                   # optional trailing pack echo
        \s*$
        """,
        re.VERBOSE,
    )

    _INV_RE = re.compile(r"OSI\d+", re.IGNORECASE)

    def _extract_invoice_no(self, page_texts: List[str]) -> str:
        for txt in page_texts:
            m = self._INV_RE.search(txt)
            if m:
                return m.group(0).upper()
        return "UNKNOWN"

    def parse(self, uploaded_pdf) -> Tuple[List[Dict[str, Any]], str]:
        """Parse a JC Sales PDF, returning (rows, invoice_no).
        Each row dict contains: ITEM, DESCRIPTION, PACK, UNIT, COST.
        App will add UPC/NOW/DELTA/RETAIL and do master/pricebook joins.
        """
        if pdfplumber is None:
            return [], "UNKNOWN"

        try:
            uploaded_pdf.seek(0)
        except Exception:
            pass

        # collect page texts, then parse lines
        page_texts: List[str] = []
        rows: List[Dict[str, Any]] = []

        with pdfplumber.open(uploaded_pdf) as pdf:
            for page in pdf.pages:
                txt = page.extract_text() or ""
                if not txt.strip():
                    continue
                page_texts.append(txt)

                for raw in txt.splitlines():
                    line = raw.strip()
                    if not line or len(line) < 8:
                        continue

                    m = self._ROW_RE.match(line)
                    if not m:
                        continue

                    # unpack captures
                    # line_no = m.group(1)  # not used
                    # flag    = m.group(2)  # not used
                    item = m.group(3)
                    desc = m.group(4).strip()
                    # r_qty  = m.group(5)  # not used
                    # s_qty  = m.group(6)  # not used
                    # um     = m.group(7)  # not used, typically 'PK'
                    pack_str = m.group(8)
                    unit_p_str = m.group(9)
                    um_p_str = m.group(10)
                    # ext_p_str = m.group(11)   # not needed
                    # pack_echo = m.group(12)   # optional echo

                    try:
                        pack = int(pack_str)
                    except Exception:
                        # if somehow missing, fallback to optional echo
                        try:
                            pack = int(m.group(12)) if m.group(12) else 0
                        except Exception:
                            pack = 0

                    try:
                        unit_p = float(unit_p_str)
                    except Exception:
                        unit_p = 0.0

                    try:
                        cost = float(um_p_str)  # COST = UM_P
                    except Exception:
                        cost = 0.0

                    # authoritative UNIT from COST / PACK (avoid display rounding)
                    unit = cost / pack if pack else unit_p

                    rows.append(
                        {
                            "ITEM": item,
                            "DESCRIPTION": re.sub(r"\s{2,}", " ", desc).strip(),
                            "PACK": pack,
                            "UNIT": round(unit, 2),
                            "COST": round(cost, 2),
                        }
                    )

        inv_no = self._extract_invoice_no(page_texts)
        return rows, inv_no
