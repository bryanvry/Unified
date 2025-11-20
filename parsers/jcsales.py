# parsers/jcsales.py
# Robust JC Sales PDF parser that returns a pandas.DataFrame
# Columns returned: ITEM, DESCRIPTION, PACK, UNIT, COST (app computes UPC/NOW/DELTA/RETAIL)
# Returns: (df, invoice_no)

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
      - Optional single-letter flag (T, C, etc.) may appear after the line number.
      - PACK is taken from #/UM immediately after UM.
      - COST = UM_P (case price); UNIT is recomputed as COST / PACK (fallback to UNIT_P if PACK is zero).
    """

    # Regex capturing the row with optional flag and optional trailing fields.
    # Groups:
    #  1: line number
    #  2: optional flag (T/C/…)
    #  3: ITEM (digits)
    #  4: DESCRIPTION (greedy, trimmed)
    #  5: R-QTY (int)
    #  6: S-QTY (int)
    #  7: UM (e.g., PK)
    #  8: #/UM (PACK)
    #  9: UNIT_P
    # 10: UM_P (COST)
    # 11: EXT_P (optional)
    # 12: trailing PACK echo (optional)
    _ROW_RE = re.compile(
        r"""
        ^\s*
        (\d+)\s+                        # line number
        (?:(?:([A-Z]))\s+)?             # optional flag (e.g., 'T', 'C')
        (\d{3,})\s+                     # ITEM number (3+ digits)
        (.+?)\s+                        # DESCRIPTION
        (\d+)\s+                        # R-QTY
        (\d+)\s+                        # S-QTY
        ([A-Z]{1,3})\s+                 # UM (PK, etc.)
        (\d+)\s+                        # #/UM (PACK)
        (\d+\.\d{2})\s+                 # UNIT_P
        (\d+\.\d{2})                    # UM_P (COST)
        (?:\s+(\d+\.\d{2}))?            # EXT_P optional
        (?:\s+(\d+))?                   # trailing pack echo optional
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

    def parse(self, uploaded_pdf) -> Tuple[pd.DataFrame, str]:
        """Parse a JC Sales PDF, returning (DataFrame, invoice_no)."""
        if pdfplumber is None:
            return pd.DataFrame(columns=["ITEM", "DESCRIPTION", "PACK", "UNIT", "COST"]), "UNKNOWN"

        try:
            uploaded_pdf.seek(0)
        except Exception:
            pass

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

                    item = m.group(3)
                    desc = m.group(4).strip()
                    pack_str = m.group(8)
                    unit_p_str = m.group(9)
                    um_p_str = m.group(10)

                    # Parse numerics safely
                    try:
                        pack = int(pack_str)
                    except Exception:
                        pack = 0
                    try:
                        unit_p = float(unit_p_str)
                    except Exception:
                        unit_p = 0.0
                    try:
                        cost = float(um_p_str)  # COST = UM_P (case price)
                    except Exception:
                        cost = 0.0

                    unit = (cost / pack) if pack else unit_p

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

        if not rows:
            return pd.DataFrame(columns=["ITEM", "DESCRIPTION", "PACK", "UNIT", "COST"]), inv_no

        df = pd.DataFrame(rows, columns=["ITEM", "DESCRIPTION", "PACK", "UNIT", "COST"])
        return df, inv_no
