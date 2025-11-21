# parsers/jcsales.py
# JC Sales invoice parser (PDF → robust line parser only)
# - Avoids table/word-grid fallbacks to prevent duplicate rows
# - Parses lines using a right-to-left token approach
# - Preserves invoice order and hard de-dupes by (ITEM, DESCRIPTION, PACK, UM_P)
# - Returns: (DataFrame, invoice_number)
#
# Output columns expected by app:
#   ITEM (int), DESCRIPTION (str), PACK (int), COST (float, UM_P), UNIT (float, UNIT_P)
#
# Notes:
#   Line format (examples):
#   1 14158 AXION DISH LIQUID LEMON 900ML 1 1 PK 2.39 28.68 28.68 12
#   100 T 118815 TOY DOCTOR PLAY SET ASST COLOR 1 1 PK 0.85 20.40 20.40 24
#
#   From the right:
#     [ ... DESCRIPTION ... ] RQTY SQTY UM UNIT_P UM_P EXT_P PACK
#     We capture:
#       PACK      = last token (int)
#       EXT_P     = -2 (float)  (not used)
#       UM_P      = -3 (float)  -> COST
#       UNIT_P    = -4 (float)  -> UNIT
#       UM        = -5 ("PK")
#       SQTY      = -6 (int)    (not used)
#       RQTY      = -7 (int)    (not used)
#       DESCRIPTION = tokens between ITEM and RQTY
#       ITEM      = first integer token after optional flag (T/C) following the line number
#
#   We also read invoice number if a token like *OSI014135* appears.

from __future__ import annotations

from typing import List, Tuple, Optional
import re
import numpy as np
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None


DIGITS_RE = re.compile(r"^\d+$")


def _to_float(tok: str) -> Optional[float]:
    s = str(tok).replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return None


def _is_int(tok: str) -> bool:
    return bool(DIGITS_RE.match(str(tok).strip()))


def _clean_spaces(s: str) -> str:
    return re.sub(r"\s{2,}", " ", str(s).strip())


class JCSalesParser:
    """Parse JC Sales invoice PDFs with a strict, duplicate-safe line parser."""

    WANT_COLS = ["ITEM", "DESCRIPTION", "PACK", "COST", "UNIT"]

    HEADER_CUE = "LINE # ITEM DESCRIPTION"
    FOOTER_CUES = (
        "Customer Copy",
        "Printed.",           # page footer stamp
        "times printed",      # footer snippet
    )

    def _split_lines(self, page) -> List[str]:
        txt = page.extract_text(x_tolerance=1, y_tolerance=3) or ""
        raw_lines = [ln.rstrip() for ln in txt.splitlines()]
        # Strip obvious blank & footer noise early
        lines = []
        for ln in raw_lines:
            s = ln.strip()
            if not s:
                continue
            # drop page headers/footers and total summaries
            if any(cue in s for cue in self.FOOTER_CUES):
                continue
            if s.startswith("JCSALES ") and "Customer Copy" in s:
                continue
            lines.append(s)
        return lines

    def _find_invoice_number(self, lines: List[str]) -> Optional[str]:
        # Typically like "*OSI014135*"
        for ln in lines:
            m = re.search(r"\*([A-Za-z]{3}\d{6,})\*", ln)
            if m:
                return m.group(1)
        # Backup: sometimes "JCSALES OSI014135 ..."
        for ln in lines:
            m = re.search(r"\b([A-Za-z]{3}\d{6,})\b", ln)
            if m and m.group(1).upper().startswith(("OSI", "OSO", "OS")):
                return m.group(1)
        return None

    def _is_header_line(self, s: str) -> bool:
        s2 = s.upper()
        return self.HEADER_CUE in s2

    def _parse_item_line(self, s: str) -> Optional[Tuple[int, str, int, float, float]]:
        """
        Parse a single product line using right-to-left tokens:
        Returns (ITEM, DESCRIPTION, PACK, COST(UM_P), UNIT(UNIT_P)) or None.
        """
        # Tokenize on whitespace; keep order
        toks = s.split()
        if len(toks) < 10:
            return None

        # Must end with ... UNIT_P UM_P EXT_P PACK
        # Validate last 4 numeric pattern
        try:
            pack = int(toks[-1])
        except Exception:
            return None

        ext_p  = _to_float(toks[-2])
        um_p   = _to_float(toks[-3])
        unit_p = _to_float(toks[-4])
        um     = toks[-5] if len(toks) >= 5 else None

        if any(v is None for v in (ext_p, um_p, unit_p)) or (um is None) or (um.upper() != "PK"):
            return None

        # Two integers before UM are S-QTY and R-QTY; we don't need them but validate presence
        if len(toks) < 9:
            return None
        if not (_is_int(toks[-6]) and _is_int(toks[-7])):
            return None

        # From left: LINE#  [maybe flag T/C]  ITEM  DESCRIPTION ...  RQTY SQTY UM UNIT_P UM_P EXT_P PACK
        # Find ITEM position: after LINE# and optional flag
        # LINE# = toks[0] (int)
        if not _is_int(toks[0]):
            return None

        idx = 1
        # Optional flag like 'T' or 'C'
        if idx < len(toks) and len(toks[idx]) <= 2 and toks[idx].isalpha() and toks[idx].isupper():
            idx += 1

        # ITEM token must be integer (3–6 digits typical)
        if idx >= len(toks) or not _is_int(toks[idx]):
            return None
        try:
            item = int(toks[idx])
        except Exception:
            return None
        idx += 1  # description starts here

        # Description ends right before the RQTY token (which is toks[-7])
        desc_end_exclusive = len(toks) - 7
        if idx >= desc_end_exclusive:
            return None

        description = _clean_spaces(" ".join(toks[idx:desc_end_exclusive]))
        if not description:
            return None

        # Return parsed tuple
        return (item, description, pack, float(um_p), float(unit_p))

    def parse(self, uploaded_file) -> Tuple[pd.DataFrame, Optional[str]]:
        if pdfplumber is None:
            return pd.DataFrame(columns=self.WANT_COLS), None

        try:
            uploaded_file.seek(0)
        except Exception:
            pass

        name = (getattr(uploaded_file, "name", "") or "").lower()
        if not name.endswith(".pdf"):
            return pd.DataFrame(columns=self.WANT_COLS), None

        rows: List[dict] = []
        invoice_no: Optional[str] = None

        try:
            with pdfplumber.open(uploaded_file) as pdf:
                for page in pdf.pages:
                    lines = self._split_lines(page)
                    if invoice_no is None:
                        invoice_no = self._find_invoice_number(lines)

                    # Start capturing only after header cue has appeared at least once on the page
                    started = False
                    for ln in lines:
                        if not started:
                            if self._is_header_line(ln):
                                started = True
                            continue

                        rec = self._parse_item_line(ln)
                        if rec is None:
                            # Ignore non-item noise lines (subtotals, section titles, etc.)
                            continue

                        item, desc, pack, cost, unit = rec
                        rows.append({
                            "ITEM": item,
                            "DESCRIPTION": desc,
                            "PACK": int(pack),
                            "COST": float(cost),   # UM_P
                            "UNIT": float(unit),   # UNIT_P
                        })
        except Exception:
            # In case of any pdfplumber issue, return empty but valid shape
            return pd.DataFrame(columns=self.WANT_COLS), None

        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS), invoice_no

        df = pd.DataFrame(rows, columns=self.WANT_COLS)

        # Hard de-dup by signature, keeping the first occurrence (preserves order)
        sig = (
            df["ITEM"].astype(str).str.zfill(6) + "||" +
            df["DESCRIPTION"].astype(str).str.strip() + "||" +
            df["PACK"].astype(str) + "||" +
            df["COST"].astype(str)
        )
        keep_mask = ~sig.duplicated(keep="first")
        df = df.loc[keep_mask].reset_index(drop=True)

        return df, invoice_no


# Factory expected by app: export a callable/class as JC_SALES_PARSER
JC_SALES_PARSER = JCSalesParser()

# If your app imports via __init__.py, you may need to expose a name there like:
# from .jcsales import JC_SALES_PARSER
