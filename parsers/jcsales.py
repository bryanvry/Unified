# parsers/jcsales.py
# JC Sales invoice parser (PDF → robust text-only parser, no table fallbacks)
# Fixes 223/157 by:
#  • Starting capture once (global), not per-page
#  • Hard de-duping on the leading LINE # token (unique per invoice)
#  • Additional signature de-dupe as a safety net

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
    """
    Parse JC Sales invoice PDFs by line, preserving order and preventing duplicates.
    Output columns: ITEM, DESCRIPTION, PACK, COST(UM_P), UNIT(UNIT_P)
    Returns: (DataFrame, invoice_number)
    """

    WANT_COLS = ["ITEM", "DESCRIPTION", "PACK", "COST", "UNIT"]

    HEADER_CUE = "LINE # ITEM DESCRIPTION"
    FOOTER_CUES = (
        "Customer Copy",
        "Printed.",
        "times printed",
        "Page ",
    )

    def _split_lines(self, page) -> List[str]:
        txt = page.extract_text(x_tolerance=1, y_tolerance=3) or ""
        raw_lines = [ln.rstrip() for ln in txt.splitlines()]
        lines = []
        for ln in raw_lines:
            s = ln.strip()
            if not s:
                continue
            # Drop obvious footers and page banners
            if any(cue in s for cue in self.FOOTER_CUES):
                continue
            if s.startswith("JCSALES ") and "Customer Copy" in s:
                continue
            lines.append(s)
        return lines

    def _find_invoice_number(self, lines: List[str]) -> Optional[str]:
        # e.g. "*OSI014135*"
        for ln in lines:
            m = re.search(r"\*([A-Za-z]{3}\d{6,})\*", ln)
            if m:
                return m.group(1)
        # backup: "JCSALES OSI014135 ..."
        for ln in lines:
            m = re.search(r"\b([A-Za-z]{3}\d{6,})\b", ln)
            if m and m.group(1).upper().startswith(("OSI", "OSO", "OS")):
                return m.group(1)
        return None

    def _is_header_line(self, s: str) -> bool:
        return self.HEADER_CUE in s.upper()

    def _parse_item_line(self, s: str) -> Optional[Tuple[int, int, str, int, float, float]]:
        """
        Parse a product line using right-to-left tokens.
        Returns (line_no, ITEM, DESCRIPTION, PACK, COST(UM_P), UNIT(UNIT_P)) or None.

        Expected tail:
          ... RQTY SQTY UM UNIT_P UM_P EXT_P PACK
        """
        toks = s.split()
        if len(toks) < 10:
            return None

        # Leading LINE #
        if not _is_int(toks[0]):
            return None
        try:
            line_no = int(toks[0])
        except Exception:
            return None

        # Validate and read tail fields
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

        if len(toks) < 9 or (not _is_int(toks[-6])) or (not _is_int(toks[-7])):
            return None

        # From left: LINE# [optional flag] ITEM DESCRIPTION ... RQTY SQTY UM UNIT_P UM_P EXT_P PACK
        idx = 1
        if idx < len(toks) and len(toks[idx]) <= 2 and toks[idx].isalpha() and toks[idx].isupper():
            idx += 1

        if idx >= len(toks) or not _is_int(toks[idx]):
            return None
        try:
            item = int(toks[idx])
        except Exception:
            return None
        idx += 1

        desc_end_exclusive = len(toks) - 7
        if idx >= desc_end_exclusive:
            return None

        description = _clean_spaces(" ".join(toks[idx:desc_end_exclusive]))
        if not description:
            return None

        return (line_no, item, description, pack, float(um_p), float(unit_p))

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
        seen_line_nos: set[int] = set()

        try:
            with pdfplumber.open(uploaded_file) as pdf:
                # Detect invoice number once
                for page in pdf.pages:
                    lines = self._split_lines(page)
                    inv = self._find_invoice_number(lines)
                    if inv:
                        invoice_no = inv
                        break

                # Global capture: once we've seen the header ANYWHERE, parse all subsequent lines across pages
                started = False
                for page in pdf.pages:
                    lines = self._split_lines(page)
                    for ln in lines:
                        if not started:
                            if self._is_header_line(ln):
                                started = True
                            continue

                        parsed = self._parse_item_line(ln)
                        if parsed is None:
                            continue

                        line_no, item, desc, pack, cost, unit = parsed

                        # De-dupe by LINE # — keep first occurrence across the whole document
                        if line_no in seen_line_nos:
                            continue
                        seen_line_nos.add(line_no)

                        rows.append({
                            "LINE": int(line_no),
                            "ITEM": int(item),
                            "DESCRIPTION": desc,
                            "PACK": int(pack),
                            "COST": float(cost),   # UM_P
                            "UNIT": float(unit),   # UNIT_P
                        })
        except Exception:
            return pd.DataFrame(columns=self.WANT_COLS), None

        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS), invoice_no

        df = pd.DataFrame(rows, columns=["LINE"] + self.WANT_COLS)

        # Secondary safety de-dupe by a stable signature (if PDF text duplicated oddly)
        sig = (
            df["LINE"].astype(str) + "||" +
            df["ITEM"].astype(str) + "||" +
            df["DESCRIPTION"].astype(str).str.strip() + "||" +
            df["PACK"].astype(str) + "||" +
            df["COST"].astype(str) + "||" +
            df["UNIT"].astype(str)
        )
        keep_mask = ~sig.duplicated(keep="first")
        df = df.loc[keep_mask].copy()

        # Sort by LINE to preserve invoice order, then drop LINE
        df.sort_values("LINE", kind="stable", inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.drop(columns=["LINE"], inplace=True)

        return df[self.WANT_COLS], invoice_no


# Export instance expected by app
JC_SALES_PARSER = JCSalesParser()
