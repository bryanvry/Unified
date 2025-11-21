# parsers/jcsales.py
# JC Sales invoice parser (PDF text-line parser)
# - Starts parsing once after the column header appears (global flag)
# - Parses each product line right-to-left using the fixed tail:
#     RQTY SQTY UM UNIT_P UM_P EXT_P PACK
# - De-dupes by LINE # (keep first seen)
# - Returns (DataFrame, invoice_no)
#
# Output columns expected by app: ITEM, DESCRIPTION, PACK, COST, UNIT

from __future__ import annotations
from typing import List, Tuple, Optional
import re
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None


class JCSalesParser:
    WANT_COLS = ["ITEM", "DESCRIPTION", "PACK", "COST", "UNIT"]

    # Header cue that precedes the grid
    HEADER_CUE = "LINE # ITEM DESCRIPTION"
    # Footer noise to drop
    FOOTER_CUES = ("Customer Copy", "Printed.", "times printed", "Page ")

    # ---------- helpers ----------
    @staticmethod
    def _is_int(tok: str) -> bool:
        return bool(re.fullmatch(r"\d+", str(tok).strip()))

    @staticmethod
    def _to_float(tok: str) -> Optional[float]:
        s = str(tok).replace(",", "").strip()
        try:
            return float(s)
        except Exception:
            return None

    @staticmethod
    def _clean_spaces(s: str) -> str:
        return re.sub(r"\s{2,}", " ", str(s).strip())

    def _split_lines(self, page) -> List[str]:
        # tolerate tight spacing
        txt = page.extract_text(x_tolerance=1, y_tolerance=3) or ""
        raw = [ln.rstrip() for ln in txt.splitlines()]
        out = []
        for s in raw:
            s = s.strip()
            if not s:
                continue
            if any(cue in s for cue in self.FOOTER_CUES):
                continue
            if s.startswith("JCSALES ") and "Customer Copy" in s:
                continue
            out.append(s)
        return out

    def _find_invoice_number(self, all_lines: List[str]) -> Optional[str]:
        # Look for *OSI014135*-style tag first
        for ln in all_lines:
            m = re.search(r"\*([A-Za-z]{3}\d{6,})\*", ln)
            if m:
                return m.group(1)
        # Backup: a token like OSI014135 near "JCSALES"
        for ln in all_lines:
            if "JCSALES" in ln:
                m = re.search(r"\b([A-Za-z]{3}\d{6,})\b", ln)
                if m:
                    return m.group(1)
        return None

    def _is_header_line(self, s: str) -> bool:
        return self.HEADER_CUE in s.upper()

    def _parse_item_line(self, s: str) -> Optional[Tuple[int, int, str, int, float, float]]:
        """
        Parse one product row.

        Expected right tail:
            ... RQTY SQTY UM UNIT_P UM_P EXT_P PACK
        where:
            RQTY, SQTY, PACK are ints
            UM is 'PK'
            UNIT_P, UM_P, EXT_P are floats

        Returns (line_no, item, description, pack, cost(UM_P), unit(UNIT_P)) or None.
        """
        toks = s.split()
        if len(toks) < 10:
            return None

        # 1) LINE #
        if not self._is_int(toks[0]):
            return None
        try:
            line_no = int(toks[0])
        except Exception:
            return None

        # 2) Validate the fixed tail (right-to-left)
        try:
            pack = int(toks[-1])
        except Exception:
            return None

        ext_p  = self._to_float(toks[-2])
        um_p   = self._to_float(toks[-3])
        unit_p = self._to_float(toks[-4])
        um     = toks[-5] if len(toks) >= 5 else None
        if any(v is None for v in (ext_p, um_p, unit_p)) or um is None or um.upper() != "PK":
            return None

        # two integer quantities before UM
        if len(toks) < 9 or not self._is_int(toks[-6]) or not self._is_int(toks[-7]):
            return None

        # 3) From the left: LINE# [optional flag] ITEM DESCRIPTION ... tail
        i = 1
        # optional single-letter flag like "T" or "C"
        if i < len(toks) and len(toks[i]) <= 2 and toks[i].isalpha() and toks[i].isupper():
            i += 1

        if i >= len(toks) or not self._is_int(toks[i]):
            return None
        try:
            item = int(toks[i])
        except Exception:
            return None
        i += 1

        desc_end = len(toks) - 7  # stop before RQTY SQTY UM UNIT_P UM_P EXT_P PACK
        if i >= desc_end:
            return None
        desc = self._clean_spaces(" ".join(toks[i:desc_end]))
        if not desc:
            return None

        return (line_no, item, desc, pack, float(um_p), float(unit_p))

    # ---------- public ----------
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
        seen_lines: set[int] = set()
        invoice_no: Optional[str] = None

        try:
            with pdfplumber.open(uploaded_file) as pdf:
                # Gather all lines once for invoice number detection
                all_lines: List[str] = []
                for p in pdf.pages:
                    all_lines.extend(self._split_lines(p))
                invoice_no = self._find_invoice_number(all_lines)

                # Single global start toggle: begin AFTER we encounter the header once
                started = False
                for page in pdf.pages:
                    for ln in self._split_lines(page):
                        if not started:
                            if self._is_header_line(ln):
                                started = True
                            continue

                        parsed = self._parse_item_line(ln)
                        if not parsed:
                            continue

                        line_no, item, desc, pack, cost, unit = parsed
                        if line_no in seen_lines:
                            continue  # hard de-dupe
                        seen_lines.add(line_no)

                        rows.append(
                            {
                                "LINE": line_no,
                                "ITEM": item,
                                "DESCRIPTION": desc,
                                "PACK": pack,
                                "COST": cost,  # UM_P
                                "UNIT": unit,  # UNIT_P
                            }
                        )
        except Exception:
            return pd.DataFrame(columns=self.WANT_COLS), None

        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS), invoice_no

        df = pd.DataFrame(rows, columns=["LINE"] + self.WANT_COLS)
        # Safety de-dupe by a signature, just in case
        sig = (
            df["LINE"].astype(str)
            + "||"
            + df["ITEM"].astype(str)
            + "||"
            + df["DESCRIPTION"].astype(str).str.strip()
            + "||"
            + df["PACK"].astype(str)
            + "||"
            + df["COST"].astype(str)
            + "||"
            + df["UNIT"].astype(str)
        )
        df = df.loc[~sig.duplicated(keep="first")].copy()

        df.sort_values("LINE", kind="stable", inplace=True)
        df.drop(columns=["LINE"], inplace=True)
        df.reset_index(drop=True, inplace=True)

        return df[self.WANT_COLS], invoice_no


# what app.py imports
JC_SALES_PARSER = JCSalesParser()
