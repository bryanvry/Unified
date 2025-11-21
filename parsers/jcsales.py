# parsers/jcsales.py
# Robust JC Sales invoice parser (PDF only)
# Returns: (DataFrame, invoice_no)
# DataFrame columns: ITEM, DESCRIPTION, PACK, COST, UNIT
#
# Parsing logic:
#   Right-to-left fixed tail per line:
#       ... RQTY SQTY UM UNIT_P UM_P EXT_P PACK
#   where UM == "PK", PACK is an int, and prices are decimals.
#   Optional single-letter flag (e.g., "T" or "C") may appear after LINE#.
#   No dependency on detecting the header — we parse any line matching shape.

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
    FOOTER_CUES = ("Customer Copy", "Printed.", "times printed", "Page ")

    # ---------- helpers ----------
    @staticmethod
    def _is_int_tok(tok: str) -> bool:
        return bool(re.fullmatch(r"\d+", str(tok).strip()))

    @staticmethod
    def _to_float(tok: str) -> Optional[float]:
        s = str(tok).replace(",", "").strip()
        try:
            return float(s)
        except Exception:
            return None

    @staticmethod
    def _clean(s: str) -> str:
        return re.sub(r"\s{2,}", " ", str(s or "").strip())

    def _page_lines_text(self, page) -> List[str]:
        # Try a few tolerances to be robust to spacing/kerning
        lines = []
        for xt, yt in [(1, 3), (2, 4), (3, 6)]:
            txt = page.extract_text(x_tolerance=xt, y_tolerance=yt) or ""
            for ln in txt.splitlines():
                s = ln.strip()
                if not s:
                    continue
                if any(cue in s for cue in self.FOOTER_CUES):
                    continue
                lines.append(s)
        # de-dup consecutive
        out = []
        for s in lines:
            if not out or out[-1] != s:
                out.append(s)
        return out

    def _page_lines_words(self, page) -> List[str]:
        words = page.extract_words(keep_blank_chars=False, use_text_flow=True) or []
        if not words:
            return []
        rows = {}
        for w in words:
            key = round(float(w["top"]) / 2.5, 1)
            rows.setdefault(key, []).append(w)
        lines = []
        for k in sorted(rows.keys()):
            ws = sorted(rows[k], key=lambda x: x["x0"])
            s = " ".join(w["text"] for w in ws).strip()
            if not s:
                continue
            if any(cue in s for cue in self.FOOTER_CUES):
                continue
            lines.append(s)
        return lines

    def _gather_lines(self, pdf) -> List[str]:
        all_lines = []
        for p in pdf.pages:
            a = self._page_lines_text(p)
            b = self._page_lines_words(p)
            # preserve order, drop duplicates
            merged = list(dict.fromkeys(a + b))
            all_lines.extend(merged)
        return all_lines

    def _find_invoice_no(self, lines: List[str]) -> Optional[str]:
        # e.g., "*OSI014135*" or standalone token like OSI014135
        for ln in lines:
            m = re.search(r"\*([A-Za-z]{3}\d{6,})\*", ln)
            if m:
                return m.group(1)
        for ln in lines:
            m = re.search(r"\b([A-Za-z]{3}\d{6,})\b", ln)
            if m:
                return m.group(1)
        return None

    # ---------- core line parser ----------
    def _parse_line_rtl(self, s: str) -> Optional[Tuple[int, int, str, int, float, float]]:
        """
        Parse one invoice row by scanning tokens from the right:
          ... RQTY SQTY UM UNIT_P UM_P EXT_P PACK
        Returns: (line_no, item, description, pack, cost(UM_P), unit(UNIT_P))
        """
        toks = s.split()
        if len(toks) < 10:
            return None

        # Right tail
        try:
            pack = int(toks[-1])
        except Exception:
            return None

        ext_p  = self._to_float(toks[-2])
        um_p   = self._to_float(toks[-3])
        unit_p = self._to_float(toks[-4])
        um     = toks[-5] if len(toks) >= 5 else None
        if None in (ext_p, um_p, unit_p) or um is None:
            return None
        if um.upper() != "PK":
            return None

        # S-QTY / R-QTY immediately before UM
        if not (self._is_int_tok(toks[-6]) and self._is_int_tok(toks[-7])):
            return None

        # From left: LINE#, optional FLAG, ITEM, then DESCRIPTION ... until R-QTY
        i = 0
        if not self._is_int_tok(toks[i]):
            return None
        line_no = int(toks[i]); i += 1

        # Optional single-letter flag (T/C/etc.)
        if i < len(toks) and re.fullmatch(r"[A-Z]{1,2}", toks[i]):
            i += 1

        if i >= len(toks) or not self._is_int_tok(toks[i]):
            return None
        item = int(toks[i]); i += 1

        desc_end = len(toks) - 7  # up to the token before R-QTY
        if i >= desc_end:
            return None

        desc = self._clean(" ".join(toks[i:desc_end]))
        if not desc:
            return None

        return (line_no, item, desc, pack, float(um_p), float(unit_p))

    def _parse_line_regex(self, s: str) -> Optional[Tuple[int, int, str, int, float, float]]:
        """
        Regex fallback allowing commas, optional flag, and noisy desc.
        """
        pat = re.compile(
            r"""^\s*
                 (?P<line>\d+)\s+
                 (?:(?P<flag>[A-Z]{1,2})\s+)?     # optional flag
                 (?P<item>\d+)\s+
                 (?P<desc>.+?)\s+
                 (?P<rqty>\d+)\s+
                 (?P<sqty>\d+)\s+
                 (?P<um>[A-Za-z]+)\s+
                 (?P<unit>[\d,]+\.\d{2})\s+
                 (?P<ump>[\d,]+\.\d{2})\s+
                 (?P<ext>[\d,]+\.\d{2})\s+
                 (?P<pack>\d+)\s*$
            """,
            re.VERBOSE
        )
        m = pat.match(s)
        if not m:
            return None
        if m.group("um").upper() != "PK":
            return None
        line_no = int(m.group("line"))
        item = int(m.group("item"))
        desc = self._clean(m.group("desc"))
        unit = self._to_float(m.group("unit"))
        ump  = self._to_float(m.group("ump"))
        pack = int(m.group("pack"))
        if None in (unit, ump):
            return None
        return (line_no, item, desc, pack, float(ump), float(unit))

    def _parse_any_line(self, s: str) -> Optional[Tuple[int, int, str, int, float, float]]:
        # primary: strict tail; fallback: regex
        out = self._parse_line_rtl(s)
        if out:
            return out
        return self._parse_line_regex(s)

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

        rows = []
        seen = set()
        invoice_no = None

        try:
            with pdfplumber.open(uploaded_file) as pdf:
                lines = self._gather_lines(pdf)
                invoice_no = self._find_invoice_no(lines)

                # Parse ANY line that matches; don't require seeing the header
                for ln in lines:
                    parsed = self._parse_any_line(ln)
                    if not parsed:
                        continue
                    line_no, item, desc, pack, cost, unit = parsed
                    if line_no in seen:
                        continue
                    seen.add(line_no)
                    rows.append({
                        "LINE": line_no,
                        "ITEM": item,
                        "DESCRIPTION": desc,
                        "PACK": pack,
                        "COST": cost,   # case-pack price (UM_P)
                        "UNIT": unit,   # unit price (UNIT_P)
                    })
        except Exception:
            return pd.DataFrame(columns=self.WANT_COLS), None

        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS), invoice_no

        df = pd.DataFrame(rows, columns=["LINE"] + self.WANT_COLS)

        # Stable sort & hard de-dup signature
        sig = (
            df["LINE"].astype(str) + "||" +
            df["ITEM"].astype(str) + "||" +
            df["DESCRIPTION"].astype(str).str.strip() + "||" +
            df["PACK"].astype(str) + "||" +
            df["COST"].astype(str) + "||" +
            df["UNIT"].astype(str)
        )
        df = df.loc[~sig.duplicated(keep="first")].copy()
        df.sort_values("LINE", kind="stable", inplace=True)
        df.drop(columns=["LINE"], inplace=True)
        df.reset_index(drop=True, inplace=True)

        return df[self.WANT_COLS], invoice_no


# what app.py imports
JC_SALES_PARSER = JCSalesParser()
