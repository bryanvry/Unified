# parsers/jcsales.py
# Robust JC Sales invoice parser (PDF)
# Output to app: returns (DataFrame, invoice_no)
# DataFrame columns: ITEM, DESCRIPTION, PACK, COST, UNIT

from __future__ import annotations
from typing import List, Tuple, Optional
import re
import itertools
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None


class JCSalesParser:
    WANT_COLS = ["ITEM", "DESCRIPTION", "PACK", "COST", "UNIT"]

    HEADER_CUE = "LINE # ITEM DESCRIPTION"
    FOOTER_CUES = ("Customer Copy", "Printed.", "times printed", "Page ")

    # ---------------- helpers ----------------
    @staticmethod
    def _is_int(s: str) -> bool:
        return bool(re.fullmatch(r"\d+", str(s).strip()))

    @staticmethod
    def _to_float(tok: str) -> Optional[float]:
        s = str(tok).replace(",", "").strip()
        try:
            return float(s)
        except Exception:
            return None

    @staticmethod
    def _clean_spaces(s: str) -> str:
        return re.sub(r"\s{2,}", " ", str(s or "").strip())

    def _lines_via_text(self, page) -> List[str]:
        """Grab lines using several tolerance combos."""
        out = []
        for xt, yt in [(1, 3), (2, 4), (3, 6)]:
            txt = page.extract_text(x_tolerance=xt, y_tolerance=yt) or ""
            for ln in txt.splitlines():
                s = ln.strip()
                if not s:
                    continue
                if any(cue in s for cue in self.FOOTER_CUES):
                    continue
                if s.startswith("JCSALES ") and "Customer Copy" in s:
                    continue
                out.append(s)
        # keep original order, drop dup consecutive variants
        uniq = []
        for s in out:
            if not uniq or uniq[-1] != s:
                uniq.append(s)
        return uniq

    def _lines_via_words(self, page) -> List[str]:
        """Fallback: reconstruct lines from words grid."""
        words = page.extract_words(keep_blank_chars=False, use_text_flow=True) or []
        if not words:
            return []
        # bucket by approx y
        rows = {}
        for w in words:
            key = round(float(w["top"]) / 2.5, 1)
            rows.setdefault(key, []).append(w)
        lines = []
        for key in sorted(rows.keys()):
            ws = sorted(rows[key], key=lambda x: x["x0"])
            s = " ".join(w["text"] for w in ws).strip()
            if not s:
                continue
            if any(cue in s for cue in self.FOOTER_CUES):
                continue
            if s.startswith("JCSALES ") and "Customer Copy" in s:
                continue
            lines.append(s)
        return lines

    def _gather_all_lines(self, pdf) -> List[str]:
        all_lines = []
        for p in pdf.pages:
            via_text = self._lines_via_text(p)
            via_words = self._lines_via_words(p)
            merged = list(dict.fromkeys(via_text + via_words))  # preserve order, drop dup
            all_lines.extend(merged)
        return all_lines

    def _find_invoice_number(self, all_lines: List[str]) -> Optional[str]:
        for ln in all_lines:
            m = re.search(r"\*([A-Za-z]{3}\d{6,})\*", ln)
            if m:
                return m.group(1)
        for ln in all_lines:
            if "JCSALES" in ln:
                m = re.search(r"\b([A-Za-z]{3}\d{6,})\b", ln)
                if m:
                    return m.group(1)
        return None

    def _is_header_line(self, s: str) -> bool:
        return self.HEADER_CUE in s.upper()

    # ---------- line parsers ----------
    def _parse_tail_rtl(self, s: str) -> Optional[Tuple[int, int, str, int, float, float]]:
        """
        Right-to-left parser for the fixed tail:
        ... RQTY SQTY UM UNIT_P UM_P EXT_P PACK
        Returns (line_no, item, desc, pack, cost(UM_P), unit(UNIT_P))
        """
        toks = s.split()
        if len(toks) < 10:
            return None

        # LINE#
        if not self._is_int(toks[0]):
            return None
        try:
            line_no = int(toks[0])
        except Exception:
            return None

        # tail
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

        if len(toks) < 9 or not self._is_int(toks[-6]) or not self._is_int(toks[-7]):
            return None

        i = 1
        # optional single-letter flag (e.g., T/C)
        if i < len(toks) and len(toks[i]) <= 2 and toks[i].isalpha() and toks[i].isupper():
            i += 1
        if i >= len(toks) or not self._is_int(toks[i]):
            return None
        try:
            item = int(toks[i])
        except Exception:
            return None
        i += 1

        desc_end = len(toks) - 7
        if i >= desc_end:
            return None
        desc = self._clean_spaces(" ".join(toks[i:desc_end]))
        if not desc:
            return None

        return (line_no, item, desc, pack, float(um_p), float(unit_p))

    def _parse_regex(self, s: str) -> Optional[Tuple[int, int, str, int, float, float]]:
        """
        Regex fallback; allows commas in prices and optional flag.
        Captures:
          LINE, [FLAG], ITEM, DESC, RQTY, SQTY, UM, UNIT_P, UM_P, EXT_P, PACK
        """
        pat = re.compile(
            r"""^\s*
                 (?P<line>\d+)\s+
                 (?:(?P<flag>[A-Z])\s+)?        # optional single-letter flag
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
        desc = self._clean_spaces(m.group("desc"))
        pack = int(m.group("pack"))
        unit = self._to_float(m.group("unit"))
        ump  = self._to_float(m.group("ump"))
        if None in (unit, ump):
            return None
        return (line_no, item, desc, pack, float(ump), float(unit))

    def _parse_one_line(self, s: str) -> Optional[Tuple[int, int, str, int, float, float]]:
        # Try strict tail first, then regex
        parsed = self._parse_tail_rtl(s)
        if parsed:
            return parsed
        return self._parse_regex(s)

    # ---------------- public ----------------
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
                all_lines = self._gather_all_lines(pdf)
                invoice_no = self._find_invoice_number(all_lines)

                # One global start toggle: begin AFTER we see the header once
                started = False
                for ln in all_lines:
                    if not started:
                        if self._is_header_line(ln):
                            started = True
                        continue

                    parsed = self._parse_one_line(ln)
                    if not parsed:
                        continue

                    line_no, item, desc, pack, cost, unit = parsed
                    if line_no in seen_lines:
                        continue
                    seen_lines.add(line_no)

                    rows.append(
                        {
                            "LINE": line_no,
                            "ITEM": item,
                            "DESCRIPTION": desc,
                            "PACK": pack,
                            "COST": cost,  # UM_P (case-pack price)
                            "UNIT": unit,  # UNIT_P
                        }
                    )
        except Exception:
            return pd.DataFrame(columns=self.WANT_COLS), None

        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS), invoice_no

        df = pd.DataFrame(rows, columns=["LINE"] + self.WANT_COLS)

        # Secondary hard de-dupe by signature in case the same visual line reconstructed twice
        sig = (
            df["LINE"].astype(str)
            + "||" + df["ITEM"].astype(str)
            + "||" + df["DESCRIPTION"].astype(str).str.strip()
            + "||" + df["PACK"].astype(str)
            + "||" + df["COST"].astype(str)
            + "||" + df["UNIT"].astype(str)
        )
        df = df.loc[~sig.duplicated(keep="first")].copy()

        df.sort_values("LINE", kind="stable", inplace=True)
        df.drop(columns=["LINE"], inplace=True)
        df.reset_index(drop=True, inplace=True)

        return df[self.WANT_COLS], invoice_no


# what app.py imports
JC_SALES_PARSER = JCSalesParser()
