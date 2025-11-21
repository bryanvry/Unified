# parsers/jcsales.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Tuple
import re
import math
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None


@dataclass
class JCLine:
    line_no: int
    item: str
    desc: str
    r_qty: int
    s_qty: int
    um: str
    pack: int
    unit_p: float   # UNIT_P (unit each)
    um_p: float     # UM_P   (case price) -> COST
    ext_p: float


class JCSalesParser:
    """
    Output columns expected by app.py post-processing:
      ITEM, DESCRIPTION, PACK, COST, UNIT, RETAIL
    (UPC, NOW, DELTA are added later by the app using Master + Pricebook.)
    """
    WANT_COLS = ["ITEM", "DESCRIPTION", "PACK", "COST", "UNIT", "RETAIL"]

    # ------------ public API ------------
    def parse(self, uploaded_pdf) -> Tuple[pd.DataFrame, str]:
        if pdfplumber is None:
            return pd.DataFrame(columns=self.WANT_COLS), ""

        try:
            uploaded_pdf.seek(0)
        except Exception:
            pass

        try:
            with pdfplumber.open(uploaded_pdf) as pdf:
                raw_text = "\n".join([p.extract_text() or "" for p in pdf.pages])
                inv_no = self._extract_invoice_no(raw_text)

                # Build lines from words (more robust than extract_text on this layout)
                all_lines = []
                for page in pdf.pages:
                    words = page.extract_words(
                        keep_blank_chars=False,
                        use_text_flow=True,
                        extra_attrs=["x0","x1","top","bottom","text"]
                    ) or []

                    lines = self._group_words_into_lines(words)
                    all_lines.extend(lines)

                # Parse each candidate line
                parsed: List[JCLine] = []
                for line in all_lines:
                    rec = self._parse_right_anchored(line)
                    if rec:
                        parsed.append(rec)

                if not parsed:
                    # Final fallback: try the simpler extract_text() lines with the same parser
                    text_lines = []
                    for page in pdf.pages:
                        t = page.extract_text() or ""
                        text_lines.extend(l for l in (ln.strip() for ln in t.splitlines()) if l)
                    for l in text_lines:
                        rec = self._parse_right_anchored(l)
                        if rec:
                            parsed.append(rec)

                if not parsed:
                    return pd.DataFrame(columns=self.WANT_COLS), (inv_no or "UNKNOWN")

                parsed.sort(key=lambda r: r.line_no)
                df = pd.DataFrame({
                    "ITEM": [r.item for r in parsed],
                    "DESCRIPTION": [r.desc for r in parsed],
                    "PACK": [r.pack for r in parsed],
                    "COST": [r.um_p for r in parsed],   # COST uses UM_P (case price)
                    # extras (not returned): r.r_qty, r.s_qty, r.unit_p, r.ext_p, r.um
                })

                # Compute UNIT and RETAIL
                df["PACK"] = pd.to_numeric(df["PACK"], errors="coerce").fillna(0).astype(int)
                df["COST"] = pd.to_numeric(df["COST"], errors="coerce")
                with pd.option_context("mode.use_inf_as_na", True):
                    df["UNIT"] = (df["COST"] / df["PACK"].replace(0, pd.NA)).round(2)
                df["RETAIL"] = (df["UNIT"] * 2).round(2)

                return df[self.WANT_COLS], (inv_no or "UNKNOWN")
        except Exception:
            return pd.DataFrame(columns=self.WANT_COLS), ""

    # ------------ helpers ------------
    def _extract_invoice_no(self, text: str) -> Optional[str]:
        if not text:
            return None
        # Matches OSI014135 / OSI14135 etc. (your sample: OSI014135)
        m = re.search(r"\bOSI0?\d{5}\b", text)
        return m.group(0) if m else None

    def _group_words_into_lines(self, words: List[dict]) -> List[str]:
        """
        Cluster words by y (top) into lines, then join by x order.
        We use a small tolerance to handle tiny baseline drift.
        """
        if not words:
            return []

        # Sort by top then x
        words = sorted(words, key=lambda w: (w["top"], w["x0"]))
        lines: List[List[dict]] = []
        tol = self._y_tolerance(words)

        for w in words:
            if not lines:
                lines.append([w])
                continue
            last_line = lines[-1]
            # If this word is roughly on the same baseline as the last line, append
            if abs(w["top"] - last_line[0]["top"]) <= tol:
                last_line.append(w)
            else:
                lines.append([w])

        out_lines: List[str] = []
        for line_words in lines:
            # Join by x order and single spaces
            line_words = sorted(line_words, key=lambda ww: ww["x0"])
            text = " ".join(ww["text"] for ww in line_words if ww.get("text"))
            # Skip obvious headers/footers
            if not text.strip():
                continue
            if text.startswith("LINE # ITEM") or "Customer Copy" in text:
                continue
            if "Printed." in text and "Page" in text:
                continue
            out_lines.append(text.strip())
        return out_lines

    def _y_tolerance(self, words: List[dict]) -> float:
        # Estimate a reasonable vertical tolerance based on median line height
        heights = []
        for w in words:
            try:
                heights.append(abs(float(w["bottom"]) - float(w["top"])))
            except Exception:
                pass
        if not heights:
            return 2.0
        med = sorted(heights)[len(heights)//2]
        return max(1.5, min(4.0, med * 0.4))

    def _parse_right_anchored(self, line: str) -> Optional[JCLine]:
        """
        Parse using a *right-anchored* tail for the 7 numeric-ish tokens:
           R-QTY S-QTY UM UNIT_P UM_P EXT_P PACK
        Then parse the head for:
           LINE# [optional T/C] ITEM DESCRIPTION...
        """
        tail = re.compile(
            r"""
            \s(?P<rqty>\d+)
            \s+(?P<sqty>\d+)
            \s+(?P<um>[A-Z]{1,3})
            \s+(?P<unit_p>\d+\.\d{2})
            \s+(?P<um_p>\d+\.\d{2})
            \s+(?P<ext_p>\d+\.\d{2})
            \s+(?P<pack>\d+)\s*$
            """,
            re.VERBOSE,
        )
        m_tail = tail.search(line)
        if not m_tail:
            return None

        head = line[: m_tail.start()].strip()

        # Head pattern: LINE# [T|C]? ITEM DESCRIPTION...
        head_re = re.compile(
            r"""
            ^\s*
            (?P<lineno>\d+)
            (?:\s+[TC])?
            \s+(?P<item>\d+)
            \s+(?P<desc>.+?)\s*$
            """,
            re.VERBOSE,
        )
        m_head = head_re.match(head)
        if not m_head:
            return None

        try:
            return JCLine(
                line_no=int(m_head.group("lineno")),
                item=m_head.group("item"),
                desc=m_head.group("desc").strip(),
                r_qty=int(m_tail.group("rqty")),
                s_qty=int(m_tail.group("sqty")),
                um=m_tail.group("um"),
                pack=int(m_tail.group("pack")),
                unit_p=float(m_tail.group("unit_p")),
                um_p=float(m_tail.group("um_p")),
                ext_p=float(m_tail.group("ext_p")),
            )
        except Exception:
            return None
