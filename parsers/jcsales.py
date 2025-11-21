# parsers/jcsales.py
# Robust JC Sales PDF parser that:
#  • extracts invoice number (e.g., OSI014135)
#  • reads each text line and parses from the RIGHT edge
#    tail = [PACK(int), EXT_P($), UM_P($), UNIT_P($), 'PK', S_QTY(int), R_QTY(int)]
#  • everything left of R_QTY is DESCRIPTION (with ITEM just before it)
#  • handles optional flag after LINE ("T", "C", etc.) and squeezed spaces (e.g. "OZ1 1 PK")
#
# Returns: (rows_df, invoice_no)
# rows_df columns (stable names for app): 
#   ["LINE","ITEM","DESCRIPTION","R_QTY","S_QTY","UM","PACK","UNIT_P","UM_P","EXT_P"]

from __future__ import annotations
import re
import numpy as np
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None


class JCSalesParser:
    WANT_COLS = ["LINE","ITEM","DESCRIPTION","R_QTY","S_QTY","UM","PACK","UNIT_P","UM_P","EXT_P"]

    # ---------- utils ----------
    @staticmethod
    def _is_money(tok: str) -> bool:
        return bool(re.fullmatch(r"-?\d+\.\d{2}", tok.replace(",", "")))

    @staticmethod
    def _to_float(tok):
        if tok is None:
            return np.nan
        if isinstance(tok, (int, float, np.number)):
            return float(tok)
        t = str(tok).replace(",", "").strip()
        m = re.search(r"-?\d+(?:\.\d+)?", t)
        return float(m.group(0)) if m else np.nan

    @staticmethod
    def _to_int(tok):
        if tok is None:
            return np.nan
        try:
            return int(str(tok).strip())
        except Exception:
            return np.nan

    @staticmethod
    def _clean_spaces(s: str) -> str:
        # Fix common squeeze like "OZ1 1 PK" → "OZ 1 1 PK"
        # general rule: ensure spaces between letter→digit and digit→letter boundaries
        if not s:
            return s
        s = re.sub(r"([A-Za-z])(\d)", r"\1 \2", s)
        s = re.sub(r"(\d)([A-Za-z])", r"\1 \2", s)
        # collapse multi-spaces
        s = re.sub(r"\s{2,}", " ", s).strip()
        return s

    # ---------- invoice number ----------
    def _extract_invoice_no(self, page) -> str | None:
        txt = page.extract_text() or ""
        m = re.search(r"OSI\d{6,}", txt)
        if m:
            return m.group(0)
        m2 = re.search(r"\*([A-Z]{3}\d{6,})\*", txt)
        return m2.group(1) if m2 else None

    # ---------- line parser (reverse-tail) ----------
    def _parse_text_line(self, raw: str):
        """
        Robust parse of a single invoice item line.

        Expected head:  LINE [FLAG]? ITEM  DESCRIPTION ...  R_QTY  S_QTY  PK  UNIT_P  UM_P  EXT_P  PACK
        Where tail is parsed from the RIGHT to be resilient to spacing glitches.
        """
        if not raw or len(raw) < 12:
            return None

        line = self._clean_spaces(raw)

        # Skip obvious non-data lines
        if "Customer Copy" in line or "Printed." in line or "Page" in line:
            return None
        if "LINE # ITEM DESCRIPTION" in line:
            return None

        # Tokenize
        toks = line.split()
        if not toks:
            return None

        # Must start with LINE number
        if not re.fullmatch(r"\d{1,3}", toks[0]):
            return None
        line_no = int(toks[0])

        # Optional single-letter flag after LINE
        idx = 1
        flag = None
        if idx < len(toks) and re.fullmatch(r"[A-Z]", toks[idx]):
            flag = toks[idx]
            idx += 1

        # Next must be ITEM (digits; can include leading zeros, length 3–6+)
        if idx >= len(toks) or not re.fullmatch(r"\d{3,}", toks[idx]):
            # Some lines might merge FLAG with ITEM (rare); give up
            return None
        item_tok = toks[idx]
        idx += 1

        head_tokens = toks[:idx]            # LINE (and optional flag) + ITEM
        mid_tokens  = toks[idx:]            # DESCRIPTION + numeric tail

        if len(mid_tokens) < 7:
            return None  # not enough tokens to hold tail

        # Walk from the right to pull the tail fields
        rtoks = mid_tokens[::-1]  # reversed
        # PACK (int)
        pack_tok = None
        while rtoks and not re.fullmatch(r"\d+", rtoks[0]):
            # sometimes a stray character or space—try to salvage
            rtoks.pop(0)
        if not rtoks:
            return None
        pack_tok = rtoks.pop(0)

        # EXT_P ($)
        if not rtoks:
            return None
        ext_tok = rtoks.pop(0)
        if not self._is_money(ext_tok):
            return None

        # UM_P ($)
        if not rtoks:
            return None
        um_p_tok = rtoks.pop(0)
        if not self._is_money(um_p_tok):
            return None

        # UNIT_P ($)
        if not rtoks:
            return None
        unit_p_tok = rtoks.pop(0)
        if not self._is_money(unit_p_tok):
            return None

        # 'PK' literal (can be merged nearby; we already cleaned, so look for exact 'PK')
        um_tok = None
        if rtoks and rtoks[0] == "PK":
            um_tok = rtoks.pop(0)
        else:
            # If absent, still assume UM=PK (JC uses PK)
            um_tok = "PK"

        # S_QTY (int)
        if not rtoks:
            return None
        s_qty_tok = rtoks.pop(0)
        if not re.fullmatch(r"\d+", s_qty_tok):
            return None

        # R_QTY (int)
        if not rtoks:
            return None
        r_qty_tok = rtoks.pop(0)
        if not re.fullmatch(r"\d+", r_qty_tok):
            return None

        # Remaining (reversed) tokens form DESCRIPTION, but currently reversed.
        desc_tokens = rtoks[::-1]  # put back in forward order

        # Build fields
        item = item_tok
        desc = " ".join(desc_tokens).strip()
        r_qty = self._to_int(r_qty_tok)
        s_qty = self._to_int(s_qty_tok)
        pack  = self._to_int(pack_tok)
        unit_p = self._to_float(unit_p_tok)
        um_p   = self._to_float(um_p_tok)
        ext_p  = self._to_float(ext_tok)

        # Sanity: derive unit if missing math consistency
        if (np.isnan(unit_p) or unit_p is None) and (um_p is not None and not np.isnan(um_p)) and (pack not in (np.nan, 0)):
            unit_p = um_p / float(pack)

        # Final guards
        if not desc or pd.isna(pack):
            return None

        return {
            "LINE": int(line_no),
            "ITEM": item,
            "DESCRIPTION": desc,
            "R_QTY": int(r_qty) if not pd.isna(r_qty) else np.nan,
            "S_QTY": int(s_qty) if not pd.isna(s_qty) else np.nan,
            "UM": um_tok or "PK",
            "PACK": int(pack) if not pd.isna(pack) else np.nan,
            "UNIT_P": float(unit_p) if not pd.isna(unit_p) else np.nan,
            "UM_P": float(um_p) if not pd.isna(um_p) else np.nan,
            "EXT_P": float(ext_p) if not pd.isna(ext_p) else np.nan,
        }

    # ---------- parse entire PDF ----------
    def parse(self, uploaded_pdf):
        if pdfplumber is None:
            return pd.DataFrame(columns=self.WANT_COLS), None

        try:
            uploaded_pdf.seek(0)
        except Exception:
            pass

        rows = []
        inv_no = None
        try:
            with pdfplumber.open(uploaded_pdf) as pdf:
                for pi, page in enumerate(pdf.pages):
                    if inv_no is None:
                        inv_no = self._extract_invoice_no(page)

                    txt = page.extract_text() or ""
                    if not txt.strip():
                        continue

                    # Some PDFs duplicate "Page 1 of 4 ..." lines; skip them
                    for raw in txt.splitlines():
                        raw = raw.strip()
                        if not raw:
                            continue
                        parsed = self._parse_text_line(raw)
                        if parsed:
                            rows.append(parsed)
        except Exception:
            return pd.DataFrame(columns=self.WANT_COLS), inv_no

        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS), inv_no

        df = pd.DataFrame(rows, columns=self.WANT_COLS)

        # Deduplicate by LINE (keep first occurrence), keep order
        if "LINE" in df.columns:
            df = df.sort_values(["LINE"]).drop_duplicates(subset=["LINE"], keep="first")

        # Keep only plausible rows (PACK present)
        df = df[df["PACK"].notna()].copy()

        # Final tidy
        df = df.sort_values("LINE").reset_index(drop=True)
        return df, inv_no
