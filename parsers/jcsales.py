# parsers/jcsales.py
# Robust JC Sales PDF parser
# - Tolerates missing spaces at alpha/number boundaries (e.g., "OZ1 1 PK")
# - Handles optional T/C flags before ITEM
# - Preserves invoice order
# - Returns (DataFrame, invoice_no) where DataFrame has columns:
#   ["LINE", "ITEM", "DESCRIPTION", "R_QTY", "S_QTY", "PACK", "UNIT", "COST", "UM_P", "EXT_P"]
#   (Downstream app builds parsed_* with UPC mapping, NOW, DELTA, etc.)

from __future__ import annotations
import re
import numpy as np
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None


LINE_RE = re.compile(
    r"""
    ^\s*
    (?P<line>\d+)\s+                                 # LINE #
    (?:[TC]\s+)?                                     # optional T/C flag
    (?P<item>\d{3,6})\s+                             # ITEM (3-6 digits)
    (?P<desc>.*?)                                    # DESCRIPTION (lazy)
    \s+(?P<rqty>\d+)\s+(?P<sqty>\d+)\s+              # R-QTY S-QTY
    (?:PK|CS|EA)\s+                                  # UM (we key on PK but tolerate)
    (?P<unit>\d+(?:\.\d{2})?)\s+                     # UNIT_P
    (?P<ump>\d+(?:\.\d{2})?)\s+                      # UM_P
    (?P<ext>\d+(?:\.\d{2})?)\s+                      # EXT_P
    (?P<pack>\d+)\s*                                 # #/UM  (PACK)
    $
    """,
    re.VERBOSE,
)


def _fix_line_glitches(s: str) -> str:
    """
    Heal common OCR/text-flow glitches in JC Sales lines:
    - Insert a space at alpha→digit boundaries (e.g., 'OZ1' -> 'OZ 1')
    - Collapse excessive spaces
    - Ensure ' PK ' token is preserved (UM), but allow CS/EA just in case
    """
    if not s:
        return ""
    t = s

    # Insert a space when a letter is immediately followed by a digit (OZ1 -> OZ 1)
    t = re.sub(r"([A-Za-z])(?=\d)", r"\1 ", t)

    # Also insert a space when a digit is immediately followed by a letter, but
    # do NOT split decimals like 28.68; only digit->letter (e.g., '12PK' -> '12 PK')
    t = re.sub(r"(?<=\d)(?=[A-Za-z])", " ", t)

    # Normalize PK/CS/EA casing and spacing around it
    t = re.sub(r"\s+(PK|CS|EA)\s+", r" \1 ", t, flags=re.IGNORECASE)

    # Collapse runs of spaces
    t = re.sub(r"\s{2,}", " ", t).strip()

    return t


def _extract_invoice_no(all_text: str) -> str | None:
    # Look for OSI number anywhere, e.g., *OSI014135*
    m = re.search(r"\bOSI\d+\b", all_text)
    if m:
        return m.group(0)
    # Fallback: the starred version in header
    m2 = re.search(r"\*?(OSI\d+)\*?", all_text)
    return m2.group(1) if m2 else None


def _parse_page_text(txt: str) -> list[dict]:
    rows = []
    for raw in txt.splitlines():
        line = raw.strip()
        if not line or len(line) < 10:
            continue

        healed = _fix_line_glitches(line)

        m = LINE_RE.match(healed)
        if not m:
            continue

        gd = m.groupdict()
        try:
            rows.append(
                {
                    "LINE": int(gd["line"]),
                    "ITEM": gd["item"],
                    "DESCRIPTION": gd["desc"].strip(),
                    "R_QTY": int(gd["rqty"]),
                    "S_QTY": int(gd["sqty"]),
                    "PACK": int(gd["pack"]),
                    "UNIT": float(gd["unit"]),
                    "COST": float(gd["ump"]),   # UM_P is the case cost ("COST" in parsed spec)
                    "UM_P": float(gd["ump"]),
                    "EXT_P": float(gd["ext"]),
                }
            )
        except Exception:
            # If any cast fails, skip this line
            continue
    return rows


class JCSalesParser:
    def parse(self, uploaded_pdf) -> tuple[pd.DataFrame | None, str | None]:
        if pdfplumber is None or uploaded_pdf is None:
            return None, None

        try:
            uploaded_pdf.seek(0)
        except Exception:
            pass

        try:
            with pdfplumber.open(uploaded_pdf) as pdf:
                all_rows = []
                all_text_for_inv = []
                for page in pdf.pages:
                    txt = page.extract_text() or ""
                    all_text_for_inv.append(txt)
                    all_rows.extend(_parse_page_text(txt))

                inv_text = "\n".join(all_text_for_inv)
                invoice_no = _extract_invoice_no(inv_text)

        except Exception:
            return None, None

        if not all_rows:
            return None, invoice_no

        # Sort by LINE to preserve invoice order and de-dup exact repeats
        df = pd.DataFrame(all_rows)
        df = df.sort_values(["LINE"]).drop_duplicates(subset=["LINE", "ITEM", "DESCRIPTION", "PACK", "COST"], keep="first")
        df.reset_index(drop=True, inplace=True)
        return df, invoice_no
