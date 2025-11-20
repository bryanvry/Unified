# JC Sales PDF parser
# Output columns for parsed workbook: UPC, DESCRIPTION, PACK, COST, UNIT, RETAIL, NOW, DELTA
# This parser only extracts the raw line items from the PDF:
#   ITEM, DESCRIPTION, PACK (#/UM), COST (UM_P), UNIT (UNIT_P)
# …and returns (rows_df, invoice_number).
#
# Safeguards to avoid phantom rows:
#   • ITEM must be all digits
#   • PACK >= 1, UNIT > 0, COST > 0
#   • |UNIT*PACK - COST| <= 0.02 (drops footers like "2 times printed Page …")
#
# The app wires the rest (UPC resolution via Master+Pricebook, NOW/DELTA math, POS_update).

from __future__ import annotations
from typing import Tuple, Optional, List, Dict
import re
import math
import numpy as np
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None

NUM_TOL = 0.02  # tolerance for UNIT*PACK ~= COST


def _to_float(x):
    if x is None:
        return np.nan
    if isinstance(x, (int, float, np.number)):
        return float(x)
    s = str(x).replace("$", "").replace(",", "").strip()
    s = re.sub(r"[^\d.\-]", "", s)
    try:
        return float(s)
    except Exception:
        return np.nan


def _to_int(x):
    try:
        return int(round(float(str(x).strip())))
    except Exception:
        return 0


def _canon(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())


def _nearest_xcenter(header_words, *aliases):
    target_keys = [_canon(a) for a in aliases]
    for w in header_words:
        if any(k in _canon(w["text"]) for k in target_keys):
            return (w["x0"] + w["x1"]) / 2.0
    # if not found, median of all header x-centers as fallback
    if header_words:
        return float(np.median([(w["x0"] + w["x1"]) / 2.0 for w in header_words]))
    return None


def _bucket_by_columns(words_line, col_x: Dict[str, float]) -> Dict[str, str]:
    """Assign each word to the closest column x-center."""
    buckets = {k: [] for k in ["line", "item", "desc", "rqty", "sqty", "um", "pack", "unitp", "ump", "extp"]}
    for w in sorted(words_line, key=lambda x: x["x0"]):
        xc = (w["x0"] + w["x1"]) / 2.0
        best = None
        best_dx = 1e9
        for key, xcenter in col_x.items():
            if xcenter is None:
                continue
            dx = abs(xc - xcenter)
            if dx < best_dx:
                best = key
                best_dx = dx
        if best is None:
            # default to description if we truly can't place it
            best = "desc"
        # squash multiple spaces later
        buckets[best].append(w["text"])
    return {k: " ".join(v).strip() for k, v in buckets.items()}


def _extract_invoice_number(text: str) -> Optional[str]:
    # Typical: "*OSI014135*" or "JCSALES OSI014135 …"
    m = re.search(r"\bOSI(\d+)\b", text)
    if m:
        return "OSI" + m.group(1)
    return None


class JCSalesParser:
    WANT_COLS = ["ITEM", "DESCRIPTION", "PACK", "UNIT", "COST"]

    def parse(self, uploaded_pdf) -> Tuple[pd.DataFrame, Optional[str]]:
        if pdfplumber is None:
            return pd.DataFrame(columns=self.WANT_COLS), None

        try:
            uploaded_pdf.seek(0)
        except Exception:
            pass

        try:
            with pdfplumber.open(uploaded_pdf) as pdf:
                all_rows: List[dict] = []
                invoice_no: Optional[str] = None

                for page in pdf.pages:
                    # page text for invoice number
                    raw_text = page.extract_text() or ""
                    if invoice_no is None:
                        inv = _extract_invoice_number(raw_text)
                        if inv:
                            invoice_no = inv

                    words = page.extract_words(keep_blank_chars=False, use_text_flow=True)
                    if not words:
                        continue

                    # Group words into rough lines by Y bucket
                    lines: Dict[float, List[dict]] = {}
                    for w in words:
                        key = round(w["top"] / 2.0, 0)
                        lines.setdefault(key, []).append(w)

                    # Find the header row for this page
                    header_key = None
                    best_score = 0
                    for key, ws in lines.items():
                        txt = " ".join([ww["text"] for ww in sorted(ws, key=lambda x: x["x0"])])
                        c = _canon(txt)
                        score = sum(k in c for k in [
                            "line", "item", "description",
                            "rqty", "sqty", "um", "um", "unitp", "ump", "extp"
                        ])
                        if score >= 3 and score >= best_score and "line" in c and "item" in c and "description" in c:
                            best_score = score
                            header_key = key

                    if header_key is None:
                        # no header; skip page
                        continue

                    header_words = sorted(lines[header_key], key=lambda x: x["x0"])
                    header_y = float(np.mean([w["top"] for w in header_words]))

                    # Column x-centers
                    col_x = {
                        "line":  _nearest_xcenter(header_words, "LINE #", "LINE"),
                        "item":  _nearest_xcenter(header_words, "ITEM"),
                        "desc":  _nearest_xcenter(header_words, "DESCRIPTION", "DESC"),
                        "rqty":  _nearest_xcenter(header_words, "R-QTY", "RQTY"),
                        "sqty":  _nearest_xcenter(header_words, "S-QTY", "SQTY"),
                        "um":    _nearest_xcenter(header_words, "UM"),
                        "pack":  _nearest_xcenter(header_words, "#/UM", "# UM"),
                        "unitp": _nearest_xcenter(header_words, "UNIT_P", "UNIT P"),
                        "ump":   _nearest_xcenter(header_words, "UM_P", "UM P"),
                        "extp":  _nearest_xcenter(header_words, "EXT_P", "EXT P"),
                    }

                    # Iterate lines below header and parse rows
                    for key in sorted(lines.keys()):
                        if key <= header_key:
                            continue
                        line_words = lines[key]

                        # Skip obvious footers / junk by looking for "Page", "Printed", "Customer Copy", etc.
                        raw_line = " ".join([w["text"] for w in sorted(line_words, key=lambda x: x["x0"])])
                        if re.search(r"\b(Page|Printed|Customer Copy|JCSALES)\b", raw_line, re.I):
                            continue

                        buck = _bucket_by_columns(line_words, col_x)

                        # Normalize fields
                        # line: allow optional flag after number (e.g., "2 T")
                        line_txt = buck.get("line", "")
                        line_no_m = re.match(r"^\s*(\d+)(?:\s+[A-Z])?\s*$", line_txt)
                        if not line_no_m:
                            # If no line number, it's likely a wrap/continuation — skip
                            continue

                        item_txt = buck.get("item", "").strip()
                        if not item_txt.isdigit():
                            continue  # ITEM must be digits per your invoice

                        desc_txt = buck.get("desc", "").strip()
                        if not desc_txt or desc_txt.lower().startswith(("line", "item", "description")):
                            continue

                        # PACK comes from #/UM column; some PDFs repeat it at the end — we'll trust #/UM first
                        pack_txt = buck.get("pack", "").strip()
                        pack_val = _to_int(re.search(r"\d+", pack_txt).group(0)) if re.search(r"\d+", pack_txt) else None

                        # UNIT and COST
                        unit_val = _to_float(buck.get("unitp", ""))
                        cost_val = _to_float(buck.get("ump", ""))

                        # If #/UM was empty, try to pick trailing integer from raw line (common on your sample)
                        if (pack_val is None or pack_val <= 0):
                            tail_nums = re.findall(r"(\d+)\s*$", raw_line)
                            if tail_nums:
                                pack_val = _to_int(tail_nums[-1])

                        # Final validation to kill header/footer noise
                        if pack_val is None or pack_val <= 0:
                            continue
                        if unit_val is None or math.isnan(unit_val) or unit_val <= 0:
                            continue
                        if cost_val is None or math.isnan(cost_val) or cost_val <= 0:
                            continue
                        if abs(unit_val * pack_val - cost_val) > NUM_TOL:
                            # Not a sane row (often footer/garbage)
                            continue

                        all_rows.append({
                            "ITEM": item_txt,
                            "DESCRIPTION": desc_txt,
                            "PACK": int(pack_val),
                            "UNIT": float(unit_val),
                            "COST": float(cost_val),
                            "_order": int(line_no_m.group(1)),
                        })

                if not all_rows:
                    return pd.DataFrame(columns=self.WANT_COLS), invoice_no

                out = pd.DataFrame(all_rows).sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)
                return out[self.WANT_COLS], invoice_no
        except Exception:
            return pd.DataFrame(columns=self.WANT_COLS), None
