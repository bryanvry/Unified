# parsers/jcsales.py
# JC Sales PDF parser → returns (rows_df, invoice_number) where rows_df has:
#   ITEM, DESCRIPTION, PACK, COST, UNIT
# App layer will add: UPC, RETAIL, NOW, DELTA, and will do all Master/Pricebook joins.

from __future__ import annotations
from typing import List, Optional, Tuple
import re
import numpy as np
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None


def _digits(s: str) -> str:
    return "".join(ch for ch in str(s or "") if ch.isdigit())

def _norm_upc_12(u: str) -> str:
    d = _digits(u)
    if len(d) > 12:
        d = d[-12:]
    return d.zfill(12)

def _to_float(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
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
        return int(round(float(str(x).replace(",", "").strip())))
    except Exception:
        return 0


def _find_header_map(header: List[str]) -> Optional[dict]:
    """
    Header aliases from your JC Sales PDF:
      ITEM, DESCRIPTION, #/UM, UNIT_P, UM_P
    We only *require* ITEM/DESCRIPTION/#/UM/UM_P. UNIT_P is nice-to-have; we can recompute if missing.
    """
    def canon(s: str) -> str:
        return re.sub(r"[^a-z0-9]", "", (s or "").lower())

    cmap = {canon(h): h for h in header if h is not None}

    def pick(*aliases):
        for a in aliases:
            ca = canon(a)
            if ca in cmap:
                return cmap[ca]
        # substring fallback
        for a in aliases:
            ca = canon(a)
            for k, orig in cmap.items():
                if ca in k:
                    return orig
        return None

    col_item = pick("item")
    col_desc = pick("description", "desc")
    col_pack = pick("#/um", "pack", "qtypercase", "qtycase")
    col_unit = pick("unit_p", "unitp", "unitprice")
    col_cost = pick("um_p", "ump", "caseprice", "casecost", "umprice")

    # We need at least ITEM/DESCRIPTION/PACK/COST
    if all([col_item, col_desc, col_pack, col_cost]):
        return {"ITEM": col_item, "DESCRIPTION": col_desc, "PACK": col_pack, "UNIT_P": col_unit, "UM_P": col_cost}
    return None


class JCSalesParser:
    name = "JC Sales"

    def __init__(self):
        self.last_invoice_number: Optional[str] = None

    def _extract_invoice_number(self, page_texts: List[str]) -> Optional[str]:
        # Looks like OSI014135 style appears on the PDF
        txt = "\n".join(t for t in page_texts if t)
        m = re.search(r"\b(OSI\d{5,})\b", txt, re.IGNORECASE)
        if m:
            return m.group(1).upper()
        m = re.search(r"Invoice\s*(No\.?|#|:)\s*([A-Z0-9\-]+)", txt, re.IGNORECASE)
        if m:
            return m.group(2).upper()
        return None

    def _tables_from_pdf(self, pdf: "pdfplumber.PDF") -> pd.DataFrame:
        rows = []
        page_texts = []

        for pg in pdf.pages:
            page_texts.append(pg.extract_text() or "")
            table_settings_list = [
                {"vertical_strategy": "lines", "horizontal_strategy": "lines", "intersection_tolerance": 5, "snap_tolerance": 3, "join_tolerance": 3, "edge_min_length": 3},
                {"vertical_strategy": "text",  "horizontal_strategy": "text"},
            ]
            for settings in table_settings_list:
                try:
                    tables = pg.extract_tables(table_settings=settings)
                except Exception:
                    tables = []
                for tbl in tables or []:
                    if not tbl or len(tbl) < 2:
                        continue

                    # find a non-empty header row
                    header = None
                    header_idx = 0
                    for i, r in enumerate(tbl[:6]):
                        if any(str(x or "").strip() for x in r):
                            header = [str(x or "").strip() for x in r]
                            header_idx = i
                            break
                    if not header:
                        continue

                    colmap = _find_header_map(header)
                    if not colmap:
                        continue

                    body = tbl[header_idx + 1 :]
                    if not body:
                        continue

                    df = pd.DataFrame(body, columns=header)
                    for _, r in df.iterrows():
                        item = str(r.get(colmap["ITEM"], "")).strip()
                        desc = str(r.get(colmap["DESCRIPTION"], "")).strip()
                        pack = _to_int(r.get(colmap["PACK"], ""))
                        unit = _to_float(r.get(colmap["UNIT_P"], ""))
                        cost = _to_float(r.get(colmap["UM_P"], ""))  # case cost

                        if not item or not desc or pack <= 0 or (cost is None) or np.isnan(cost):
                            continue

                        if unit is None or np.isnan(unit) or unit <= 0:
                            # recompute if missing
                            unit = cost / pack if pack > 0 else np.nan

                        rows.append({
                            "ITEM": item,
                            "DESCRIPTION": desc,
                            "PACK": int(pack),
                            "COST": float(cost),
                            "UNIT": float(unit),
                        })

        # cache invoice number
        self.last_invoice_number = self._extract_invoice_number(page_texts)

        if not rows:
            return pd.DataFrame(columns=["ITEM","DESCRIPTION","PACK","COST","UNIT"])
        return pd.DataFrame(rows, columns=["ITEM","DESCRIPTION","PACK","COST","UNIT"]).reset_index(drop=True)

    def parse(self, uploaded_pdf) -> Tuple[pd.DataFrame, Optional[str]]:
        if pdfplumber is None:
            return pd.DataFrame(columns=["ITEM","DESCRIPTION","PACK","COST","UNIT"]), None

        try:
            uploaded_pdf.seek(0)
        except Exception:
            pass

        name = (getattr(uploaded_pdf, "name", "") or "").lower()
        if not name.endswith(".pdf"):
            return pd.DataFrame(columns=["ITEM","DESCRIPTION","PACK","COST","UNIT"]), None

        try:
            with pdfplumber.open(uploaded_pdf) as pdf:
                out = self._tables_from_pdf(pdf)
        except Exception:
            out = pd.DataFrame(columns=["ITEM","DESCRIPTION","PACK","COST","UNIT"])

        return out, (self.last_invoice_number or None)
