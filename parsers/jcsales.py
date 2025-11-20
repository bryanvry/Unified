# parsers/jcsales.py
# JC Sales PDF parser → returns (rows_df, invoice_number)
# rows_df columns: ITEM, DESCRIPTION, PACK, COST, UNIT
# App layer (your app.py) computes UPC/RETAIL/NOW/DELTA and builds POS_update.

from __future__ import annotations
from typing import List, Optional, Tuple, Dict
import re
import numpy as np
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None


# ---------- small utils ----------
def _digits(s: str) -> str:
    return "".join(ch for ch in str(s or "") if ch.isdigit())

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

def _canon(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


# ---------- header detection ----------
def _find_header_map(header: List[str]) -> Optional[dict]:
    """
    JC Sales header variants seen on PDF:
      ITEM | DESCRIPTION | #/UM | UNIT_P | UM_P
    We'll accept loose matches like "UNIT P", "UM P", "Qty/UM", etc.
    """
    cmap = {_canon(h): h for h in header if h is not None}

    def pick(*aliases):
        for a in aliases:
            ca = _canon(a)
            if ca in cmap:
                return cmap[ca]
        # substring fallback
        for a in aliases:
            ca = _canon(a)
            for k, orig in cmap.items():
                if ca in k:
                    return orig
        return None

    col_item = pick("item")
    col_desc = pick("description", "desc")
    col_pack = pick("#/um", "qty/um", "qtypercase", "qtycase", "pack")
    col_unit = pick("unit_p", "unit p", "unitprice", "unit")
    col_cost = pick("um_p", "um p", "caseprice", "casecost", "umprice")

    # ITEM/DESCRIPTION/PACK and COST must exist; UNIT optional (recomputed if missing)
    if all([col_item, col_desc, col_pack, col_cost]):
        return {"ITEM": col_item, "DESCRIPTION": col_desc, "PACK": col_pack, "UNIT_P": col_unit, "UM_P": col_cost}
    return None


# ---------- parser class ----------
class JCSalesParser:
    name = "JC Sales"

    def __init__(self):
        self.last_invoice_number: Optional[str] = None

    # --- invoice number extractor ---
    def _extract_invoice_number(self, page_texts: List[str]) -> Optional[str]:
        txt = "\n".join(t for t in page_texts if t)
        m = re.search(r"\b(OSI\d{5,})\b", txt, re.IGNORECASE)
        if m:
            return m.group(1).upper()
        m = re.search(r"Invoice\s*(No\.?|#|:)\s*([A-Z0-9\-]+)", txt, re.IGNORECASE)
        if m:
            return m.group(2).upper()
        return None

    # --- table strategy (first try) ---
    def _extract_by_tables(self, pdf: "pdfplumber.PDF") -> Tuple[pd.DataFrame, List[str]]:
        rows = []
        page_texts = []
        table_settings_list = [
            {"vertical_strategy": "lines", "horizontal_strategy": "lines",
             "intersection_tolerance": 5, "snap_tolerance": 3,
             "join_tolerance": 3, "edge_min_length": 3},
            {"vertical_strategy": "text", "horizontal_strategy": "text"},
        ]
        for pg in pdf.pages:
            page_texts.append(pg.extract_text() or "")
            for settings in table_settings_list:
                try:
                    tables = pg.extract_tables(table_settings=settings)
                except Exception:
                    tables = []
                for tbl in tables or []:
                    if not tbl or len(tbl) < 2:
                        continue
                    # find non-empty header
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
                            unit = cost / pack if pack > 0 else np.nan

                        rows.append({"ITEM": item, "DESCRIPTION": desc, "PACK": int(pack), "COST": float(cost), "UNIT": float(unit)})

        df_out = pd.DataFrame(rows, columns=["ITEM","DESCRIPTION","PACK","COST","UNIT"]) if rows else pd.DataFrame(columns=["ITEM","DESCRIPTION","PACK","COST","UNIT"])
        return df_out, page_texts

    # --- word-grid (robust) fallback ---
    def _extract_by_wordgrid(self, pdf: "pdfplumber.PDF") -> Tuple[pd.DataFrame, List[str]]:
        page_texts = []
        all_rows = []

        def find_header_and_xcenters(page) -> Optional[Tuple[float, Dict[str, float]]]:
            words = page.extract_words(keep_blank_chars=False, use_text_flow=True)
            if not words:
                return None
            # cluster by y to form lines
            lines = {}
            for w in words:
                key = round(float(w["top"]) / 2.0, 0)
                lines.setdefault(key, []).append(w)
            header_key = None
            best_score = 0
            for key, ws in lines.items():
                txt = " ".join([w["text"] for w in sorted(ws, key=lambda x: x["x0"])])
                c = _canon(txt)
                score = 0
                for tok in ["item", "description", "um", "unit", "um_p", "unit_p", "#/um"]:
                    if tok.replace("_","") in c:
                        score += 1
                if score >= 3 and score >= best_score:
                    best_score = score
                    header_key = key
            if header_key is None:
                return None
            header_words = sorted(lines[header_key], key=lambda x: x["x0"])

            def xcenter_of(*aliases):
                for a in aliases:
                    ca = _canon(a)
                    for w in header_words:
                        if ca in _canon(w["text"]):
                            return (w["x0"] + w["x1"]) / 2.0
                # fallback: median
                if header_words:
                    return float(np.median([(w["x0"] + w["x1"]) / 2.0 for w in header_words]))
                return None

            x_item = xcenter_of("item")
            x_desc = xcenter_of("description","desc")
            x_pack = xcenter_of("#/um","qty/um","pack")
            x_unit = xcenter_of("unit_p","unit p","unit")
            x_cost = xcenter_of("um_p","um p","casecost","caseprice","umprice")

            header_y = float(np.mean([w["top"] for w in header_words]))
            cols = {"item": x_item, "desc": x_desc, "pack": x_pack, "unit": x_unit, "cost": x_cost}
            if sum(v is not None for v in cols.values()) < 3:
                return None
            return header_y, cols

        def bucket(words_line, xcenters):
            buckets = {"item": [], "desc": [], "pack": [], "unit": [], "cost": []}
            for w in sorted(words_line, key=lambda x: x["x0"]):
                xc = (w["x0"] + w["x1"]) / 2.0
                best, best_dx = None, 1e9
                for key, xx in xcenters.items():
                    if xx is None: 
                        continue
                    dx = abs(xc - xx)
                    if dx < best_dx:
                        best, best_dx = key, dx
                buckets[best or "desc"].append(w["text"])
            return {k: " ".join(v).strip() for k, v in buckets.items()}

        for pg in pdf.pages:
            page_texts.append(pg.extract_text() or "")
            words = pg.extract_words(keep_blank_chars=False, use_text_flow=True)
            if not words:
                continue
            header = find_header_and_xcenters(pg)
            if not header:
                continue
            header_y, colx = header

            # group lines below header
            lines = {}
            for w in words:
                if w["top"] <= header_y + 1:
                    continue
                key = round(float(w["top"]) / 2.0, 0)
                lines.setdefault(key, []).append(w)

            for key in sorted(lines.keys()):
                line_words = lines[key]
                b = bucket(line_words, colx)
                item_txt = b.get("item","").strip()
                desc_txt = b.get("desc","").strip()
                pack_txt = b.get("pack","").strip()
                unit_txt = b.get("unit","").strip()
                cost_txt = b.get("cost","").strip()

                # numeric cleaning
                pack = _to_int(pack_txt)
                unit = _to_float(unit_txt)
                cost = _to_float(cost_txt)

                # tolerate unit missing; recompute from cost/pack later
                if not item_txt or not desc_txt:
                    continue
                if pack <= 0:
                    continue
                if cost is None or np.isnan(cost) or cost <= 0:
                    continue
                if unit is None or np.isnan(unit) or unit <= 0:
                    unit = cost / pack if pack > 0 else np.nan

                all_rows.append({"ITEM": item_txt, "DESCRIPTION": desc_txt, "PACK": int(pack), "COST": float(cost), "UNIT": float(unit)})

        df = pd.DataFrame(all_rows, columns=["ITEM","DESCRIPTION","PACK","COST","UNIT"]) if all_rows else pd.DataFrame(columns=["ITEM","DESCRIPTION","PACK","COST","UNIT"])
        return df, page_texts

    # --- public API ---
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

        # 1) tables
        try:
            with pdfplumber.open(uploaded_pdf) as pdf:
                df1, txt1 = self._extract_by_tables(pdf)
                if df1 is not None and not df1.empty:
                    self.last_invoice_number = self._extract_invoice_number(txt1)
                    return df1.reset_index(drop=True), (self.last_invoice_number or None)
        except Exception:
            pass

        # 2) word-grid fallback
        try:
            with pdfplumber.open(uploaded_pdf) as pdf:
                df2, txt2 = self._extract_by_wordgrid(pdf)
                self.last_invoice_number = self._extract_invoice_number(txt2)
                return df2.reset_index(drop=True), (self.last_invoice_number or None)
        except Exception:
            pass

        return pd.DataFrame(columns=["ITEM","DESCRIPTION","PACK","COST","UNIT"]), None
