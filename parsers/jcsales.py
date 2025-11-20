# parsers/jcsales.py
# JC Sales PDF parser with high-recall hybrid extraction.
# Public APIs preserved:
#   - parse(invoice_pdf) -> (rows_df, invoice_number)
#       rows_df columns: ["ITEM","DESCRIPTION","PACK","COST","UNIT"]
#   - parse_invoice(invoice_pdf, jc_master_xlsx, pricebook_csv) -> (parsed_df, pos_slice)
#       parsed_df columns: ["UPC","DESCRIPTION","PACK","COST","UNIT","RETAIL","NOW","DELTA","ITEM"]
#
# Notes:
# - Soft validation only (no UNIT*PACK≈COST rejection) to avoid dropping legit lines.
# - Handles: "PK 12" / "12PK" / "12 PK", optional trailing pack override, optional flags ("T","C"),
#   optional leading line #, wrapped descriptions, multi-page footers/headers.
# - Keeps invoice order.

from __future__ import annotations
from typing import Tuple, Optional, List, Dict
import re
import numpy as np
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None

# ---------- small helpers ----------
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

def _lz_strip(s: str) -> str:
    s = re.sub(r"\D", "", str(s or ""))
    return s.lstrip("0") or "0"

def _digits(s: str) -> str:
    return re.sub(r"\D", "", str(s or ""))

# ---------- core regex patterns ----------
_MONEY = r"(\d+\.\d{2})"

# Greedy line matcher:
# [opt LINE#] [opt FLAG] ITEM DESCRIPTION RQ SQ (PK 12 | 12PK | 12 PK) UNIT_P UM_P EXT_P [opt PACK_OVERRIDE]
_RX_MAIN = re.compile(
    rf"""
    ^
    \s*
    (?:\d+\s+)?                    # optional leading line number
    (?:[A-Z]\s+)?                  # optional flag (T/C/etc.)
    (?P<item>\d{{3,6}})\s+         # ITEM (3-6 digits)
    (?P<desc>.*?)\s+               # DESCRIPTION (greedy)
    (?P<rqty>\d+)\s+               # R-QTY
    (?P<sqty>\d+)\s+               # S-QTY
    (?:
        (?P<umA>[A-Z]+)\s+(?P<packA>\d+)      # "PK 12"
        |
        (?P<packB>\d+)\s*(?P<umB>[A-Z]+)      # "12PK" or "12 PK"
    )
    \s+
    (?P<unit>{_MONEY})\s+          # UNIT_P
    (?P<cost>{_MONEY})\s+          # UM_P (case cost)
    {_MONEY}                       # EXT_P (ignored)
    (?:\s+(?P<packZ>\d+))?         # optional trailing pack override
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

# A bit looser on the description / spaces (fallback on same text stream)
_RX_FALLBACK = re.compile(
    rf"""
    ^
    \s*
    (?:\d+\s+)?(?:[A-Z]\s+)?       # optional line number + flag
    (?P<item>\d{{3,6}})\s+
    (?P<desc>.+?)\s+
    (?P<rqty>\d+)\s+(?P<sqty>\d+)\s+
    (?:
        (?P<umA>[A-Z]+)\s+(?P<packA>\d+)
        |
        (?P<packB>\d+)\s*(?P<umB>[A-Z]+)
    )
    \s+
    (?P<unit>{_MONEY})\s+(?P<cost>{_MONEY})\s+{_MONEY}
    (?:\s+(?P<packZ>\d+))?
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

_INV_RX = re.compile(r"\bOSI0?\d+\b", re.IGNORECASE)

def _extract_invoice_number(text: str) -> Optional[str]:
    m = _INV_RX.search(text or "")
    return m.group(0) if m else None

# ---------- word-grid helpers for fallback ----------
def _nearest_xcenter(header_words, *aliases):
    want = [_canon(a) for a in aliases]
    for w in header_words:
        if any(k in _canon(w["text"]) for k in want):
            return (w["x0"] + w["x1"]) / 2.0
    if header_words:
        return float(np.median([(w["x0"] + w["x1"]) / 2.0 for w in header_words]))
    return None

def _bucket_by_columns(words_line, col_x: Dict[str, float]) -> Dict[str, str]:
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
            best = "desc"
        buckets[best].append(w["text"])
    return {k: " ".join(v).strip() for k, v in buckets.items()}

# ---------- JC Sales Parser ----------
class JCSalesParser:
    WANT_COLS = ["ITEM", "DESCRIPTION", "PACK", "COST", "UNIT"]

    # ---- Stage 1: text lines with merge ----
    def _parse_text_stream(self, text: str) -> List[dict]:
        """
        Merge wrapped lines and run regex; return list of dict rows.
        """
        raw_lines = [ln.rstrip() for ln in (text or "").splitlines()]
        merged: List[str] = []
        buf = ""

        def flush():
            nonlocal buf
            if buf.strip():
                merged.append(buf.strip())
            buf = ""

        for ln in raw_lines:
            s = ln.strip()
            if not s:
                continue
            # If it *looks* like a new row (starts with [line?][flag?] ITEMID ...),
            # flush buffer first.
            if re.match(r"^\s*(?:\d+\s+)?(?:[A-Z]\s+)?\d{3,6}\b", s):
                flush()
                buf = s
            else:
                # continuation of description / spillover
                buf = (buf + " " + s).strip()

        flush()

        rows = []
        for li, line in enumerate(merged):
            # skip obvious page furniture
            if re.search(r"\b(Page|Printed|Customer Copy|JCSALES)\b", line, re.I):
                continue
            m = _RX_MAIN.match(line) or _RX_FALLBACK.match(line)
            if not m:
                continue
            item = m.group("item").strip()
            desc = re.sub(r"\s{2,}", " ", m.group("desc")).strip()

            rqty = _to_int(m.group("rqty"))
            sqty = _to_int(m.group("sqty"))

            pack_override = _to_int(m.group("packZ"))
            packA = _to_int(m.group("packA"))
            packB = _to_int(m.group("packB"))
            pack = pack_override or packA or packB or 0

            unit_p = _to_float(m.group("unit"))
            um_p   = _to_float(m.group("cost"))

            # minimal guards (soft)
            if sqty <= 0 or pack <= 0:
                continue
            if np.isnan(unit_p) or np.isnan(um_p):
                continue

            # If unit > cost (rare OCR), swap
            if unit_p > um_p:
                unit_p, um_p = um_p, unit_p

            rows.append({
                "ITEM": item,
                "DESCRIPTION": desc,
                "PACK": int(pack),
                "COST": float(um_p),
                "UNIT": float(unit_p),
                "_order": len(rows)
            })
        return rows

    # ---- Stage 2: word-grid fallback ----
    def _parse_word_grid_page(self, page) -> List[dict]:
        words = page.extract_words(keep_blank_chars=False, use_text_flow=True)
        if not words:
            return []

        # group into y-buckets
        lines: Dict[float, List[dict]] = {}
        for w in words:
            key = round(w["top"] / 2.0, 0)
            lines.setdefault(key, []).append(w)

        # find header
        header_key = None
        best_score = 0
        for key, ws in lines.items():
            txt = " ".join([ww["text"] for ww in sorted(ws, key=lambda x: x["x0"])])
            c = _canon(txt)
            score = sum(k in c for k in ["line", "item", "description", "rqty", "sqty", "um", "unitp", "ump", "extp"])
            if score >= 3 and score >= best_score and "line" in c and "item" in c and "description" in c:
                best_score = score
                header_key = key
        if header_key is None:
            return []

        header_words = sorted(lines[header_key], key=lambda x: x["x0"])
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

        out = []
        for key in sorted(lines.keys()):
            if key <= header_key:
                continue
            line_words = lines[key]
            raw_line = " ".join([w["text"] for w in sorted(line_words, key=lambda x: x["x0"])])

            if re.search(r"\b(Page|Printed|Customer Copy|JCSALES)\b", raw_line, re.I):
                continue

            buck = _bucket_by_columns(line_words, col_x)

            # accept optional flag after the number, but require a leading number
            line_txt = buck.get("line", "")
            if not re.match(r"^\s*\d+(?:\s+[A-Z])?\s*$", line_txt or ""):
                continue

            item_txt = (buck.get("item", "") or "").strip()
            if not item_txt.isdigit():
                continue

            desc_txt = (buck.get("desc", "") or "").strip()
            rqty = _to_int(buck.get("rqty", ""))
            sqty = _to_int(buck.get("sqty", ""))

            pack_txt = (buck.get("pack", "") or "")
            mpack = re.search(r"\d+", pack_txt)
            pack = _to_int(mpack.group(0)) if mpack else 0

            unit_p = _to_float(buck.get("unitp", ""))
            um_p   = _to_float(buck.get("ump", ""))

            if sqty <= 0 or pack <= 0:
                continue
            if np.isnan(unit_p) or np.isnan(um_p):
                continue
            if unit_p > um_p:
                unit_p, um_p = um_p, unit_p

            out.append({
                "ITEM": item_txt,
                "DESCRIPTION": desc_txt,
                "PACK": int(pack),
                "COST": float(um_p),
                "UNIT": float(unit_p),
                "_order": len(out)
            })
        return out

    # ---- Stage 3: pdf tables fallback ----
    def _parse_tables(self, page) -> List[dict]:
        out = []
        tries = [
            {"vertical_strategy": "lines", "horizontal_strategy": "lines", "intersection_tolerance": 5, "snap_tolerance": 3, "join_tolerance": 3, "edge_min_length": 3},
            {"vertical_strategy": "text", "horizontal_strategy": "text"},
        ]
        for settings in tries:
            try:
                tables = page.extract_tables(table_settings=settings)
            except Exception:
                tables = []
            for tbl in tables or []:
                if not tbl or len(tbl) < 2:
                    continue
                header = None; idx = 0
                for i, row in enumerate(tbl[:6]):
                    if any(str(x or "").strip() for x in row):
                        header = [str(x or "").strip() for x in row]
                        idx = i; break
                if header is None:
                    continue
                cmap = { _canon(c): c for c in header }
                def pick(*names):
                    for nm in names:
                        c = _canon(nm)
                        if c in cmap: return cmap[c]
                    for nm in names:
                        c = _canon(nm)
                        for k, v in cmap.items():
                            if c in k: return v
                    return None
                c_item = pick("ITEM")
                c_desc = pick("DESCRIPTION","DESC")
                c_rqty = pick("R-QTY","RQTY")
                c_sqty = pick("S-QTY","SQTY")
                c_um   = pick("UM")
                c_pack = pick("#/UM","# UM")
                c_unit = pick("UNIT_P","UNIT P")
                c_cost = pick("UM_P","UM P")
                body = tbl[idx+1:]
                if not all([c_item,c_desc,c_sqty,c_pack,c_unit,c_cost]):  # minimal columns
                    continue
                df = pd.DataFrame(body, columns=header)
                for _, r in df.iterrows():
                    item = str(r.get(c_item,"")).strip()
                    if not item.isdigit():
                        continue
                    desc = str(r.get(c_desc,"")).strip()
                    sqty = _to_int(r.get(c_sqty,""))
                    pack = _to_int(r.get(c_pack,""))
                    unit = _to_float(r.get(c_unit,""))
                    cost = _to_float(r.get(c_cost,""))
                    if sqty <= 0 or pack <= 0:
                        continue
                    if np.isnan(unit) or np.isnan(cost):
                        continue
                    if unit > cost:
                        unit, cost = cost, unit
                    out.append({
                        "ITEM": item,
                        "DESCRIPTION": desc,
                        "PACK": int(pack),
                        "COST": float(cost),
                        "UNIT": float(unit),
                        "_order": len(out)
                    })
        return out

    # ---------- Public minimal API ----------
    def parse(self, invoice_pdf) -> Tuple[pd.DataFrame, Optional[str]]:
        if pdfplumber is None:
            return pd.DataFrame(columns=self.WANT_COLS), None
        try:
            invoice_pdf.seek(0)
        except Exception:
            pass

        rows: List[dict] = []
        inv_no: Optional[str] = None
        try:
            with pdfplumber.open(invoice_pdf) as pdf:
                for page in pdf.pages:
                    text = page.extract_text() or ""
                    if inv_no is None:
                        inv_no = _extract_invoice_number(text)
                    # Stage 1
                    rows += self._parse_text_stream(text)
                    # If we’re still clearly short, try Stage 2
                    if len(rows) < 120:  # heuristic for long invoices; still safe for short ones
                        rows += self._parse_word_grid_page(page)
                    # If still meager, try Stage 3
                    if len(rows) < 120:
                        rows += self._parse_tables(page)
        except Exception:
            return pd.DataFrame(columns=self.WANT_COLS), None

        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS), inv_no

        # Deduplicate by (ITEM, PACK, COST, UNIT) keeping first (order preserved)
        df = pd.DataFrame(rows)
        df = df.drop_duplicates(subset=["ITEM","PACK","COST","UNIT"], keep="first")
        df = df.sort_index(kind="stable")  # preserve original discovery order
        return df[["ITEM","DESCRIPTION","PACK","COST","UNIT"]].reset_index(drop=True), inv_no

    # ---------- Rich API (unchanged behavior) ----------
    def parse_invoice(self, invoice_pdf, jc_master_xlsx, pricebook_csv) -> Tuple[pd.DataFrame, pd.DataFrame]:
        # parse body
        body, _ = self.parse(invoice_pdf)
        if body.empty:
            return pd.DataFrame(), pd.DataFrame()

        # load master + pricebook
        try:
            jc_master = pd.read_excel(jc_master_xlsx)
        except Exception:
            jc_master = pd.DataFrame()

        try:
            pb = pd.read_csv(pricebook_csv, dtype=str)
        except Exception:
            pb = pd.DataFrame()

        for col in ["Upc","cents","cost_cents","cost_qty"]:
            if col not in pb.columns:
                pb[col] = ""

        pb["_Upc_norm"] = pb["Upc"].astype(str).str.replace(r"\D","",regex=True).apply(_lz_strip)

        if not jc_master.empty:
            mcols = {c.lower(): c for c in jc_master.columns}
            col_item = mcols.get("item")
            col_u1 = mcols.get("upc1")
            col_u2 = mcols.get("upc2")
            if col_item:
                master = jc_master[[col_item] + [c for c in [col_u1,col_u2] if c]].copy()
                master.columns = ["ITEM"] + ([ "UPC1" ] if col_u1 else []) + ([ "UPC2" ] if col_u2 else [])
                master["ITEM"] = master["ITEM"].astype(str).str.strip()
                master = master.drop_duplicates("ITEM").set_index("ITEM")
            else:
                master = pd.DataFrame(columns=["UPC1","UPC2"])
        else:
            master = pd.DataFrame(columns=["UPC1","UPC2"])

        pb_set = set(pb["_Upc_norm"].tolist())

        def resolve_upc(item_str: str) -> str:
            if item_str in master.index:
                u1 = str(master.loc[item_str, "UPC1"]) if "UPC1" in master.columns else ""
                u2 = str(master.loc[item_str, "UPC2"]) if "UPC2" in master.columns else ""
            else:
                u1 = u2 = ""
            u1n = _lz_strip(u1)
            u2n = _lz_strip(u2)
            if u1n != "0" and u1n in pb_set:
                return _digits(u1).zfill(12)
            if u2n != "0" and u2n in pb_set:
                return _digits(u2).zfill(12)
            return f"No Match {item_str}"

        parsed = body.copy()
        parsed["UPC"] = parsed["ITEM"].astype(str).apply(resolve_upc)

        parsed["UNIT"] = pd.to_numeric(parsed["UNIT"], errors="coerce")
        parsed["RETAIL"] = parsed["UNIT"] * 2

        pb_num = pb.copy()
        pb_num["cents"] = pd.to_numeric(pb_num["cents"], errors="coerce")
        pb_num["cost_cents"] = pd.to_numeric(pb_num["cost_cents"], errors="coerce")
        pb_num["cost_qty"] = pd.to_numeric(pb_num["cost_qty"], errors="coerce")

        def now_for_upc(u: str) -> float:
            if not u or u.startswith("No Match"):
                return np.nan
            key = _lz_strip(u)
            m = pb_num.loc[pb_num["_Upc_norm"] == key, "cents"]
            if m.empty or m.isna().all():
                return np.nan
            return float(m.iloc[0]) / 100.0

        def delta_for_upc(u: str, unit_price: float) -> float:
            if not u or u.startswith("No Match") or unit_price is None or np.isnan(unit_price):
                return np.nan
            key = _lz_strip(u)
            row = pb_num.loc[pb_num["_Upc_norm"] == key, ["cost_cents","cost_qty"]]
            if row.empty:
                return np.nan
            cc = row["cost_cents"].iloc[0]
            cq = row["cost_qty"].iloc[0]
            if pd.isna(cc) or pd.isna(cq) or cq <= 0:
                return np.nan
            return float(unit_price) - (float(cc) / float(cq) / 100.0)

        parsed["NOW"] = parsed["UPC"].apply(now_for_upc)
        parsed["DELTA"] = [delta_for_upc(u, up) for u, up in zip(parsed["UPC"], parsed["UNIT"])]

        parsed = parsed[["UPC","DESCRIPTION","PACK","COST","UNIT","RETAIL","NOW","DELTA","ITEM"]]

        # POS slice (matched UPCs only)
        matched = parsed[~parsed["UPC"].astype(str).str.startswith("No Match")].copy()
        if matched.empty:
            pos_slice = pd.DataFrame(columns=pb.columns)
        else:
            upd = pd.DataFrame({
                "Upc": matched["UPC"].astype(str).str.replace(r"\D","",regex=True).str.zfill(12),
                "cost_qty": matched["PACK"].astype(int),
                "cost_cents": (matched["COST"].astype(float) * 100.0).round().astype(int),
            })
            base = pb.copy()
            base["Upc"] = base["Upc"].astype(str).str.replace(r"\D","",regex=True)
            upd["Upc"] = upd["Upc"].astype(str).str.replace(r"\D","",regex=True)
            pos_slice = base.merge(upd, on="Upc", how="inner", suffixes=("", "_NEW"))
            if not pos_slice.empty:
                pos_slice["cost_qty"] = pos_slice["cost_qty_NEW"].combine_first(pos_slice["cost_qty"])
                pos_slice["cost_cents"] = pos_slice["cost_cents_NEW"].combine_first(pos_slice["cost_cents"])
                pos_slice.drop(columns=[c for c in pos_slice.columns if c.endswith("_NEW")], inplace=True)

        return parsed, pos_slice
