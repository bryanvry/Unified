# parsers/breakthru.py
from __future__ import annotations
import io
import pandas as pd
import numpy as np

REQ_BASE = [
    "UPC Number(Each)",
    "Item Description",
    "Net Value at Header Level",
    "Quantity",
]
OPT_ITEMNUM = ["Item Number", "ItemNumber", "Item No", "Item #", "Item"]

def _digits(s: str) -> str:
    return "".join(ch for ch in str(s) if ch.isdigit())

def _norm12(x: str) -> str:
    # Pads "123" -> "000000000123"
    d = _digits(x)
    if not d: return ""
    if len(d) > 12: d = d[-12:]
    return d.zfill(12)

def _find_col(cols, candidates):
    low = [c.lower() for c in cols]
    for cand in candidates:
        if cand.lower() in low:
            return cols[low.index(cand.lower())]
    for cand in candidates:
        for i, c in enumerate(low):
            if cand.lower() in c:
                return cols[i]
    return None

class BreakthruParser:
    name = "Breakthru"
    tokens = REQ_BASE[:]

    def parse(self, uploaded_file) -> pd.DataFrame:
        raw = uploaded_file.read()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="ignore")

        df = pd.read_csv(io.StringIO(raw), dtype=str, keep_default_na=False)

        cols = list(df.columns)
        c_upc   = _find_col(cols, ["UPC Number(Each)", "UPC Number", "UPC"])
        c_name  = _find_col(cols, ["Item Description", "Description"])
        c_net   = _find_col(cols, ["Net Value at Header Level", "Net Value"])
        c_qty   = _find_col(cols, ["Quantity", "Qty"])
        c_itemn = _find_col(cols, OPT_ITEMNUM)

        if not all([c_upc, c_name, c_net, c_qty]):
            return pd.DataFrame(columns=["UPC", "Item Name", "Cost", "Cases", "Item Number"])

        # Calculations
        qty = pd.to_numeric(df[c_qty], errors="coerce").fillna(0).astype(int)
        net = (
            df[c_net].astype(str)
            .str.replace(r"[,$]", "", regex=True)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0.0)
        )
        cost = net / qty.replace(0, np.nan)

        # Output raw columns
        # We DO NOT normalize Item Number here; we leave it raw so it matches your DB exactly
        out = pd.DataFrame({
            "UPC": df[c_upc].astype(str).map(_norm12),
            "Item Name": df[c_name].astype(str).str.strip(),
            "Cost": pd.to_numeric(cost, errors="coerce"),
            "Cases": qty,
            "_order": range(len(df)),
        })

        if c_itemn:
            out["Item Number"] = df[c_itemn].astype(str).str.strip()
        else:
            out["Item Number"] = ""

        # --- FILTER LOGIC ---
        # Keep row if it has (Valid UPC) OR (Valid Item Number)
        # This fixes the issue where items without UPCs were disappearing
        has_upc = (out["UPC"] != "") & (out["UPC"] != "000000000000")
        has_item = (out["Item Number"] != "") & (out["Item Number"] != "000000000000")
        
        out = out[
            out["Cases"].gt(0) & 
            out["Cost"].ge(0.01) & 
            (has_upc | has_item)
        ].copy()

        if out.empty:
            return pd.DataFrame(columns=["UPC", "Item Name", "Cost", "Cases", "Item Number"])

        out = out.sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)
        return out[["UPC", "Item Name", "Cost", "Cases", "Item Number"]]
