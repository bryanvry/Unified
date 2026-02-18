# parsers/breakthru.py
from __future__ import annotations
import io
import pandas as pd
import numpy as np

# We now look for Item Number first
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
    d = _digits(x)
    if not d:
        return ""
    if len(d) > 12:
        d = d[-12:]
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

        # LOGIC CHANGE: Prioritize Item Number over UPC
        # Because your Key File uses Item Number in the 'Invoice UPC' column.
        if c_itemn:
            # We use the Item Number as the "UPC" for matching
            # We strip whitespace but leave it raw (app will normalize/pad it)
            upc_col = df[c_itemn].astype(str).str.strip()
            # If Item Number is missing for a row, fallback to the real UPC
            real_upc = df[c_upc].astype(str).map(_norm12)
            upc_col = np.where(upc_col == "", real_upc, upc_col)
            item_num_col = df[c_itemn].astype(str).str.strip()
        else:
            upc_col = df[c_upc].astype(str).map(_norm12)
            item_num_col = ""

        out = pd.DataFrame({
            "UPC": upc_col,
            "Item Name": df[c_name].astype(str).str.strip(),
            "Cost": pd.to_numeric(cost, errors="coerce"),
            "Cases": qty,
            "Item Number": item_num_col,
            "_order": range(len(df)),
        })

        # Filter out bad rows (Service Fees, zero qty)
        out = out[
            out["Cases"].gt(0) & 
            out["Cost"].ge(0.01) & 
            (out["UPC"] != "") & 
            (out["UPC"] != "000000000000")
        ].copy()

        if out.empty:
            return pd.DataFrame(columns=["UPC", "Item Name", "Cost", "Cases", "Item Number"])

        out = out.sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)
        return out[["UPC", "Item Name", "Cost", "Cases", "Item Number"]]
