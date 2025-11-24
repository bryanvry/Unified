# parsers/breakthru.py
# Breakthru CSV parser → outputs columns: [UPC, Item Name, Cost, Cases, Item Number]
# - UPC: "UPC Number(Each)" (may miss leading zeros → normalize to 12 digits)
# - Item Name: "Item Description"
# - Cases: "Quantity" (integer)
# - Cost: per-case = "Net Value at Header Level" / "Quantity"
# - Item Number: from "Item Number" (kept to allow UPC fallback in the app layer)
#
# Keeps invoice order. Skips rows with qty <= 0 or missing UPC/cost.
# NOTE: The app will create a *download-only* version of invoice items where rows with
# blank UPC will have "Item Number" substituted into the UPC column for manual fixing.

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
# Item Number can sometimes be missing; treat as optional but prefer to include.
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
    tokens = REQ_BASE[:]  # for any upstream token sniffing

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

        # NEW: fallback – if we can't find a UPC column at all, use Item Number as UPC
        used_item_as_upc = False
        if not c_upc and c_itemn:
            c_upc = c_itemn
            used_item_as_upc = True

        if not all([c_upc, c_name, c_net, c_qty]):
            return pd.DataFrame(columns=["UPC", "Item Name", "Cost", "Cases", "Item Number"])


        qty = pd.to_numeric(df[c_qty], errors="coerce").fillna(0).astype(int)
        net = (
            df[c_net].astype(str)
            .str.replace(r"[,$]", "", regex=True)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0.0)
        )
        # cost per case; NaN when qty == 0 (filtered later)
        cost = net / qty.replace(0, np.nan)

        out = pd.DataFrame({
            "UPC": df[c_upc].astype(str).map(_norm12),
            "Item Name": df[c_name].astype(str).str.strip(),
            "Cost": pd.to_numeric(cost, errors="coerce"),
            "Cases": qty,
            "_order": range(len(df)),
        })

        # Optional Item Number
        if c_itemn:
            out["Item Number"] = df[c_itemn].astype(str).str.strip()
        else:
            out["Item Number"] = ""

        # NEW: row-level fallback – if UPC is blank but Item Number exists, use Item Number digits
        if c_itemn:
            itemnum_series = df[c_itemn].astype(str).str.strip()
            mask_blank_upc = out["UPC"].eq("") & itemnum_series.ne("")
            out.loc[mask_blank_upc, "UPC"] = itemnum_series[mask_blank_upc].map(_norm12)

        # Filter invalid rows for processing
        out = out[
            out["Cases"].gt(0) & out["Cost"].ge(0.01)
        ].copy()


        if out.empty:
            return pd.DataFrame(columns=["UPC", "Item Name", "Cost", "Cases", "Item Number"])

        # Preserve invoice order; ensure UPC textual for downloads
        out = out.sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)
        out["UPC"] = out["UPC"].astype(str)

        # Return with Item Number so the app can do the UPC fallback for the download only
        return out[["UPC", "Item Name", "Cost", "Cases", "Item Number"]]
