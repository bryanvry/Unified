# parsers/breakthru.py
# Breakthru CSV parser → outputs 4 columns: [UPC, Item Name, Cost, Cases]
# - UPC: "UPC Number(Each)" (may miss leading zeros → normalize to 12 digits)
# - Item Name: "Item Description"
# - Cases: "Quantity" (integer)
# - Cost: per-case = "Net Value at Header Level" / "Quantity"
#
# Keeps invoice order. Skips rows with qty <= 0 or missing UPC/cost.

from __future__ import annotations
import io
import pandas as pd
import numpy as np

REQ = [
    "UPC Number(Each)",
    "Item Description",
    "Net Value at Header Level",
    "Quantity",
]

def _digits(s: str) -> str:
    return "".join(ch for ch in str(s) if ch.isdigit())

def _norm12(x: str) -> str:
    d = _digits(x)
    if not d:
        return ""
    if len(d) > 12:
        d = d[-12:]
    return d.zfill(12)

class BreakthruParser:
    name = "Breakthru"
    tokens = REQ[:]

    def parse(self, uploaded_file) -> pd.DataFrame:
        raw = uploaded_file.read()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="ignore")

        df = pd.read_csv(io.StringIO(raw), dtype=str, keep_default_na=False)

        # Find columns case-insensitively / substring fallback
        cols = list(df.columns)
        def find_col(cands):
            low = [c.lower() for c in cols]
            for cand in cands:
                if cand.lower() in low:
                    return cols[low.index(cand.lower())]
            for cand in cands:
                for i, c in enumerate(low):
                    if cand.lower() in c:
                        return cols[i]
            return None

        c_upc  = find_col(["UPC Number(Each)", "UPC Number", "UPC"])
        c_name = find_col(["Item Description", "Description"])
        c_net  = find_col(["Net Value at Header Level", "Net Value"])
        c_qty  = find_col(["Quantity", "Qty"])

        if not all([c_upc, c_name, c_net, c_qty]):
            return pd.DataFrame(columns=["UPC", "Item Name", "Cost", "Cases"])

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

        # Filter invalid rows
        out = out[
            out["UPC"].astype(str).str.len().ge(1)
            & out["Cases"].gt(0)
            & out["Cost"].ge(0.01)
        ].copy()

        if out.empty:
            return pd.DataFrame(columns=["UPC", "Item Name", "Cost", "Cases"])

        # Preserve invoice order; ensure UPC textual for downloads
        out = out.sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)
        out["UPC"] = out["UPC"].astype(str)
        return out[["UPC", "Item Name", "Cost", "Cases"]]
