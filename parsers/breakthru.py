# parsers/breakthru.py
# Breakthru (CSV) parser
#
# Input: CSV with at least the columns:
#   - "UPC Number(Each)"         → raw UPC, may be blank, may be missing leading zeros
#   - "Item Description"         → item name
#   - "Quantity"                 → cases
#   - "Net Value at Header Level"→ total net value for those cases (sum for all units)
#   - "Item Number"              → Breakthru item id (used to backfill UPC from Master)
#
# Output DataFrame (invoice order preserved):
#   ["UPC", "Item Name", "Cost", "Cases", "Item Number"]
#
# Rules:
#   • Cost (unit) = "Net Value at Header Level" / "Quantity"
#   • Cases      = "Quantity"
#   • UPC normalization: keep digits only, UPC-A 12 digits left-truncated if longer, zero-padded if shorter
#   • If "UPC Number(Each)" is blank, we still return row with UPC=""
#     (app.py will backfill UPC by joining "Item Number" → Master["Invoice UPC"] → Master["Full Barcode"])

from __future__ import annotations
import io
import re
import numpy as np
import pandas as pd


class BreakthruParser:
    WANT_COLS = ["UPC", "Item Name", "Cost", "Cases", "Item Number"]

    @staticmethod
    def _digits_only(s: str) -> str:
        return re.sub(r"\D", "", str(s)) if s is not None else ""

    @classmethod
    def _norm_upc12(cls, s: str) -> str:
        d = cls._digits_only(s)
        if not d:
            return ""
        # If >12, keep last 12 (most UPC exports drop leading system digits)
        if len(d) > 12:
            d = d[-12:]
        return d.zfill(12)

    @staticmethod
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

    @staticmethod
    def _to_int(x):
        try:
            return int(round(float(str(x).strip())))
        except Exception:
            return 0

    def parse(self, uploaded_file) -> pd.DataFrame:
        if uploaded_file is None:
            return pd.DataFrame(columns=self.WANT_COLS)

        # Read CSV as text; let pandas infer delimiter, but tip that it's comma CSV
        uploaded_file.seek(0)
        raw = uploaded_file.read()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="ignore")
        buf = io.StringIO(raw)

        df = pd.read_csv(buf, dtype=str, keep_default_na=False)

        # Column name normalization
        cols = {c.strip(): c for c in df.columns}
        def pick(*names):
            for n in names:
                if n in cols:
                    return cols[n]
            # Loose match fallback
            for n in names:
                for c in cols:
                    if n.lower() == c.lower():
                        return cols[c]
            return None

        c_upc = pick("UPC Number(Each)", "UPC Number (Each)", "UPC Number", "UPC")
        c_desc = pick("Item Description", "Description", "Item Name")
        c_qty = pick("Quantity", "Qty", "Cases")
        c_net = pick("Net Value at Header Level", "Net Value", "NetValue")
        c_item = pick("Item Number", "Item #", "ItemNumber")

        if not all([c_desc, c_qty, c_net, c_item]):
            return pd.DataFrame(columns=self.WANT_COLS)

        # Compute Cost = Net / Quantity
        qty = pd.to_numeric(df[c_qty], errors="coerce").fillna(0).astype(int)
        net = pd.to_numeric(df[c_net].astype(str).str.replace(r"[,$]", "", regex=True), errors="coerce")
        with np.errstate(divide="ignore", invalid="ignore"):
            cost = (net / qty.replace(0, np.nan))

        # Build output
        out = pd.DataFrame({
            "UPC": df[c_upc] if c_upc else "",   # may be blank; app.py will backfill with Master if needed
            "Item Name": df[c_desc],
            "Cost": cost,
            "Cases": qty,
            "Item Number": df[c_item]
        })

        # Normalize UPC to 12 digits (but keep blanks as "")
        out["UPC"] = out["UPC"].apply(lambda x: self._norm_upc12(x) if str(x).strip() != "" else "")

        # Keep rows with positive cases and a computable cost
        out = out[(out["Cases"] > 0) & out["Cost"].notna()].copy()

        # Preserve CSV order
        out.reset_index(drop=True, inplace=True)
        return out[self.WANT_COLS]
