# parsers/breakthru.py
# Minimal CSV parser for Breakthru.
# Produces: ["UPC", "Item Name", "Cost", "Cases", "Item Number"]
# - UPC source: "UPC Number(Each)" (may be blank in some rows; leave blank here)
# - Item Name: "Item Description"
# - Cost: "Net Value at Header Level" / "Quantity" (per-case cost)
# - Cases: "Quantity"
#
# NOTE: We do not backfill UPCs here (to keep this file self-contained and tiny).
# The app will optionally backfill UPC from Master via Item Number if Master is uploaded.

from __future__ import annotations
import io
import pandas as pd

REQUIRED_COLS = [
    "UPC Number(Each)",
    "Item Description",
    "Net Value at Header Level",
    "Quantity",
    "Item Number",
]

class BreakthruParser:
    slug = "breakthru"
    label = "Breakthru"

    def parse(self, file) -> pd.DataFrame:
        # Accept only CSV (you asked specifically for CSV for Breakthru)
        # Streamlit uploads give a file-like object; read directly.
        raw = file.read()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="ignore")

        df = pd.read_csv(io.StringIO(raw), dtype=str, keep_default_na=False)

        # Ensure columns exist; if any are missing, return empty to avoid breaking other flows
        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            return pd.DataFrame(columns=["UPC", "Item Name", "Cost", "Cases", "Item Number"])

        # Coerce numerics used for math
        qty = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
        net_val = pd.to_numeric(
            df["Net Value at Header Level"].astype(str).str.replace(r"[,$]", "", regex=True),
            errors="coerce",
        ).fillna(0.0)

        # Build output (preserve CSV order)
        out = pd.DataFrame({
            "UPC": df["UPC Number(Each)"].astype(str).str.strip(),    # may be blank -> backfill later in app
            "Item Name": df["Item Description"].astype(str),
            "Cost": (net_val / qty.replace(0, pd.NA)).astype(float),  # per-case cost
            "Cases": qty.astype(int),
            "Item Number": df["Item Number"].astype(str).str.strip(),
        })

        # If division by zero produced NA, set Cost = 0.0 (keeps things simple)
        out["Cost"] = pd.to_numeric(out["Cost"], errors="coerce").fillna(0.0)

        # Return only required columns for downstream
        return out[["UPC", "Item Name", "Cost", "Cases", "Item Number"]]
