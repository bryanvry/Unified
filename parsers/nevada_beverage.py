# parsers/nevada_beverage.py
import io
import pandas as pd
import numpy as np
import re

class NevadaBeverageParser:
    """
    Parse Nevada Beverage CSV (semicolon ';' delimited).

    Input columns (case-insensitive):
      - unitUpc   -> UPC (normalize to 12 digits)
      - itemName  -> Item Name
      - total     -> Cost  (float)
      - quantity  -> Cases (int)

    Output dataframe columns (in invoice order):
      ["UPC", "Item Name", "Cost", "Cases"]
    """

    WANT_COLS = ["UPC", "Item Name", "Cost", "Cases"]

    @staticmethod
    def _digits_only(s: str) -> str:
        return re.sub(r"\D", "", str(s)) if s is not None else ""

    @staticmethod
    def _norm_upc_12(s: str) -> str:
        d = NevadaBeverageParser._digits_only(s)
        if len(d) > 12:
            d = d[-12:]
        return d.zfill(12)

    @staticmethod
    def _to_float(x):
        if x is None:
            return np.nan
        if isinstance(x, (int, float, np.number)):
            return float(x)
        s = str(x).replace("$", "").replace(",", "").strip()
        try:
            return float(s)
        except:
            return np.nan

    @staticmethod
    def _to_int(x):
        try:
            return int(float(str(x).strip()))
        except:
            return 0

    def parse(self, uploaded_file) -> pd.DataFrame:
        # Read semicolon-separated CSV, preserving order
        # (Streamlit gives a BytesIO-like object; ensure we start at 0)
        try:
            uploaded_file.seek(0)
        except Exception:
            pass

        # Try decoding to text; fall back to binary stream with sep=';'
        if hasattr(uploaded_file, "read"):
            raw = uploaded_file.read()
            # put stream back for any caller who reuses it
            try:
                uploaded_file.seek(0)
            except Exception:
                pass
            # Decode safely; CSV is typically UTF-8
            text = raw.decode("utf-8", errors="ignore")
            df = pd.read_csv(io.StringIO(text), sep=";", dtype=str, keep_default_na=False)
        else:
            # If somehow a filepath ends up here
            df = pd.read_csv(uploaded_file, sep=";", dtype=str, keep_default_na=False)

        if df is None or df.empty:
            return pd.DataFrame(columns=self.WANT_COLS)

        # Normalize headers to lowercase for robust lookup
        lower_map = {c.lower().strip(): c for c in df.columns}
        def pick(*names):
            for n in names:
                if n.lower() in lower_map:
                    return lower_map[n.lower()]
            # fuzzy contains
            for n in names:
                for lc, orig in lower_map.items():
                    if n.lower() in lc:
                        return orig
            return None

        col_upc   = pick("unitUpc", "unit_upc", "upc", "unit upc")
        col_name  = pick("itemName", "item_name", "name", "item name")
        col_total = pick("total", "extended", "amount", "price")
        col_qty   = pick("quantity", "qty", "cases")

        out = pd.DataFrame(columns=self.WANT_COLS)
        if not all([col_upc, col_name, col_total, col_qty]):
            return out

        res = pd.DataFrame()
        res["UPC"]       = df[col_upc].map(self._norm_upc_12)
        res["Item Name"] = df[col_name].astype(str)
        res["Cost"]      = df[col_total].map(self._to_float)
        res["Cases"]     = df[col_qty].map(self._to_int)

        # Keep only rows with valid UPC and non-null cost
        res = res[(res["UPC"] != "") & res["Cost"].notna()].copy()

        # Preserve original invoice order (no sorting)
        res = res[self.WANT_COLS].reset_index(drop=True)
        return res
