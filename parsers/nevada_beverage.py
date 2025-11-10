# parsers/nevada_beverage.py
import io
import re
import pandas as pd
import numpy as np

class NevadaBeverageParser:
    """
    Parser for Nevada Beverage CSV invoices.

    Expected header (case-insensitive, semicolon ';' delimiter):
      - unitUpc   -> UPC (normalize to 12 digits, keep rightmost 12, pad with zeros)
      - itemName  -> Item Name
      - total     -> Cost (float)
      - quantity  -> Cases (int)

    Output columns (invoice order preserved):
      ["UPC", "Item Name", "Cost", "Cases"]
    """

    WANT_COLS = ["UPC", "Item Name", "Cost", "Cases"]

    # ---- helpers ----
    @staticmethod
    def _digits_only(s: str) -> str:
        return re.sub(r"\D", "", str(s)) if s is not None else ""

    @staticmethod
    def _norm_upc_12(s: str) -> str:
        d = NevadaBeverageParser._digits_only(s)
        # NV often has extra leading zeros; keep rightmost 12
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

    @staticmethod
    def _canon(s: str) -> str:
        return re.sub(r"[^a-z0-9]", "", s.lower()) if s is not None else ""

    def _pick_col(self, columns, *aliases):
        # exact canonical match first, then "contains"
        cmap = {self._canon(c): c for c in columns}
        for alias in aliases:
            ca = self._canon(alias)
            if ca in cmap:
                return cmap[ca]
        for alias in aliases:
            ca = self._canon(alias)
            for k, orig in cmap.items():
                if ca in k:
                    return orig
        return None

    # ---- main ----
    def parse(self, uploaded_file) -> pd.DataFrame:
        # Ensure we can read the uploaded stream multiple times
        try:
            uploaded_file.seek(0)
        except Exception:
            pass

        # Read as ';'-delimited first (that’s what your file uses)
        # Use utf-8-sig to strip BOM if present
        try:
            raw = uploaded_file.read()
            try:
                uploaded_file.seek(0)
            except Exception:
                pass
            text = raw.decode("utf-8-sig", errors="ignore")
            df = pd.read_csv(io.StringIO(text), sep=";", dtype=str, keep_default_na=False)
        except Exception:
            # Fallback: try comma in case a future export changes delimiter
            try:
                uploaded_file.seek(0)
            except Exception:
                pass
            raw = uploaded_file.read()
            try:
                uploaded_file.seek(0)
            except Exception:
                pass
            text = raw.decode("utf-8-sig", errors="ignore")
            df = pd.read_csv(io.StringIO(text), sep=",", dtype=str, keep_default_na=False)

        if df is None or df.empty:
            return pd.DataFrame(columns=self.WANT_COLS)

        # Map columns (case-insensitive)
        col_upc = self._pick_col(df.columns, "unitUpc", "unit_upc", "unit upc", "upc")
        col_nm  = self._pick_col(df.columns, "itemName", "item_name", "item name", "name", "description")
        col_tot = self._pick_col(df.columns, "total", "totalAmount", "extprice", "amount")
        col_qty = self._pick_col(df.columns, "quantity", "qty", "cases", "caseqty")

        # If any are missing, return empty so app shows the “could not parse” message
        if not all([col_upc, col_nm, col_tot, col_qty]):
            return pd.DataFrame(columns=self.WANT_COLS)

        out = pd.DataFrame()
        out["UPC"]       = df[col_upc].map(self._norm_upc_12)
        out["Item Name"] = df[col_nm].astype(str)
        out["Cost"]      = df[col_tot].map(self._to_float)
        out["Cases"]     = df[col_qty].map(self._to_int)

        # Keep usable rows only; preserve invoice order (no sorting)
        out = out[(out["UPC"] != "") & out["Cost"].notna()].reset_index(drop=True)

        # Return with the exact column set/order expected by the app
        return out[self.WANT_COLS]
