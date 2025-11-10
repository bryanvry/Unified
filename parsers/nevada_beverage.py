# parsers/nevada_beverage.py
import io
import re
import csv
import pandas as pd
import numpy as np

class NevadaBeverageParser:
    """
    Robust parser for Nevada Beverage CSV invoices.

    Accepts:
      - CSV with ; , or \t delimiter (auto-detected)
      - Header row may not be the first line (we scan for it)
      - Column aliases:
          unitUpc / unit_upc / upc / unit upc        -> UPC
          itemName / item_name / name / description  -> Item Name
          total / totalAmount / extprice / amount    -> Cost
          quantity / qty / cases / caseqty           -> Cases

    Output (invoice order preserved):
      columns: ["UPC", "Item Name", "Cost", "Cases"]
    """

    WANT_COLS = ["UPC", "Item Name", "Cost", "Cases"]

    # -------- helpers --------
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

    @staticmethod
    def _canon(s: str) -> str:
        # normalize header tokens: lowercase, remove non-alphanum
        return re.sub(r"[^a-z0-9]", "", s.lower()) if s is not None else ""

    @staticmethod
    def _detect_delimiter(header_line: str) -> str:
        # choose the delimiter that appears most in the header line
        cands = [";", ",", "\t"]
        counts = {d: header_line.count(d) for d in cands}
        # fallback to ';' if tie/zero
        best = max(counts, key=counts.get) if any(counts.values()) else ";"
        return best

    def _find_header_index_and_delim(self, text: str):
        """
        Scan lines to find the first header line that contains any of our
        canonical header tokens, and detect its delimiter.
        """
        lines = text.splitlines()
        # sets of canonical aliases we accept
        upc_keys   = {"unitupc", "upc", "unitupc"}  # duplicate ok
        name_keys  = {"itemname", "item", "name", "description", "desc"}
        cost_keys  = {"total", "totalamount", "extprice", "amount", "extended", "extendedamount", "nettotal"}
        qty_keys   = {"quantity", "qty", "cases", "caseqty", "qtyordered", "qtydelivered", "qtyshipped"}

        for i, line in enumerate(lines[:200]):  # only scan first 200 lines
            if not line.strip():
                continue
            delim = self._detect_delimiter(line)
            parts = [self._canon(p) for p in line.split(delim)]
            partset = set(parts)

            # if the line contains at least one from each category, likely a header
            has_upc  = any(k in partset for k in upc_keys)
            has_name = any(k in partset for k in name_keys)
            has_cost = any(k in partset for k in cost_keys)
            has_qty  = any(k in partset for k in qty_keys)

            if (has_upc and has_name and has_cost and has_qty) or \
               (has_upc and has_name and (has_cost or has_qty)):
                return i, delim

        # fallback: assume first non-empty line is header; default ';'
        for i, line in enumerate(lines):
            if line.strip():
                return i, self._detect_delimiter(line)
        return 0, ";"  # ultimate fallback

    def _read_csv_with_known_header(self, text: str, header_idx: int, delim: str) -> pd.DataFrame:
        # Keep invoice order, read starting from header
        lines = text.splitlines()
        sliced = "\n".join(lines[header_idx:])
        return pd.read_csv(
            io.StringIO(sliced),
            sep=delim,
            dtype=str,
            keep_default_na=False,
            engine="python"  # enables sep inference behaviors if needed
        )

    def _pick_col(self, columns, *aliases):
        """
        Pick first matching column by canonical alias, with contains fallback.
        """
        cmap = {self._canon(c): c for c in columns}
        # exact canonical match
        for alias in aliases:
            ca = self._canon(alias)
            if ca in cmap:
                return cmap[ca]
        # contains fallback
        for alias in aliases:
            ca = self._canon(alias)
            for k, orig in cmap.items():
                if ca in k:
                    return orig
        return None

    # -------- main entry --------
    def parse(self, uploaded_file) -> pd.DataFrame:
        # load full text (keep a copy for other reads)
        try:
            uploaded_file.seek(0)
        except Exception:
            pass

        if hasattr(uploaded_file, "read"):
            raw = uploaded_file.read()
            try:
                uploaded_file.seek(0)
            except Exception:
                pass
            # decode with BOM tolerance
            text = raw.decode("utf-8-sig", errors="ignore")
        else:
            # path-like
            with open(uploaded_file, "rb") as f:
                text = f.read().decode("utf-8-sig", errors="ignore")

        header_idx, delim = self._find_header_index_and_delim(text)
        df = self._read_csv_with_known_header(text, header_idx, delim)

        if df is None or df.empty:
            return pd.DataFrame(columns=self.WANT_COLS)

        # map columns
        col_upc = self._pick_col(df.columns, "unitUpc", "unit_upc", "unit upc", "upc")
        col_nm  = self._pick_col(df.columns, "itemName", "item_name", "item name", "name", "description", "desc")
        col_tot = self._pick_col(df.columns, "total", "totalAmount", "extprice", "amount", "extended", "extendedAmount", "nettotal")
        col_qty = self._pick_col(df.columns, "quantity", "qty", "cases", "caseqty", "qtyordered", "qtydelivered", "qtyshipped")

        out = pd.DataFrame(columns=self.WANT_COLS)
        if not all([col_upc, col_nm, col_tot, col_qty]):
            return out  # let caller show “Could not parse…” message

        res = pd.DataFrame()
        res["UPC"]       = df[col_upc].map(self._norm_upc_12)
        res["Item Name"] = df[col_nm].astype(str)
        res["Cost"]      = df[col_tot].map(self._to_float)
        res["Cases"]     = df[col_qty].map(self._to_int)

        # filter unusable rows
        res = res[(res["UPC"] != "") & res["Cost"].notna()].copy()

        # preserve invoice order (no sorting)
        return res[self.WANT_COLS].reset_index(drop=True)
