# parsers/nevada_beverage.py
import re
from typing import List, Optional

import numpy as np
import pandas as pd

try:
    import pdfplumber
except Exception:
    pdfplumber = None


class NevadaBeverageParser:
    """
    Parse Nevada Beverage invoice PDFs.

    Expected columns on the invoice table (case/spacing tolerant):
      - QTY          -> Cases (int)
      - DESCRIPTION  -> Item Name (str)
      - U.P.C.       -> UPC (normalize to 12-digit UPC-A)
      - D.PRICE      -> Cost (float)

    Output columns (invoice order preserved):
      ["UPC", "Item Name", "Cost", "Cases"]

    Rules:
      - Keep rightmost 12 digits of UPC, left-pad zeros to 12.
      - Drop rows with Cases == 0 (out-of-stock).
    """

    WANT_COLS = ["UPC", "Item Name", "Cost", "Cases"]

    # ---------- small helpers ----------
    @staticmethod
    def _digits_only(s: str) -> str:
        return re.sub(r"\D", "", str(s)) if s is not None else ""

    @classmethod
    def _norm_upc_12(cls, s: str) -> str:
        d = cls._digits_only(s)
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
        # normalize header tokens: lowercase, remove non-alphanum
        return re.sub(r"[^a-z0-9]", "", s.lower()) if s is not None else ""

    # ---------- table/header matching ----------
    def _match_header(self, columns: List[str]) -> Optional[dict]:
        """
        Try to match header columns: return a mapping to the original column names.
        We accept variants/spaces/punctuation (e.g., 'U.P.C.', 'UPC', 'u p c').
        """
        cmap = {self._canon(c): c for c in columns}

        def pick(*aliases):
            for a in aliases:
                ca = self._canon(a)
                if ca in cmap:
                    return cmap[ca]
            # contains fallback
            for a in aliases:
                ca = self._canon(a)
                for k, orig in cmap.items():
                    if ca in k:
                        return orig
            return None

        col_qty = pick("QTY", "Quantity", "Qty")
        col_desc = pick("DESCRIPTION", "Desc", "Item Description", "Description")
        col_upc = pick("U.P.C.", "UPC", "U P C", "U.P.C")
        col_price = pick("D.PRICE", "DPRICE", "Unit Price", "Price")

        if all([col_qty, col_desc, col_upc, col_price]):
            return {
                "QTY": col_qty,
                "DESCRIPTION": col_desc,
                "U.P.C.": col_upc,
                "D.PRICE": col_price,
            }
        return None

    # ---------- PDF parsing ----------
    def _parse_pdf_tables(self, pdf: "pdfplumber.PDF") -> pd.DataFrame:
        """
        Extract tables from pages; locate the invoice line-item table by header match.
        Build normalized output rows; preserve natural table order across pages.
        """
        all_rows: List[dict] = []

        for page in pdf.pages:
            try:
                tables = page.extract_tables()
            except Exception:
                tables = []

            for tbl in tables or []:
                # Need at least a header + one row
                if not tbl or len(tbl) < 2:
                    continue

                # Some PDFs render duplicated header rows; try first non-empty row as header
                header = None
                header_idx = 0
                for i, row in enumerate(tbl[:5]):  # inspect first few rows for header
                    if any((str(x or "").strip() for x in row)):
                        header = [str(x or "").strip() for x in row]
                        header_idx = i
                        break
                if header is None:
                    continue

                colmap = self._match_header(header)
                if not colmap:
                    continue  # not the line-items table

                # Build a DataFrame using this header
                body = tbl[header_idx + 1 :]
                if not body:
                    continue

                df = pd.DataFrame(body, columns=header)
                # Normalize/clean and append rows
                for _, r in df.iterrows():
                    upc_raw = r.get(colmap["U.P.C."], "")
                    name = r.get(colmap["DESCRIPTION"], "")
                    price = r.get(colmap["D.PRICE"], "")
                    qty = r.get(colmap["QTY"], "")

                    upc = self._norm_upc_12(upc_raw)
                    cost = self._to_float(price)
                    cases = self._to_int(qty)
                    name = str(name).strip()

                    # Skip invalid/empty lines
                    if upc == "" or np.isnan(cost):
                        continue
                    # Ignore out-of-stock / zero-qty lines
                    if cases <= 0:
                        continue

                    all_rows.append({
                        "UPC": upc,
                        "Item Name": name,
                        "Cost": float(cost),
                        "Cases": int(cases),
                    })

        if not all_rows:
            return pd.DataFrame(columns=self.WANT_COLS)

        out = pd.DataFrame(all_rows, columns=self.WANT_COLS)
        # preserve encounter order (already in order we appended)
        out.reset_index(drop=True, inplace=True)
        return out

    # ---------- public entry ----------
    def parse(self, uploaded_file) -> pd.DataFrame:
        """
        Main parse entry. Only PDF is supported here (NV CSVs are unreliable per request).
        """
        if pdfplumber is None:
            # If pdfplumber isn't available in the environment
            return pd.DataFrame(columns=self.WANT_COLS)

        # Reset file pointer just in case
        try:
            uploaded_file.seek(0)
        except Exception:
            pass

        name = (getattr(uploaded_file, "name", "") or "").lower()
        if not name.endswith(".pdf"):
            # Only PDFs for Nevada Beverage in this implementation
            return pd.DataFrame(columns=self.WANT_COLS)

        try:
            with pdfplumber.open(uploaded_file) as pdf:
                items = self._parse_pdf_tables(pdf)
        except Exception:
            items = pd.DataFrame(columns=self.WANT_COLS)

        # Final normalization pass
        if items is None or items.empty:
            return pd.DataFrame(columns=self.WANT_COLS)

        items["UPC"] = items["UPC"].astype(str).str.zfill(12)
        items["Item Name"] = items["Item Name"].astype(str)
        items["Cost"] = pd.to_numeric(items["Cost"], errors="coerce")
        items["Cases"] = pd.to_numeric(items["Cases"], errors="coerce").fillna(0).astype(int)

        # Filter unusable rows (defensive)
        items = items[(items["UPC"] != "") & items["Cost"].notna() & (items["Cases"] > 0)].copy()
        items.reset_index(drop=True, inplace=True)
        return items[self.WANT_COLS]
