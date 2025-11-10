# parsers/nevada_beverage.py
# PDF-only Nevada Beverage parser.
# Expects an invoice table with columns like:
#   ITEM# | QTY | DESCRIPTION | U.P.C. | D.PRICE | DEP | EXT
# Output columns (order preserved): ["UPC", "Item Name", "Cost", "Cases"]

from typing import List, Optional
import re
import numpy as np
import pandas as pd

try:
    import pdfplumber  # make sure pdfplumber is in requirements.txt
except Exception:
    pdfplumber = None


class NevadaBeverageParser:
    WANT_COLS = ["UPC", "Item Name", "Cost", "Cases"]

    # ------------- small helpers -------------
    @staticmethod
    def _digits_only(s: str) -> str:
        return re.sub(r"\D", "", str(s)) if s is not None else ""

    @classmethod
    def _norm_upc_12(cls, s: str) -> str:
        d = cls._digits_only(s)
        if len(d) > 12:
            d = d[-12:]  # keep rightmost 12
        return d.zfill(12)  # left-pad to 12

    @staticmethod
    def _to_float(x):
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return np.nan
        if isinstance(x, (int, float, np.number)):
            return float(x)
        s = str(x).replace("$", "").replace(",", "").strip()
        try:
            return float(s)
        except Exception:
            return np.nan

    @staticmethod
    def _to_int(x):
        try:
            return int(float(str(x).strip()))
        except Exception:
            return 0

    @staticmethod
    def _canon(s: str) -> str:
        # normalize header tokens: lowercase, remove non-alphanum
        return re.sub(r"[^a-z0-9]", "", s.lower()) if s is not None else ""

    # ------------- header matching -------------
    def _match_header(self, columns: List[str]) -> Optional[dict]:
        """
        Return mapping if we can find all required header columns.
        We’re permissive on punctuation & spacing (e.g., U.P.C., U P C, etc.).
        """
        cmap = {self._canon(c): c for c in columns}

        def pick(*aliases):
            # try exact canonical match first
            for a in aliases:
                ca = self._canon(a)
                if ca in cmap:
                    return cmap[ca]
            # then "contains" fallback (handles extra tokens)
            for a in aliases:
                ca = self._canon(a)
                for k, orig in cmap.items():
                    if ca in k:
                        return orig
            return None

        col_qty   = pick("QTY", "Qty", "Quantity")
        col_desc  = pick("DESCRIPTION", "Description", "Item Description", "Desc")
        col_upc   = pick("U.P.C.", "UPC", "U P C", "U.P.C")
        col_price = pick("D.PRICE", "DPRICE", "Unit Price", "Price")

        if all([col_qty, col_desc, col_upc, col_price]):
            return {
                "QTY": col_qty,
                "DESCRIPTION": col_desc,
                "U.P.C.": col_upc,
                "D.PRICE": col_price,
            }
        return None

    # ------------- PDF parsing -------------
    def _parse_pdf_tables(self, pdf: "pdfplumber.PDF") -> pd.DataFrame:
        rows = []

        for page in pdf.pages:
            try:
                tables = page.extract_tables()
            except Exception:
                tables = []

            for tbl in tables or []:
                if not tbl or len(tbl) < 2:
                    continue

                # Find a plausible header row in first few lines
                header = None
                header_idx = 0
                for i, row in enumerate(tbl[:5]):
                    if any(str(x or "").strip() for x in row):
                        header = [str(x or "").strip() for x in row]
                        header_idx = i
                        break
                if header is None:
                    continue

                colmap = self._match_header(header)
                if not colmap:
                    continue

                body = tbl[header_idx + 1 :]
                if not body:
                    continue

                df = pd.DataFrame(body, columns=header)

                # Build normalized rows (preserves order encountered)
                for _, r in df.iterrows():
                    upc_raw = r.get(colmap["U.P.C."], "")
                    name    = r.get(colmap["DESCRIPTION"], "")
                    price   = r.get(colmap["D.PRICE"], "")
                    qty     = r.get(colmap["QTY"], "")

                    upc   = self._norm_upc_12(upc_raw)
                    cost  = self._to_float(price)
                    cases = self._to_int(qty)
                    name  = str(name).strip()

                    # require UPC + cost; ignore zero-qty/out-of-stock lines
                    if upc and not np.isnan(cost) and cases > 0:
                        rows.append({
                            "UPC": upc,
                            "Item Name": name,
                            "Cost": float(cost),
                            "Cases": int(cases),
                        })

        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS)

        out = pd.DataFrame(rows, columns=self.WANT_COLS)
        out.reset_index(drop=True, inplace=True)  # preserve invoice order
        return out

    # ------------- public API -------------
    def parse(self, uploaded_file) -> pd.DataFrame:
        """
        PDF-only parser. Returns DataFrame with columns:
        ["UPC", "Item Name", "Cost", "Cases"]
        """
        if pdfplumber is None:
            return pd.DataFrame(columns=self.WANT_COLS)

        # Ensure at start in case file-like was touched upstream
        try:
            uploaded_file.seek(0)
        except Exception:
            pass

        name = (getattr(uploaded_file, "name", "") or "").lower()
        if not name.endswith(".pdf"):
            # PDF-only by design
            return pd.DataFrame(columns=self.WANT_COLS)

        try:
            with pdfplumber.open(uploaded_file) as pdf:
                items = self._parse_pdf_tables(pdf)
        except Exception:
            items = pd.DataFrame(columns=self.WANT_COLS)

        if items is None or items.empty:
            return pd.DataFrame(columns=self.WANT_COLS)

        # Final normalization pass (defensive)
        items["UPC"] = items["UPC"].astype(str).str.zfill(12)
        items["Item Name"] = items["Item Name"].astype(str)
        items["Cost"] = pd.to_numeric(items["Cost"], errors="coerce")
        items["Cases"] = pd.to_numeric(items["Cases"], errors="coerce").fillna(0).astype(int)

        items = items[(items["UPC"] != "") & items["Cost"].notna() & (items["Cases"] > 0)].copy()
        items.reset_index(drop=True, inplace=True)
        return items[self.WANT_COLS]
