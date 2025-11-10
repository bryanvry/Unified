# parsers/nevada_beverage.py
# Nevada Beverage PDF-only parser with robust fallback.
# Output columns: ["UPC", "Item Name", "Cost", "Cases"] in invoice order.

from typing import List, Optional, Tuple
import re
import numpy as np
import pandas as pd

try:
    import pdfplumber  # ensure in requirements.txt
except Exception:
    pdfplumber = None


class NevadaBeverageParser:
    WANT_COLS = ["UPC", "Item Name", "Cost", "Cases"]

    # ----------------- basic helpers -----------------
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
        # handle stray unicode or trailing dots
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

    @staticmethod
    def _canon(s: str) -> str:
        # normalize to alnum lowercase for matching ("U.P.C." -> "upc")
        return re.sub(r"[^a-z0-9]", "", s.lower()) if s is not None else ""

    # ----------------- header matching -----------------
    def _match_header(self, columns: List[str]) -> Optional[dict]:
        """
        Given a list of header cell texts, return a mapping to the original header labels.
        """
        cmap = {self._canon(c): c for c in columns}

        def pick(*aliases):
            # exact canonical first
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

    # ----------------- table-first parse -----------------
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

                # find a plausible header row
                header = None
                header_idx = 0
                for i, row in enumerate(tbl[:6]):
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

                for _, r in df.iterrows():
                    upc_raw = r.get(colmap["U.P.C."], "")
                    name    = r.get(colmap["DESCRIPTION"], "")
                    price   = r.get(colmap["D.PRICE"], "")
                    qty     = r.get(colmap["QTY"], "")

                    upc   = self._norm_upc_12(upc_raw)
                    cost  = self._to_float(price)
                    cases = self._to_int(qty)
                    name  = str(name).strip()

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
        out.reset_index(drop=True, inplace=True)  # preserve discovery order
        return out

    # ----------------- word-grid fallback -----------------
    def _find_header_spans(self, page) -> Optional[Tuple[float, dict]]:
        """
        Use page.extract_words() to find the header row and approximate column x centers.
        Returns (header_y, col_xcenters) where col_xcenters = {"qty": x, "desc": x, "upc": x, "price": x}
        """
        words = page.extract_words(keep_blank_chars=False, use_text_flow=True)
        if not words:
            return None

        # group words by (approx) y to find a header-like line
        # allow small y tolerance to cluster
        lines = {}
        for w in words:
            y = float(w["top"])
            # quantize y to 2px buckets
            key = round(y / 2.0, 0)
            lines.setdefault(key, []).append(w)

        header_key = None
        best_score = 0
        for key, ws in lines.items():
            txt = " ".join([w["text"] for w in sorted(ws, key=lambda x: x["x0"])])
            ctxt = self._canon(txt)
            score = sum(a in ctxt for a in ["qty", "description", "desc", "upc", "dprice", "price"])
            if score >= 3 and score >= best_score:
                best_score = score
                header_key = key

        if header_key is None:
            return None

        header_words = sorted(lines[header_key], key=lambda x: x["x0"])
        header_txts = [w["text"] for w in header_words]
        # map each needed label to nearest word x-center
        def nearest_xcenter(*aliases):
            for target in aliases:
                tgt = self._canon(target)
                best = None
                best_dx = 1e9
                for w in header_words:
                    if tgt in self._canon(w["text"]):
                        xc = (w["x0"] + w["x1"]) / 2.0
                        dx = 0  # exact hit
                        if dx < best_dx:
                            best = xc
                            best_dx = dx
                # fallback: look for a word that is close in string distance
            # looser fallback: search any word containing a chunk
            for target in aliases:
                tgt = self._canon(target)
                for w in header_words:
                    if tgt[:3] in self._canon(w["text"]):
                        return (w["x0"] + w["x1"]) / 2.0
            return None

        x_qty   = nearest_xcenter("QTY", "Qty", "Quantity")
        x_desc  = nearest_xcenter("DESCRIPTION", "Description", "Desc")
        x_upc   = nearest_xcenter("U.P.C.", "UPC")
        x_price = nearest_xcenter("D.PRICE", "Price")

        # if description not found, approximate to middle
        if x_desc is None:
            x_desc = np.median([w["x0"] for w in header_words]) if header_words else 200.0

        colmap = {"qty": x_qty, "desc": x_desc, "upc": x_upc, "price": x_price}
        header_y = np.mean([w["top"] for w in header_words])
        # ensure at least three columns found (qty/desc/upc/price)
        if sum(v is not None for v in colmap.values()) < 3:
            return None
        return header_y, colmap

    def _bucket_line(self, words_line, col_x):
        """
        Assign words to nearest of qty/desc/upc/price by x-center; join text per column.
        """
        buckets = {"qty": [], "desc": [], "upc": [], "price": []}
        for w in sorted(words_line, key=lambda x: x["x0"]):
            xc = (w["x0"] + w["x1"]) / 2.0
            best = None
            best_dx = 1e9
            for key, x in col_x.items():
                if x is None:
                    continue
                dx = abs(xc - x)
                if dx < best_dx:
                    best = key
                    best_dx = dx
            if best is None:
                # default to desc if no column centers available
                best = "desc"
            buckets[best].append(w["text"])
        return {k: " ".join(v).strip() for k, v in buckets.items()}

    def _parse_word_grid(self, page) -> pd.DataFrame:
        words = page.extract_words(keep_blank_chars=False, use_text_flow=True)
        if not words:
            return pd.DataFrame(columns=self.WANT_COLS)

        found = self._find_header_spans(page)
        if not found:
            return pd.DataFrame(columns=self.WANT_COLS)

        header_y, col_x = found

        # group words into lines below header
        lines = {}
        for w in words:
            if w["top"] <= header_y + 1:  # allow just below header baseline
                continue
            key = round(w["top"] / 2.0, 0)
            lines.setdefault(key, []).append(w)

        rows = []
        for key in sorted(lines.keys()):
            buck = self._bucket_line(lines[key], col_x)
            qty_txt   = buck.get("qty", "")
            desc_txt  = buck.get("desc", "")
            upc_txt   = buck.get("upc", "")
            price_txt = buck.get("price", "")

            # Some PDFs push UPC into desc column; try to rescue
            if not upc_txt and re.search(r"\d", desc_txt or ""):
                # grab a 10-14 digit chunk from desc end
                m = re.search(r"(\d[\d\-\s]{8,})$", desc_txt)
                if m:
                    upc_txt = m.group(1)

            upc   = self._norm_upc_12(upc_txt)
            cases = self._to_int(qty_txt)
            cost  = self._to_float(price_txt)
            name  = desc_txt.strip()

            # sanity: require UPC + cost; cases > 0
            if upc and not np.isnan(cost) and cases > 0:
                rows.append({
                    "UPC": upc,
                    "Item Name": name,
                    "Cost": float(cost),
                    "Cases": int(cases),
                })

        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS)
        return pd.DataFrame(rows, columns=self.WANT_COLS)

    # ----------------- public API -----------------
    def parse(self, uploaded_file) -> pd.DataFrame:
        """
        PDF-only parser. Returns DataFrame with columns:
        ["UPC", "Item Name", "Cost", "Cases"]
        """
        if pdfplumber is None:
            return pd.DataFrame(columns=self.WANT_COLS)

        try:
            uploaded_file.seek(0)
        except Exception:
            pass

        name = (getattr(uploaded_file, "name", "") or "").lower()
        if not name.endswith(".pdf"):
            return pd.DataFrame(columns=self.WANT_COLS)

        try:
            with pdfplumber.open(uploaded_file) as pdf:
                # 1) table-based
                items = self._parse_pdf_tables(pdf)
                if items is None or items.empty:
                    # 2) word-grid fallback, page by page until we get rows
                    rows = []
                    for page in pdf.pages:
                        pg = self._parse_word_grid(page)
                        if not pg.empty:
                            rows.append(pg)
                    items = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=self.WANT_COLS)
        except Exception:
            items = pd.DataFrame(columns=self.WANT_COLS)

        if items is None or items.empty:
            return pd.DataFrame(columns=self.WANT_COLS)

        # Defensive normalization
        items["UPC"] = items["UPC"].astype(str).str.zfill(12)
        items["Item Name"] = items["Item Name"].astype(str)
        items["Cost"] = pd.to_numeric(items["Cost"], errors="coerce")
        items["Cases"] = pd.to_numeric(items["Cases"], errors="coerce").fillna(0).astype(int)

        items = items[(items["UPC"] != "") & items["Cost"].notna() & (items["Cases"] > 0)].copy()
        items.reset_index(drop=True, inplace=True)
        return items[self.WANT_COLS]
