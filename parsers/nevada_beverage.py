# parsers/nevada_beverage.py
# Nevada Beverage PDF-only parser with robust UPC extraction
# and explicit preference for D.PRICE (unit price) over EXT (extended).
#
# Stages:
#   1) Tables via pdfplumber.extract_tables()
#   2) Word-grid reconstruction (header-detected column bucketing)
#   3) Text/regex sweep per line (with unit-price selection)
#
# Output columns (invoice order preserved): ["UPC", "Item Name", "Cost", "Cases"]

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

    # ----------------- helpers -----------------
    @staticmethod
    def _digits_only(s: str) -> str:
        return re.sub(r"\D", "", str(s)) if s is not None else ""

    @classmethod
    def _extract_upc_token(cls, s: str) -> str:
        """
        Find a 12–13 digit token *bounded by non-digits* (prevents gluing qty/price).
        Rules:
          - If 13 digits and starts with '0' (EAN-13 wrapper), return last 12 (UPC-A).
          - Else if 13 digits and doesn't start with '0', return first 12.
          - If 12 digits, return as-is.
          - Otherwise return "" (or first 12 of any long blob as a last resort).
        """
        if not s:
            return ""
        text = str(s)

        m = re.search(r"(?<!\d)(\d{12,13})(?!\d)", text)
        if not m:
            m = re.search(r"(\d{10,})", text)
            if not m:
                return ""
            run = m.group(1)
        else:
            run = m.group(1)

        if len(run) == 12:
            return run
        if len(run) == 13:
            if run.startswith("0"):
                return run[1:]
            return run[:12]
        return run[:12]

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

    @staticmethod
    def _canon(s: str) -> str:
        return re.sub(r"[^a-z0-9]", "", s.lower()) if s is not None else ""

    # ----------------- header matching -----------------
    def _match_header(self, columns: List[str]) -> Optional[dict]:
        cmap = {self._canon(c): c for c in columns}

        def pick(*aliases):
            for a in aliases:
                ca = self._canon(a)
                if ca in cmap:
                    return cmap[ca]
            for a in aliases:
                ca = self._canon(a)
                for k, orig in cmap.items():
                    if ca in k:
                        return orig
            return None

        col_qty   = pick("QTY", "Qty", "Quantity")
        col_desc  = pick("DESCRIPTION", "Description", "Item Description", "Desc")
        col_upc   = pick("U.P.C.", "UPC", "U P C", "U.P.C")
        # D.PRICE = unit price; aliases included
        col_price = pick("D.PRICE", "D PRICE", "DPRICE", "UNITPRICE", "UNIT PRICE", "PRICE")

        if all([col_qty, col_desc, col_upc, col_price]):
            return {
                "QTY": col_qty,
                "DESCRIPTION": col_desc,
                "U.P.C.": col_upc,
                "D.PRICE": col_price,
            }
        return None

    # ----------------- Stage 1: tables -----------------
    def _parse_pdf_tables(self, pdf: "pdfplumber.PDF") -> pd.DataFrame:
        rows = []

        table_settings_list = [
            {"vertical_strategy": "lines", "horizontal_strategy": "lines", "intersection_tolerance": 5, "snap_tolerance": 3, "join_tolerance": 3, "edge_min_length": 3},
            {"vertical_strategy": "text", "horizontal_strategy": "text"},
        ]

        for page in pdf.pages:
            for settings in table_settings_list:
                try:
                    tables = page.extract_tables(table_settings=settings)
                except Exception:
                    tables = []
                for tbl in tables or []:
                    if not tbl or len(tbl) < 2:
                        continue

                    # plausible header
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

                        upc   = self._extract_upc_token(upc_raw)
                        cost  = self._to_float(price)
                        cases = self._to_int(qty)
                        name  = str(name).strip()

                        if upc and not np.isnan(cost) and cases > 0:
                            rows.append({"UPC": upc, "Item Name": name, "Cost": float(cost), "Cases": int(cases)})

        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS)
        out = pd.DataFrame(rows, columns=self.WANT_COLS)
        out.reset_index(drop=True, inplace=True)
        return out

    # ----------------- Stage 2: word-grid -----------------
    def _find_header_spans(self, page) -> Optional[Tuple[float, dict]]:
        words = page.extract_words(keep_blank_chars=False, use_text_flow=True)
        if not words:
            return None

        lines = {}
        for w in words:
            key = round(float(w["top"]) / 2.0, 0)
            lines.setdefault(key, []).append(w)

        header_key = None
        best_score = 0
        for key, ws in lines.items():
            txt = " ".join([w["text"] for w in sorted(ws, key=lambda x: x["x0"])])
            ctxt = self._canon(txt)
            score = sum(a in ctxt for a in ["qty", "description", "desc", "upc", "dprice", "price", "unitprice"])
            if score >= 3 and score >= best_score:
                best_score = score
                header_key = key

        if header_key is None:
            return None

        header_words = sorted(lines[header_key], key=lambda x: x["x0"])

        def nearest_xcenter(*aliases):
            for target in aliases:
                tgt = self._canon(target)
                for w in header_words:
                    if tgt in self._canon(w["text"]):
                        return (w["x0"] + w["x1"]) / 2.0
            if header_words:
                return float(np.median([(w["x0"] + w["x1"]) / 2.0 for w in header_words]))
            return None

        x_qty   = nearest_xcenter("QTY", "Qty", "Quantity")
        x_desc  = nearest_xcenter("DESCRIPTION", "Description", "Desc")
        x_upc   = nearest_xcenter("U.P.C.", "UPC")
        x_price = nearest_xcenter("D.PRICE", "D PRICE", "UNIT PRICE", "PRICE")  # price center ~ unit price

        colmap = {"qty": x_qty, "desc": x_desc, "upc": x_upc, "price": x_price}
        header_y = float(np.mean([w["top"] for w in header_words]))
        if sum(v is not None for v in colmap.values()) < 3:
            return None
        return header_y, colmap

    def _bucket_line(self, words_line, col_x):
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

        lines = {}
        for w in words:
            if w["top"] <= header_y + 1:
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

            # Rescue UPC if shoved into desc
            if not upc_txt and re.search(r"\d", desc_txt or ""):
                m = re.search(r"(?<!\d)(\d{12,13})(?!\d)", desc_txt)
                if m:
                    upc_txt = m.group(1)
                else:
                    m2 = re.search(r"(\d{10,})", desc_txt)
                    if m2:
                        upc_txt = m2.group(1)

            upc   = self._extract_upc_token(upc_txt)
            cases = self._to_int(qty_txt)
            cost  = self._to_float(price_txt)
            name  = desc_txt.strip()

            if upc and not np.isnan(cost) and cases > 0:
                rows.append({"UPC": upc, "Item Name": name, "Cost": float(cost), "Cases": int(cases)})

        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS)
        return pd.DataFrame(rows, columns=self.WANT_COLS)

    # ----------------- Stage 3: text/regex sweep -----------------
    def _select_unit_price(self, prices: List[float], qty: int) -> Optional[float]:
        """
        Given all price-like tokens on a line and the parsed qty:
          - If multiple prices, prefer the smallest (likely D.PRICE).
          - If we have both small and large and large ≈ small * qty, pick small.
          - Else return the smallest non-NaN.
        """
        vals = [p for p in prices if not np.isnan(p)]
        if not vals:
            return None
        vals_sorted = sorted(vals)
        unit = vals_sorted[0]
        if qty and len(vals_sorted) >= 2:
            big = vals_sorted[-1]
            if abs(big - unit * qty) <= 0.02 * max(1.0, big):  # within ~2%
                return unit
        return unit

    def _parse_text_regex(self, page) -> pd.DataFrame:
        txt = page.extract_text() or ""
        if not txt.strip():
            return pd.DataFrame(columns=self.WANT_COLS)

        rows = []
        for li, raw in enumerate(txt.splitlines()):
            line = raw.strip()
            if len(line) < 6:
                continue

            # UPC token
            upc = ""
            m = re.search(r"(?<!\d)(\d{12,13})(?!\d)", line)
            if m:
                upc = self._extract_upc_token(m.group(1))
            else:
                m2 = re.search(r"(\d{10,})", line)
                if m2:
                    upc = self._extract_upc_token(m2.group(1))
            if not upc:
                continue

            # All price tokens on the line
            price_matches = list(re.finditer(r"(\d+\.\d{2})", line))
            prices = [self._to_float(m.group(1)) for m in price_matches]
            if not prices:
                continue

            # Qty (use first small integer; refine using pos relative to UPC if needed)
            qty = None
            ints = list(re.finditer(r"\b(\d{1,4})\b", line))
            if ints:
                try:
                    qty = int(ints[0].group(1))
                except Exception:
                    qty = None
            if (qty is None or qty <= 0) and m:
                left = line[:m.start()]
                ints2 = list(re.finditer(r"\b(\d{1,4})\b", left))
                if ints2:
                    try:
                        qty = int(ints2[-1].group(1))
                    except Exception:
                        qty = None
            if qty is None or qty <= 0:
                continue

            # Choose unit price over EXT
            unit_price = self._select_unit_price(prices, qty)
            if unit_price is None:
                continue

            # Description between qty and UPC if possible
            upc_start = m.start() if m else line.find(upc)
            qm = re.search(r"\b" + re.escape(str(qty)) + r"\b", line)
            left_clip = (qm.end() if qm else 0)
            desc = line[left_clip:upc_start].strip()
            desc = re.sub(r"\s{2,}", " ", desc).strip(" -–—|;,")
            if not desc:
                desc = line[:upc_start].strip()

            rows.append({"UPC": upc, "Item Name": desc, "Cost": float(unit_price), "Cases": int(qty), "_order": li})

        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS)

        out = pd.DataFrame(rows).sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)
        return out[self.WANT_COLS]

    # ----------------- public API -----------------
    def parse(self, uploaded_file) -> pd.DataFrame:
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
                # Stage 1: tables
                items = self._parse_pdf_tables(pdf)

                # Stage 2: word-grid fallback
                if items is None or items.empty:
                    rows = []
                    for page in pdf.pages:
                        pg = self._parse_word_grid(page)
                        if not pg.empty:
                            rows.append(pg)
                    items = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=self.WANT_COLS)

                # Stage 3: text/regex sweep (with unit-price logic)
                if items is None or items.empty:
                    rows = []
                    for page in pdf.pages:
                        pg = self._parse_text_regex(page)
                        if not pg.empty:
                            rows.append(pg)
                    items = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=self.WANT_COLS)
        except Exception:
            items = pd.DataFrame(columns=self.WANT_COLS)

        if items is None or items.empty:
            return pd.DataFrame(columns=self.WANT_COLS)

        # Defensive normalization
        items["UPC"] = items["UPC"].astype(str).str.replace(r"\D", "", regex=True).str[:12].str.zfill(12)
        items["Item Name"] = items["Item Name"].astype(str).str.strip()
        items["Cost"] = pd.to_numeric(items["Cost"], errors="coerce")
        items["Cases"] = pd.to_numeric(items["Cases"], errors="coerce").fillna(0).astype(int)

        items = items[(items["UPC"] != "") & items["Cost"].notna() & (items["Cases"] > 0)].copy()
        items.reset_index(drop=True, inplace=True)
        return items[self.WANT_COLS]
