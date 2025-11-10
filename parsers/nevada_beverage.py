# parsers/nevada_beverage.py
# Nevada Beverage PDF-only parser with:
#  • Robust UPC extraction (prevents glued digits)
#  • Unit price (D.PRICE) selection over EXT and DEP=0.00
#  • Out-of-Stock detection (OOS/O.S./OOS/NO SHIP/BACK ORDER) → Cases=0 (excluded)
#  • Regex-stage qty selection that honors an explicit "0" and ignores big item codes
#
# Pipeline:
#   1) Tables via pdfplumber.extract_tables()
#   2) Word-grid reconstruction (header-detected column bucketing)
#   3) Text/regex sweep per line (with unit-price logic + OOS detection + robust qty)
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
    _MIN_PRICE = 0.01  # ignore DEP=0.00 and other zeros

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
          - Otherwise return first 12 of any longer blob.
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

    # ----------------- OOS detection -----------------
    @staticmethod
    def _is_oos(text: str) -> bool:
        """
        Detect out-of-stock markers commonly seen on NV PDFs.
        Examples matched (case-insensitive, flexible spacing/punct):
          - OUT OF STOCK, OUT-OF-STOCK, OUTOFSTOCK
          - O/S, O. S., O S
          - OOS
          - NO SHIP, NO-SHIP, NOSHIP
          - BACK ORDER, BACKORDER, BACK-ORDER, BACK ORDERED
        """
        if not text:
            return False
        t = text.lower()
        patterns = [
            r"out\s*[- ]?\s*of\s*[- ]?\s*stock",
            r"\bo\s*/\s*s\b",          # O/S or O / S
            r"\boos\b",                # OOS
            r"\bno\s*[- ]?\s*ship\b",  # NO SHIP
            r"back\s*[- ]?\s*order",   # BACK ORDER / BACK-ORDER / BACKORDER / BACK ORDERED
            r"back\s*[- ]?\s*ordered",
        ]
        return any(re.search(p, t) for p in patterns)

    # ----------------- price selection -----------------
    def _select_unit_price(self, prices: List[float], qty: int) -> Optional[float]:
        """
        Given all price-like tokens on a line and the parsed qty:
          - Drop zeros/near-zeros (< _MIN_PRICE).
          - If multiple remain, prefer the smallest (likely unit D.PRICE).
          - If we have both small and large and large ≈ small * qty, pick small.
        """
        vals = [p for p in prices if (not np.isnan(p)) and (p >= self._MIN_PRICE)]
        if not vals:
            return None
        vals_sorted = sorted(vals)
        unit = vals_sorted[0]
        if qty and len(vals_sorted) >= 2:
            big = vals_sorted[-1]
            if abs(big - unit * qty) <= 0.02 * max(1.0, big):  # within ~2%
                return unit
        return unit

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
                        # gather row text for OOS check
                        row_text = " ".join(str(x) for x in [qty, name, upc_raw, price] if pd.notna(x))

                        upc   = self._extract_upc_token(upc_raw)
                        cost  = self._to_float(price)
                        cases = self._to_int(qty)
                        name  = str(name).strip()

                        # Out-of-stock override: zero cases if OOS marker appears anywhere in row
                        if self._is_oos(row_text):
                            cases = 0

                        if upc and not np.isnan(cost) and cost >= self._MIN_PRICE and cases > 0:
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
        x_price = nearest_xcenter("D.PRICE", "D PRICE", "UNIT PRICE", "PRICE")  # target unit price center

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
            line_words = lines[key]
            buck = self._bucket_line(line_words, col_x)
            qty_txt   = buck.get("qty", "")
            desc_txt  = buck.get("desc", "")
            upc_txt   = buck.get("upc", "")
            price_txt = buck.get("price", "")

            # Build full-line text for OOS detection
            full_line_text = " ".join([w["text"] for w in sorted(line_words, key=lambda x: x["x0"])])

            # If price bucket mixes values (e.g., "0.00 15.00 75.00"), choose unit (non-zero, smallest)
            prices_in_bucket = [self._to_float(m) for m in re.findall(r"\d+\.\d{2}", price_txt or "")]
            qty_val = self._to_int(qty_txt)

            # Rescue UPC if shoved into desc
            if not upc_txt and re.search(r"\d", desc_txt or ""):
                m = re.search(r"(?<!\d)(\d{12,13})(?!\d)", desc_txt)
                if m:
                    upc_txt = m.group(1)
                else:
                    m2 = re.search(r"(\d{10,})", desc_txt)
                    if m2:
                        upc_txt = m2.group(1)

            upc = self._extract_upc_token(upc_txt)

            # If bucket had no good price, try all prices on the line
            unit_price = self._select_unit_price(prices_in_bucket, qty_val)
            if unit_price is None:
                all_prices = [self._to_float(m) for m in re.findall(r"\d+\.\d{2}", full_line_text)]
                unit_price = self._select_unit_price(all_prices, qty_val)

            cases = qty_val
            name  = desc_txt.strip()

            # Out-of-stock override
            if self._is_oos(full_line_text):
                cases = 0

            if upc and (unit_price is not None) and unit_price >= self._MIN_PRICE and cases > 0:
                rows.append({"UPC": upc, "Item Name": name, "Cost": float(unit_price), "Cases": int(cases)})

        if not rows:
            return pd.DataFrame(columns=self.WANT_COLS)
        return pd.DataFrame(rows, columns=self.WANT_COLS)

    # ----------------- Stage 3: text/regex sweep -----------------
    def _pick_qty_from_line_ints(self, line: str) -> Optional[int]:
        """
        Qty heuristic for regex stage:
          - Collect all integers.
          - Drop anything >= 1000 (likely product codes).
          - If 0 present, return 0 (explicit zero-arrival).
          - Else pick the first remaining int (<= 200).
        """
        ints = [int(m.group(1)) for m in re.finditer(r"\b(\d{1,4})\b", line)]
        small = [v for v in ints if v <= 200]  # typical case counts
        # remove likely product codes (>=1000)
        small = [v for v in small if v < 1000]
        if not small:
            return None
        if 0 in small:
            return 0
        return small[0]

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

            # OOS detection
            if self._is_oos(line):
                qty = 0  # force zero-arrival
            else:
                qty = self._pick_qty_from_line_ints(line)

            if qty is None or qty <= 0:
                # zero or unknown qty → exclude
                continue

            # All price tokens on the line; ignore zeros
            prices = [self._to_float(mm.group(1)) for mm in re.finditer(r"(\d+\.\d{2})", line)]
            unit_price = self._select_unit_price(prices, qty)
            if unit_price is None or unit_price < self._MIN_PRICE:
                continue

            # Description between qty and UPC if possible
            upc_pos = m.start() if m else line.find(upc)
            # Try to find the chosen qty's position to clip description after it
            qpos = None
            for mm in re.finditer(r"\b(\d{1,4})\b", line):
                if int(mm.group(1)) == qty:
                    qpos = mm.end()
                    break
            left_clip = qpos if qpos is not None else 0
            desc = line[left_clip:upc_pos].strip()
            desc = re.sub(r"\s{2,}", " ", desc).strip(" -–—|;,")
            if not desc:
                desc = line[:upc_pos].strip()

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

                # Stage 3: text/regex sweep (with unit-price logic + OOS detection + robust qty)
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

        # Keep only arrived items (Cases > 0) and valid costs
        items = items[(items["UPC"] != "") & items["Cost"].notna() & (items["Cost"] >= self._MIN_PRICE) & (items["Cases"] > 0)].copy()
        items.reset_index(drop=True, inplace=True)
        return items[self.WANT_COLS]
